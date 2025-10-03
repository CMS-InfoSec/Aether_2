"""Reactive hedging controller for Aether trading accounts.

The module exposes a :class:`HedgingService` that continuously monitors
market volatility (average true range and realised volatility) together with
account level drawdowns.  When the observed risk exceeds configurable
thresholds the service increases the hedge allocation by routing orders to the
OMS.  When conditions normalise the hedge is gradually unwound back to the
baseline allocation.

The implementation is self contained and designed to run as a lightweight
background worker – callers are expected to provide concrete implementations of
``MarketDataSource`` and ``PnLDataSource`` for their environment.  The default
``LoggingOMSClient`` publishes the generated hedge orders via the in-memory
``TimescaleAdapter`` which mirrors the behaviour of the existing test
infrastructure.  Production deployments can supply an adapter that integrates
with the deployed OMS HTTP or websocket interface.
"""

from __future__ import annotations

import asyncio
import logging
import math
import statistics
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Iterable, List, Mapping, Optional, Protocol, Sequence

from services.common.adapters import TimescaleAdapter


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Market data abstractions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PriceBar:
    """Minimal OHLCV bar used for volatility calculations."""

    high: float
    low: float
    close: float

    def validate(self) -> None:
        if self.high <= 0 or self.low <= 0 or self.close <= 0:
            raise ValueError("Price bars must contain strictly positive prices")
        if self.low > self.high:
            raise ValueError("Low price cannot exceed high price")


class MarketDataSource(Protocol):
    """Protocol describing the market data requirements for hedging."""

    def recent_bars(self, symbol: str, limit: int) -> Sequence[PriceBar]:
        """Return the latest ``limit`` bars ordered from oldest to newest."""


class PnLDataSource(Protocol):
    """Protocol describing PnL telemetry required for hedging decisions."""

    def drawdown_pct(self) -> float:
        """Return the current peak-to-trough drawdown percentage as a positive value."""


class OMSClient(Protocol):
    """Protocol used to submit hedge orders to the OMS."""

    def submit_hedge_order(self, order: "HedgeOrder") -> None:
        """Submit the provided hedge order to the OMS."""


@dataclass
class HedgeOrder:
    """Container describing the orders routed to the OMS."""

    account_id: str
    client_order_id: str
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    order_type: str = "limit"
    time_in_force: str = "GTC"

    def to_payload(self) -> dict:
        return {
            "account_id": self.account_id,
            "client_order_id": self.client_order_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": str(self.quantity),
            "price": str(self.price),
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
        }


@dataclass(frozen=True)
class InstrumentPrecision:
    tick_size: Decimal
    lot_size: Decimal


class PrecisionProvider(Protocol):
    def get_precision(self, symbol: str) -> Optional[InstrumentPrecision]:
        """Return precision metadata for the provided instrument if available."""


class MarketMetadataPrecisionProvider:
    """Fetch precision data from the Kraken market metadata cache."""

    def __init__(self, metadata: Optional[Mapping[str, Mapping[str, float]]] = None) -> None:
        if metadata is not None:
            self._metadata_getter = lambda: metadata
        else:
            self._metadata_getter = self._default_metadata_getter

    @staticmethod
    def _default_metadata_getter() -> Mapping[str, Mapping[str, float]]:
        try:
            from services.oms import main as oms_main  # Local import to avoid circular deps
        except Exception:  # pragma: no cover - metadata unavailable in limited envs
            return {}
        return getattr(oms_main, "MARKET_METADATA", {})

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return symbol.replace("/", "-").upper()

    def get_precision(self, symbol: str) -> Optional[InstrumentPrecision]:
        metadata = self._metadata_getter()
        entry = metadata.get(self._normalize_symbol(symbol))
        if not isinstance(entry, Mapping):
            return None
        tick = entry.get("tick")
        lot = entry.get("lot")
        if tick is None or lot is None:
            return None
        try:
            tick_size = Decimal(str(tick))
            lot_size = Decimal(str(lot))
        except (InvalidOperation, TypeError, ValueError):
            return None
        if tick_size <= 0 or lot_size <= 0:
            return None
        return InstrumentPrecision(tick_size=tick_size, lot_size=lot_size)


@dataclass
class LoggingOMSClient:
    """Default OMS client that records hedge activity via ``TimescaleAdapter``."""

    account_id: str
    timescale: TimescaleAdapter = field(init=False)

    def __post_init__(self) -> None:
        self.timescale = TimescaleAdapter(account_id=self.account_id)

    def submit_hedge_order(self, order: HedgeOrder) -> None:  # pragma: no cover - thin wrapper
        logger.info(
            "Routing hedge order %s %s %s @ %s (qty %s)",
            order.client_order_id,
            order.side,
            order.symbol,
            order.price,
            order.quantity,
        )
        self.timescale.record_event(
            event_type="hedge.order", payload={"order": order.to_payload()}
        )


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class HedgeConfig:
    """Risk thresholds and hedge sizing configuration."""

    account_id: str
    hedge_symbol: str = "USDUSDT"
    base_allocation_usd: float = 50_000.0
    max_allocation_usd: float = 250_000.0
    atr_threshold: float = 15.0
    atr_window: int = 14
    realized_vol_threshold: float = 0.35
    realized_vol_window: int = 30
    max_drawdown_pct: float = 0.10
    rebalance_tolerance_usd: float = 5_000.0
    poll_interval_seconds: float = 60.0
    unwind_cooldown_seconds: float = 180.0

    def __post_init__(self) -> None:
        if self.base_allocation_usd <= 0:
            raise ValueError("base_allocation_usd must be positive")
        if self.max_allocation_usd < self.base_allocation_usd:
            raise ValueError("max_allocation_usd must be >= base_allocation_usd")
        if self.atr_window <= 1 or self.realized_vol_window <= 1:
            raise ValueError("volatility windows must be greater than one")
        if self.atr_threshold <= 0 or self.realized_vol_threshold <= 0:
            raise ValueError("volatility thresholds must be positive")
        if self.max_drawdown_pct <= 0:
            raise ValueError("max_drawdown_pct must be positive")


@dataclass
class HedgeState:
    """Mutable runtime state for the hedging service."""

    current_allocation: float
    last_rebalance: float = field(default=0.0)
    last_risk_score: float = field(default=0.0)
    last_update: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def update_allocation(self, allocation: float) -> None:
        self.current_allocation = allocation
        self.last_update = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Volatility calculations
# ---------------------------------------------------------------------------


class VolatilityMonitor:
    """Utility helpers for ATR and realised volatility."""

    @staticmethod
    def average_true_range(bars: Sequence[PriceBar], window: int) -> Optional[float]:
        if len(bars) <= window:
            return None

        for bar in bars:
            bar.validate()

        true_ranges: List[float] = []
        prev_close = bars[0].close
        for bar in bars[1:]:
            high_low = bar.high - bar.low
            high_close = abs(bar.high - prev_close)
            low_close = abs(bar.low - prev_close)
            true_ranges.append(max(high_low, high_close, low_close))
            prev_close = bar.close

        if len(true_ranges) < window:
            return None
        window_tr = true_ranges[-window:]
        atr = sum(window_tr) / window
        return float(atr)

    @staticmethod
    def realised_volatility(bars: Sequence[PriceBar], window: int) -> Optional[float]:
        if len(bars) <= window:
            return None

        returns: List[float] = []
        prev_close = bars[0].close
        for bar in bars[1:]:
            bar.validate()
            if bar.close <= 0 or prev_close <= 0:
                raise ValueError("Close prices must be positive for volatility calculations")
            returns.append(math.log(bar.close / prev_close))
            prev_close = bar.close

        if len(returns) < window:
            return None
        window_returns = returns[-window:]
        if all(value == 0 for value in window_returns):
            return 0.0

        stdev = statistics.pstdev(window_returns)
        annualised = stdev * math.sqrt(252)
        return float(annualised)


# ---------------------------------------------------------------------------
# Hedging service implementation
# ---------------------------------------------------------------------------


class HedgingService:
    """Evaluate market risk and manage USD/USDT hedge allocations."""

    def __init__(
        self,
        config: HedgeConfig,
        market_data: MarketDataSource,
        pnl_source: PnLDataSource,
        oms_client: Optional[OMSClient] = None,
        timescale: Optional[TimescaleAdapter] = None,
        precision_provider: Optional[PrecisionProvider] = None,
    ) -> None:
        self.config = config
        self.market_data = market_data
        self.pnl_source = pnl_source
        self.timescale = timescale or TimescaleAdapter(account_id=config.account_id)
        self.oms = oms_client or LoggingOMSClient(account_id=config.account_id)
        self.state = HedgeState(current_allocation=config.base_allocation_usd)
        self._cooldown_until: float = 0.0
        self._precision_provider: PrecisionProvider = (
            precision_provider or MarketMetadataPrecisionProvider()
        )

    # ------------------------------------------------------------------
    # Public orchestration
    # ------------------------------------------------------------------
    def evaluate_once(self) -> None:
        """Evaluate the current market risk and adjust hedges if required."""

        lookback = max(self.config.atr_window + 1, self.config.realized_vol_window + 1)
        bars = list(self.market_data.recent_bars(self.config.hedge_symbol, lookback))
        if len(bars) < lookback:
            logger.warning(
                "Insufficient market data for %s: required %s bars, received %s",
                self.config.hedge_symbol,
                lookback,
                len(bars),
            )
            return

        atr = VolatilityMonitor.average_true_range(bars, self.config.atr_window)
        realized_vol = VolatilityMonitor.realised_volatility(
            bars, self.config.realized_vol_window
        )
        drawdown_pct = self.pnl_source.drawdown_pct()

        risk_score = self._compute_risk_score(atr, realized_vol, drawdown_pct)
        logger.debug(
            "Hedging risk metrics account=%s atr=%s realized_vol=%s drawdown_pct=%.4f risk_score=%.4f",
            self.config.account_id,
            None if atr is None else f"{atr:.4f}",
            None if realized_vol is None else f"{realized_vol:.4f}",
            drawdown_pct,
            risk_score,
        )

        target_allocation = self._target_allocation(risk_score)
        if self._should_rebalance(target_allocation):
            last_close = bars[-1].close
            self._rebalance(target_allocation, last_close, risk_score)
        else:
            self.state.last_risk_score = risk_score
            self.state.last_update = datetime.now(timezone.utc)

    async def run_forever(self, stop_event: Optional[asyncio.Event] = None) -> None:
        """Continuously evaluate risk until the optional stop event is set."""

        while True:
            if stop_event and stop_event.is_set():
                logger.info("Hedging service stopping due to stop event")
                return
            try:
                self.evaluate_once()
            except Exception:  # pragma: no cover - protective catch
                logger.exception("Unhandled exception during hedging evaluation")
            await asyncio.sleep(self.config.poll_interval_seconds)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _compute_risk_score(
        self,
        atr: Optional[float],
        realized_vol: Optional[float],
        drawdown_pct: float,
    ) -> float:
        atr_score = (atr / self.config.atr_threshold) if atr is not None else 0.0
        vol_score = (
            (realized_vol / self.config.realized_vol_threshold)
            if realized_vol is not None and self.config.realized_vol_threshold > 0
            else 0.0
        )
        drawdown_score = drawdown_pct / self.config.max_drawdown_pct
        score = max(atr_score, vol_score, drawdown_score)
        return float(max(score, 0.0))

    def _target_allocation(self, risk_score: float) -> float:
        if risk_score <= 1.0 and time.time() >= self._cooldown_until:
            return self.config.base_allocation_usd

        scaled = self.config.base_allocation_usd * min(max(risk_score, 1.0), 4.0)
        return float(min(self.config.max_allocation_usd, scaled))

    def _should_rebalance(self, target_allocation: float) -> bool:
        delta = target_allocation - self.state.current_allocation
        return abs(delta) >= self.config.rebalance_tolerance_usd

    def _rebalance(self, target_allocation: float, price: float, risk_score: float) -> None:
        if price <= 0:
            raise ValueError("Cannot rebalance using non-positive prices")

        delta_usd = Decimal(str(target_allocation)) - Decimal(
            str(self.state.current_allocation)
        )
        if delta_usd == 0:
            return

        side = "BUY" if delta_usd > 0 else "SELL"
        raw_price = Decimal(str(price))
        if raw_price <= 0:
            raise ValueError("Cannot rebalance using non-positive prices")

        raw_quantity = delta_usd.copy_abs() / raw_price

        precision = self._precision_provider.get_precision(self.config.hedge_symbol)

        if precision:
            price_dec = self._apply_price_precision(raw_price, precision.tick_size, side)
            quantity_dec = self._apply_quantity_precision(
                raw_quantity, precision.lot_size
            )
        else:
            price_dec = raw_price
            quantity_dec = raw_quantity

        if quantity_dec <= 0:
            return

        order = HedgeOrder(
            account_id=self.config.account_id,
            client_order_id=f"HEDGE-{uuid.uuid4().hex[:12]}",
            symbol=self.config.hedge_symbol,
            side=side,
            quantity=quantity_dec,
            price=price_dec,
            order_type="market" if price_dec == Decimal("1") else "limit",
            time_in_force="IOC" if side == "SELL" else "GTC",
        )

        logger.info(
            "Rebalancing hedge to %.2f USD via %s %s @ %s (risk_score=%.3f)",
            target_allocation,
            side,
            quantity_dec,
            price_dec,
            risk_score,
        )

        self.oms.submit_hedge_order(order)

        self.timescale.record_instrument_exposure(
            self.config.hedge_symbol, float(delta_usd)
        )
        self.timescale.record_event(
            event_type="hedge.rebalance",
            payload={
                "target_allocation": target_allocation,
                "previous_allocation": self.state.current_allocation,
                "delta_usd": float(delta_usd),
                "quantity": str(quantity_dec),
                "price": str(price_dec),
                "risk_score": risk_score,
                "order_id": order.client_order_id,
            },
        )

        self.state.update_allocation(target_allocation)
        self.state.last_risk_score = risk_score
        if risk_score <= 1.0:
            self._cooldown_until = time.time() + self.config.unwind_cooldown_seconds

    @staticmethod
    def _apply_price_precision(
        price: Decimal, tick_size: Decimal, side: str
    ) -> Decimal:
        if tick_size <= 0:
            return price

        rounding = ROUND_UP if side == "BUY" else ROUND_DOWN
        try:
            snapped = price.quantize(tick_size, rounding=rounding)
        except InvalidOperation:
            exponent = tick_size.as_tuple().exponent
            snapped = price.quantize(Decimal((0, (1,), exponent)), rounding=rounding)
        if snapped <= 0:
            return tick_size
        return snapped

    @staticmethod
    def _apply_quantity_precision(quantity: Decimal, lot_size: Decimal) -> Decimal:
        if lot_size <= 0:
            return quantity

        steps = (quantity / lot_size).to_integral_value(rounding=ROUND_DOWN)
        snapped = steps * lot_size
        if snapped <= 0:
            return Decimal("0")
        return snapped


# ---------------------------------------------------------------------------
# Simple test doubles (useful for unit tests and documentation examples)
# ---------------------------------------------------------------------------


@dataclass
class InMemoryMarketData(MarketDataSource):
    """Simple in-memory market data feed used for tests/examples."""

    symbol: str
    bars: List[PriceBar]

    def recent_bars(self, symbol: str, limit: int) -> Sequence[PriceBar]:
        if symbol != self.symbol:
            return []
        return self.bars[-limit:]


@dataclass
class StaticPnLSource(PnLDataSource):
    """Static drawdown provider suitable for deterministic tests."""

    drawdown: float = 0.0

    def drawdown_pct(self) -> float:
        return float(max(self.drawdown, 0.0))

