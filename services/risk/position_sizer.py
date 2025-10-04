"""Position sizing utilities for the risk service."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Protocol

from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from services.common.precision import (
    PrecisionMetadataUnavailable,
    precision_provider,
)

logger = logging.getLogger(__name__)


DEFAULT_MIN_VOLATILITY = 1e-4
DEFAULT_FALLBACK_VOLATILITY = 0.35
DEFAULT_MIN_NOTIONAL = 10.0
DEFAULT_SLIPPAGE_BPS = 2.0
DEFAULT_SAFETY_MARGIN_BPS = 5.0


class SupportsRiskBudget(Protocol):
    """Protocol representing the subset of risk limit attributes we require."""

    max_nav_pct_per_trade: float
    notional_cap: float


@dataclass(slots=True)
class PositionSizingConfig:
    """Configuration parameters controlling position sizing behaviour."""

    max_trade_risk_pct_nav: float
    max_trade_risk_pct_cash: float
    volatility_floor: float
    slippage_bps: float
    safety_margin_bps: float
    min_notional_usd: float


@dataclass(slots=True)
class PositionSizeResult:
    """Result returned by :class:`PositionSizer.suggest_max_position`."""

    account_id: str
    symbol: str
    nav: float
    available_balance: float
    volatility: float
    risk_budget: float
    base_size_usd: float
    max_size_usd: float
    size_units: float
    reason: str
    expected_edge_bps: Optional[float]
    fee_bps_estimate: Optional[float]
    slippage_bps: float
    safety_margin_bps: float
    regime: Optional[str]
    price: Optional[float]
    timestamp: datetime
    diagnostics: Dict[str, float | str | bool | None] = field(default_factory=dict)

    @property
    def max_position(self) -> float:
        """Backward compatible alias for ``max_size_usd``."""

        return self.max_size_usd


class PositionSizer:
    """Calculate adaptive position size limits for an account."""

    def __init__(
        self,
        account_id: str,
        *,
        limits: SupportsRiskBudget,
        timescale: TimescaleAdapter | None = None,
        feature_store: RedisFeastAdapter | None = None,
        fallback_volatility: float = DEFAULT_FALLBACK_VOLATILITY,
        min_volatility: float = DEFAULT_MIN_VOLATILITY,
        log_callback: Callable[[str, str, float, float, datetime], None] | None = None,
    ) -> None:
        self.account_id = account_id
        self._limits = limits
        self._timescale = timescale or TimescaleAdapter(account_id=account_id)
        self._feature_store = feature_store or RedisFeastAdapter(account_id=account_id)
        self._fallback_volatility = max(float(fallback_volatility), DEFAULT_MIN_VOLATILITY)
        self._min_volatility = max(float(min_volatility), DEFAULT_MIN_VOLATILITY)
        self._log_callback = log_callback
        self._sizing_config: PositionSizingConfig | None = None

    def suggest_max_position(
        self,
        symbol: str,
        *,
        nav: float | None = None,
        available_balance: float | None = None,
        volatility: float | None = None,
        expected_edge_bps: float | None = None,
        fee_bps_estimate: float | None = None,
        price: float | None = None,
        regime: str | None = None,
    ) -> PositionSizeResult:
        """Return the suggested maximum notional for ``symbol`` with diagnostics."""

        config = self._load_sizing_config()
        resolved_nav = self._resolve_nav(nav)
        resolved_cash = self._resolve_cash(available_balance, resolved_nav)
        symbol_payload = self._feature_store.fetch_online_features(symbol)

        resolved_price = self._resolve_price(price, symbol_payload)
        resolved_volatility = self._resolve_volatility(symbol, volatility, symbol_payload)
        resolved_regime = self._resolve_regime(regime, symbol_payload)
        resolved_edge = self._resolve_expected_edge(expected_edge_bps, symbol_payload)
        resolved_fee = self._resolve_fee_estimate(fee_bps_estimate, symbol)

        nav_risk_budget = resolved_nav * config.max_trade_risk_pct_nav
        cash_risk_budget = resolved_cash * config.max_trade_risk_pct_cash
        risk_budget = min(nav_risk_budget, cash_risk_budget)
        notional_cap = max(float(getattr(self._limits, "notional_cap", 0.0)), 0.0)
        if notional_cap > 0.0:
            risk_budget = min(risk_budget, notional_cap)

        diagnostics: Dict[str, float | str | bool | None] = {
            "nav_risk_budget": nav_risk_budget,
            "cash_risk_budget": cash_risk_budget,
            "notional_cap": notional_cap or None,
            "volatility_floor": config.volatility_floor,
            "available_balance": resolved_cash,
            "nav": resolved_nav,
        }

        if resolved_price is None:
            diagnostics["mid_price_missing"] = True
        else:
            diagnostics["mid_price"] = resolved_price

        if risk_budget <= 0.0:
            reason = "no_risk_budget"
            result = self._finalize_result(
                symbol,
                resolved_nav,
                resolved_cash,
                resolved_volatility,
                risk_budget,
                0.0,
                0.0,
                0.0,
                reason,
                resolved_edge,
                resolved_fee,
                config,
                resolved_regime,
                resolved_price,
                diagnostics,
            )
            self._log(symbol, resolved_volatility, result.max_size_usd, result.timestamp)
            return result

        volatility_denominator = max(resolved_volatility, config.volatility_floor)
        diagnostics["volatility_input"] = resolved_volatility
        base_size_usd = risk_budget / volatility_denominator if volatility_denominator else 0.0
        diagnostics["base_size_usd"] = base_size_usd

        if notional_cap > 0.0:
            base_size_usd = min(base_size_usd, notional_cap)

        try:
            lot_size = self._resolve_lot_size(symbol)
        except PrecisionMetadataUnavailable:
            diagnostics["precision_metadata_missing"] = True
            reason = "precision_metadata_missing"
            result = self._finalize_result(
                symbol,
                resolved_nav,
                resolved_cash,
                resolved_volatility,
                risk_budget,
                base_size_usd,
                0.0,
                0.0,
                reason,
                resolved_edge,
                resolved_fee,
                config,
                resolved_regime,
                resolved_price,
                diagnostics,
            )
            self._log(symbol, resolved_volatility, result.max_size_usd, result.timestamp)
            return result

        gate_threshold = (resolved_fee or 0.0) + config.slippage_bps + config.safety_margin_bps
        diagnostics["edge_threshold_bps"] = gate_threshold

        if resolved_edge is not None:
            diagnostics["expected_edge_bps"] = resolved_edge
            if resolved_edge <= gate_threshold:
                reason = "insufficient_edge"
                result = self._finalize_result(
                    symbol,
                    resolved_nav,
                    resolved_cash,
                    resolved_volatility,
                    risk_budget,
                    base_size_usd,
                    0.0,
                    0.0,
                    reason,
                    resolved_edge,
                    resolved_fee,
                    config,
                    resolved_regime,
                    resolved_price,
                    diagnostics,
                )
                self._log(symbol, resolved_volatility, result.max_size_usd, result.timestamp)
                return result

        max_size_usd = base_size_usd
        size_units = 0.0

        if resolved_price and resolved_price > 0.0:
            raw_units = max_size_usd / resolved_price
            if lot_size > 0:
                raw_units = math.floor(raw_units / lot_size) * lot_size
            size_units = max(raw_units, 0.0)
            max_size_usd = size_units * resolved_price

        if max_size_usd < config.min_notional_usd:
            diagnostics["min_notional_usd"] = config.min_notional_usd
            reason = "below_min_notional"
            size_units = 0.0
            max_size_usd = 0.0
        else:
            reason = "sized"

        result = self._finalize_result(
            symbol,
            resolved_nav,
            resolved_cash,
            resolved_volatility,
            risk_budget,
            base_size_usd,
            max_size_usd,
            size_units,
            reason,
            resolved_edge,
            resolved_fee,
            config,
            resolved_regime,
            resolved_price,
            diagnostics,
        )
        self._log(symbol, resolved_volatility, result.max_size_usd, result.timestamp)
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_sizing_config(self) -> PositionSizingConfig:
        if self._sizing_config is not None:
            return self._sizing_config

        raw_config = self._timescale.load_risk_config()
        source: Mapping[str, Any]
        if isinstance(raw_config, Mapping) and isinstance(raw_config.get("position_sizer"), Mapping):
            source = raw_config["position_sizer"]  # type: ignore[assignment]
        elif isinstance(raw_config, Mapping):
            source = raw_config
        else:
            source = {}

        nav_pct = self._coerce_float(
            source.get("max_trade_risk_pct_nav"),
            default=float(getattr(self._limits, "max_nav_pct_per_trade", 0.0)),
        )
        cash_pct = self._coerce_float(
            source.get("max_trade_risk_pct_cash"),
            default=float(getattr(self._limits, "max_nav_pct_per_trade", 0.0)),
        )
        volatility_floor = max(
            self._coerce_float(source.get("volatility_floor"), default=self._min_volatility),
            self._min_volatility,
        )
        slippage_bps = max(
            self._coerce_float(source.get("slippage_bps"), default=DEFAULT_SLIPPAGE_BPS),
            0.0,
        )
        safety_margin_bps = max(
            self._coerce_float(
                source.get("safety_margin_bps"), default=DEFAULT_SAFETY_MARGIN_BPS
            ),
            0.0,
        )
        min_notional = max(
            self._coerce_float(source.get("min_trade_notional"), default=DEFAULT_MIN_NOTIONAL),
            0.0,
        )

        self._sizing_config = PositionSizingConfig(
            max_trade_risk_pct_nav=nav_pct,
            max_trade_risk_pct_cash=cash_pct,
            volatility_floor=volatility_floor,
            slippage_bps=slippage_bps,
            safety_margin_bps=safety_margin_bps,
            min_notional_usd=min_notional,
        )
        return self._sizing_config

    def _resolve_nav(self, nav: float | None) -> float:
        if nav is not None and nav > 0:
            return float(nav)
        config = self._timescale.load_risk_config()
        raw_nav = config.get("nav") if isinstance(config, Mapping) else None
        try:
            value = float(raw_nav) if raw_nav is not None else 0.0
        except (TypeError, ValueError):  # pragma: no cover - defensive casting
            value = 0.0
        return max(value, 0.0)

    def _resolve_cash(self, available_balance: float | None, fallback: float) -> float:
        if available_balance is not None and available_balance > 0:
            return float(available_balance)
        return max(float(fallback), 0.0)

    def _resolve_volatility(
        self,
        symbol: str,
        volatility: float | None,
        payload: Mapping[str, Any] | None,
    ) -> float:
        if volatility is not None and volatility > 0:
            return float(volatility)
        resolved: Optional[float] = None
        if isinstance(payload, Mapping):
            state = payload.get("state")
            if isinstance(state, Mapping):
                resolved = self._coerce_float(state.get("volatility"))
        if resolved is None or resolved <= 0.0:
            resolved = self._fallback_volatility
        return float(max(resolved, self._min_volatility))

    @staticmethod
    def _resolve_regime(regime: str | None, payload: Mapping[str, Any] | None) -> Optional[str]:
        if regime:
            return regime
        if isinstance(payload, Mapping):
            state = payload.get("state")
            if isinstance(state, Mapping):
                value = state.get("regime")
                if isinstance(value, str):
                    return value
        return None

    @staticmethod
    def _resolve_expected_edge(
        expected_edge_bps: float | None, payload: Mapping[str, Any] | None
    ) -> Optional[float]:
        if expected_edge_bps is not None:
            try:
                return float(expected_edge_bps)
            except (TypeError, ValueError):  # pragma: no cover - defensive casting
                return None
        if isinstance(payload, Mapping):
            raw = payload.get("expected_edge_bps")
            try:
                if raw is not None:
                    return float(raw)
            except (TypeError, ValueError):  # pragma: no cover - defensive casting
                return None
        return None

    def _resolve_price(
        self, price: float | None, payload: Mapping[str, Any] | None
    ) -> Optional[float]:
        if price is not None and price > 0:
            return float(price)
        if isinstance(payload, Mapping):
            snapshot = payload.get("book_snapshot")
            if isinstance(snapshot, Mapping):
                mid = self._coerce_float(snapshot.get("mid_price"))
                if mid is not None and mid > 0:
                    return mid
        return None

    def _resolve_fee_estimate(self, fee_bps_estimate: float | None, symbol: str) -> Optional[float]:
        if fee_bps_estimate is not None:
            try:
                value = float(fee_bps_estimate)
            except (TypeError, ValueError):  # pragma: no cover - defensive casting
                value = None
            else:
                return max(value, 0.0)

        try:
            override = self._feature_store.fee_override(symbol)
        except AttributeError:  # pragma: no cover - legacy adapter compatibility
            override = None

        if isinstance(override, Mapping):
            maker = self._coerce_float(override.get("maker"), default=0.0) or 0.0
            taker = self._coerce_float(override.get("taker"), default=0.0) or 0.0
            return max(maker, taker, 0.0)
        return None

    def _resolve_lot_size(self, symbol: str) -> float:

        metadata = precision_provider.require(symbol)
        native_pair = metadata.get("native_pair")

        lot_size = metadata.get("lot")
        try:
            value = float(lot_size) if lot_size is not None else 0.0
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive casting

            pair = native_pair or symbol
            raise PrecisionMetadataUnavailable(f"Invalid lot size for {pair}") from exc
        if value <= 0:
            pair = native_pair or symbol
            raise PrecisionMetadataUnavailable(f"Invalid lot size for {pair}")

        return value

    def _finalize_result(
        self,
        symbol: str,
        nav: float,
        available_balance: float,
        volatility: float,
        risk_budget: float,
        base_size_usd: float,
        max_size_usd: float,
        size_units: float,
        reason: str,
        expected_edge_bps: Optional[float],
        fee_bps_estimate: Optional[float],
        config: PositionSizingConfig,
        regime: Optional[str],
        price: Optional[float],
        diagnostics: Dict[str, float | str | bool | None],
    ) -> PositionSizeResult:
        timestamp = datetime.now(timezone.utc)
        result = PositionSizeResult(
            account_id=self.account_id,
            symbol=symbol.upper(),
            nav=nav,
            available_balance=available_balance,
            volatility=volatility,
            risk_budget=risk_budget,
            base_size_usd=base_size_usd,
            max_size_usd=max_size_usd,
            size_units=size_units,
            reason=reason,
            expected_edge_bps=expected_edge_bps,
            fee_bps_estimate=fee_bps_estimate,
            slippage_bps=config.slippage_bps,
            safety_margin_bps=config.safety_margin_bps,
            regime=regime,
            price=price,
            timestamp=timestamp,
            diagnostics=diagnostics,
        )
        return result

    def _log(self, symbol: str, volatility: float, size: float, timestamp: datetime) -> None:
        if self._log_callback is None:
            return
        try:
            self._log_callback(self.account_id, symbol, volatility, size, timestamp)
        except Exception:  # pragma: no cover - defensive logging
            logger.exception(
                "Failed to record position size adjustment for account=%s symbol=%s",
                self.account_id,
                symbol,
            )

    @staticmethod
    def _coerce_float(value: Any, *, default: float | None = None) -> Optional[float]:
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
