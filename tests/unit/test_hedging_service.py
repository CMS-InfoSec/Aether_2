from __future__ import annotations

from decimal import Decimal

import pytest

from decimal import Decimal
from pathlib import Path

import pytest

import hedging_service as hs
from shared import hedge_logging


class _StubMarketData:
    def recent_bars(self, symbol: str, limit: int):  # pragma: no cover - helper stub
        return []


class _StubPnL:
    def drawdown_pct(self) -> float:  # pragma: no cover - helper stub
        return 0.0


class _RecordingOMS:
    def __init__(self) -> None:
        self.orders: list[hs.HedgeOrder] = []

    def submit_hedge_order(self, order: hs.HedgeOrder) -> None:
        self.orders.append(order)


class _StaticPrecisionProvider:
    def __init__(self, precision: hs.InstrumentPrecision | None) -> None:
        self._precision = precision

    def get_precision(self, symbol: str) -> hs.InstrumentPrecision | None:
        return self._precision


class _StubTimescale:
    def record_instrument_exposure(self, symbol: str, delta: float) -> None:  # pragma: no cover - helper stub
        return None

    def record_event(self, event_type: str, payload: dict) -> None:  # pragma: no cover - helper stub
        return None


@pytest.fixture()
def hedging_config() -> hs.HedgeConfig:
    return hs.HedgeConfig(
        account_id="acct-1",
        hedge_symbol="eth/usd",
        base_allocation_usd=1_000.0,
        max_allocation_usd=10_000.0,
        rebalance_tolerance_usd=0.0,
    )


@pytest.fixture()
def precision_provider() -> _StaticPrecisionProvider:
    precision = hs.InstrumentPrecision(
        tick_size=Decimal("0.000001"),
        lot_size=Decimal("0.01"),
    )
    return _StaticPrecisionProvider(precision)


@pytest.fixture()
def hedging_service(
    hedging_config: hs.HedgeConfig,
    precision_provider: _StaticPrecisionProvider,
) -> hs.HedgingService:
    return hs.HedgingService(
        hedging_config,
        market_data=_StubMarketData(),
        pnl_source=_StubPnL(),
        oms_client=_RecordingOMS(),
        timescale=_StubTimescale(),
        precision_provider=precision_provider,
    )


def _desired_delta(price: float, quantity: Decimal) -> float:
    return float(Decimal(str(price)) * quantity)


def test_rebalance_quantizes_buy_with_high_precision(
    hedging_service: hs.HedgingService,
) -> None:
    service = hedging_service
    assert isinstance(service.oms, _RecordingOMS)
    recording_oms = service.oms
    price = 0.0654321
    quantity = Decimal("12.34567")

    target_allocation = service.state.current_allocation + _desired_delta(price, quantity)

    service._rebalance(
        target_allocation,
        price,
        risk_score=2.5,
        drawdown_pct=0.05,
        atr=None,
        realized_vol=None,
    )

    assert len(recording_oms.orders) == 1
    order = recording_oms.orders[0]
    assert order.side == "BUY"
    assert order.order_type == "limit"
    assert order.time_in_force == "GTC"
    assert isinstance(order.quantity, Decimal)
    assert isinstance(order.price, Decimal)
    assert order.quantity == Decimal("12.34")
    assert order.price == Decimal("0.065433")


def test_rebalance_quantizes_sell_with_high_precision(
    hedging_service: hs.HedgingService,
) -> None:
    service = hedging_service
    assert isinstance(service.oms, _RecordingOMS)
    recording_oms = service.oms
    price = 0.0654321
    quantity = Decimal("12.34567")

    delta = _desired_delta(price, quantity)
    service.state.current_allocation += delta
    target_allocation = service.state.current_allocation - delta

    service._rebalance(
        target_allocation,
        price,
        risk_score=3.0,
        drawdown_pct=0.12,
        atr=1.4,
        realized_vol=0.3,
    )

    assert len(recording_oms.orders) == 1
    order = recording_oms.orders[0]
    assert order.side == "SELL"
    assert order.order_type == "limit"
    assert order.time_in_force == "IOC"
    assert order.quantity == Decimal("12.34")
    assert order.price == Decimal("0.065432")


def test_rebalance_requires_precision_metadata(
    hedging_config: hs.HedgeConfig,
) -> None:
    service = hs.HedgingService(
        hedging_config,
        market_data=_StubMarketData(),
        pnl_source=_StubPnL(),
        oms_client=_RecordingOMS(),
        timescale=_StubTimescale(),
        precision_provider=_StaticPrecisionProvider(None),
    )

    with pytest.raises(RuntimeError, match="Precision metadata unavailable"):
        service._rebalance(
            hedging_config.base_allocation_usd + 100.0,
            price=0.065,
            risk_score=1.2,
            drawdown_pct=0.02,
            atr=None,
            realized_vol=None,
        )


def test_hedge_config_normalizes_spot_symbol() -> None:
    config = hs.HedgeConfig(account_id="acct-2", hedge_symbol="  btc_usd  ")
    assert config.hedge_symbol == "BTC-USD"


def test_hedge_config_rejects_derivative_symbols() -> None:
    with pytest.raises(ValueError, match="spot market pair"):
        hs.HedgeConfig(account_id="acct-3", hedge_symbol="BTC-PERP")


def test_rebalance_logs_hedge_event(
    hedging_service: hs.HedgingService, tmp_path: Path
) -> None:
    service = hedging_service
    assert isinstance(service.oms, _RecordingOMS)

    class _CapturingHedgeLogger(hedge_logging.HedgeLogger):
        def __init__(self, path: Path) -> None:
            super().__init__(path=path)
            self.entries: list[hedge_logging.HedgeLogEntry] = []

        def log(self, entry: hedge_logging.HedgeLogEntry) -> None:
            self.entries.append(entry)
            super().log(entry)

    capture_logger = _CapturingHedgeLogger(tmp_path / "hedge.csv")

    target_allocation = service.state.current_allocation + 2500.0
    price = 1.001

    with hedge_logging.override_hedge_logger(capture_logger):
        service._rebalance(
            target_allocation,
            price,
            risk_score=1.8,
            drawdown_pct=0.15,
            atr=1.1,
            realized_vol=0.45,
        )

    assert len(capture_logger.entries) == 1
    entry = capture_logger.entries[0]
    assert entry.account_id == service.config.account_id
    assert entry.symbol == service.config.hedge_symbol
    assert entry.target_allocation == Decimal(str(target_allocation))
    assert entry.drawdown_pct == Decimal("0.15")
    assert entry.risk_score == Decimal("1.8")
    assert entry.order_id and entry.order_id.startswith("HEDGE-")
