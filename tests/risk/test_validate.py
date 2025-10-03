from __future__ import annotations


from collections.abc import Generator
from typing import Any

import pytest

from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from services.common.schemas import (
    FeeBreakdown,
    PolicyDecisionPayload,
    PolicyDecisionRequest,
    PortfolioState,
    RiskIntentMetrics,
    RiskIntentPayload,
    RiskValidationRequest,
    RiskValidationResponse,
)
from services.risk.engine import RiskEngine

class StubUniverseRepository:
    def __init__(self) -> None:
        self._approved: list[str] = []
        self._fees: dict[str, dict[str, float | str]] = {}

    def configure(
        self,
        approved: list[str],
        fees: dict[str, dict[str, float | str]],
    ) -> None:
        self._approved = list(approved)
        self._fees = {symbol: dict(payload) for symbol, payload in fees.items()}

    def approved_universe(self) -> list[str]:
        return list(self._approved)

    def fee_override(self, instrument: str) -> dict[str, float | str] | None:
        override = self._fees.get(instrument) or self._fees.get("default")
        return dict(override) if override else None


STUB_UNIVERSE = StubUniverseRepository()


@pytest.fixture(autouse=True)
def reset_state() -> Generator[None, None, None]:
    TimescaleAdapter.reset()
    STUB_UNIVERSE.configure(
        ["BTC-USD", "ETH-USD", "SOL-USD"],
        {
            "BTC-USD": {"currency": "USD", "maker": 0.1, "taker": 0.2},
            "ETH-USD": {"currency": "USD", "maker": 0.12, "taker": 0.24},
            "SOL-USD": {"currency": "USD", "maker": 0.15, "taker": 0.3},
            "default": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        },
    )
    yield
    TimescaleAdapter.reset()




def make_request(**overrides: Any) -> RiskValidationRequest:
    account_id = str(overrides.pop("account_id", "company"))
    instrument = str(overrides.pop("instrument", "ETH-USD"))

    scalar_defaults = {
        "net_exposure": 100_000.0,
        "gross_notional": 25_000.0,
        "projected_loss": 10_000.0,
        "projected_fee": 1_000.0,
        "var_95": 50_000.0,
        "spread_bps": 10.0,
        "latency_ms": 100.0,
    }

    scalars: dict[str, float] = {}
    for key, default in scalar_defaults.items():
        value = overrides.pop(key, default)
        scalars[key] = float(value)

    raw_fee = overrides.pop("fee", {"currency": "USD", "maker": 0.1, "taker": 0.2})
    fee = FeeBreakdown(**raw_fee) if isinstance(raw_fee, dict) else raw_fee

    policy_request = PolicyDecisionRequest(
        account_id=account_id,
        order_id="ABC-123",
        instrument=instrument,
        side="BUY",
        quantity=1.0,
        price=25_000.0,
        fee=fee,
    )

    metrics = RiskIntentMetrics(**scalars)
    intent = RiskIntentPayload(
        policy_decision=PolicyDecisionPayload(request=policy_request),
        metrics=metrics,
    )

    portfolio_state = PortfolioState(
        nav=10_000_000.0,
        loss_to_date=5_000.0,
        fee_to_date=2_000.0,
        instrument_exposure={instrument: scalars["gross_notional"] / 2},
    )

    request_payload = {
        "account_id": account_id,
        "intent": intent,
        "portfolio_state": portfolio_state,
        "instrument": instrument,
        "fee": fee,
        **scalars,
    }

    # Propagate any remaining overrides directly.
    request_payload.update(overrides)

    return RiskValidationRequest(**request_payload)


def make_engine(account_id: str) -> RiskEngine:
    universe = RedisFeastAdapter(account_id=account_id, repository=STUB_UNIVERSE)
    return RiskEngine(account_id=account_id, universe_source=universe)


def test_engine_accepts_trade_within_limits() -> None:
    account = "company"
    engine = make_engine(account)
    request = make_request(account_id=account)

    response = engine.validate(request)

    assert isinstance(response, RiskValidationResponse)
    assert response.valid is True
    assert response.reasons == []

    metrics = TimescaleAdapter(account_id=account).get_daily_usage()
    assert metrics["loss"] == pytest.approx(request.projected_loss)
    assert metrics["fee"] == pytest.approx(request.projected_fee)

    exposure = TimescaleAdapter(account_id=account).instrument_exposure(request.instrument)
    assert exposure == pytest.approx(abs(request.gross_notional))


def test_engine_rejects_non_whitelisted_instrument() -> None:
    account = "company"
    engine = make_engine(account)
    request = make_request(account_id=account, instrument="DOGE-USD")

    response = engine.validate(request)

    assert response.valid is False
    assert any("approved trading universe" in reason for reason in response.reasons)


    events = TimescaleAdapter(account_id=account).events()
    assert any(event["type"] == "universe_rejection" for event in events["events"])



def test_engine_flags_var_breach_and_records_event() -> None:
    account = "company"
    engine = make_engine(account)
    request = make_request(account_id=account, var_95=500_000.0)

    response = engine.validate(request)

    assert response.valid is False
    assert any("VaR" in reason for reason in response.reasons)


    events = TimescaleAdapter(account_id=account).events()
    assert any(event["type"] == "var_breach" for event in events["events"])
    assert any(event["type"] == "risk_rejected" for event in events["events"])



def test_engine_honors_kill_switch_and_short_circuits() -> None:
    account = "company"
    adapter = TimescaleAdapter(account_id=account)
    original_config = adapter.load_risk_config()
    try:
        TimescaleAdapter._risk_configs[account]["kill_switch"] = True  # type: ignore[attr-defined]
        engine = make_engine(account)
        request = make_request(account_id=account)

        response = engine.validate(request)

        assert response.valid is False
        assert response.reasons == ["Risk kill switch engaged for account"]

        events = TimescaleAdapter(account_id=account).events()
        assert any(event["type"] == "kill_switch_triggered" for event in events["events"])
    finally:
        TimescaleAdapter._risk_configs[account] = original_config  # type: ignore[attr-defined]


def test_engine_records_events_without_exception() -> None:
    account = "company"
    engine = make_engine(account)
    request = make_request(account_id=account, instrument="DOGE-USD")

    response = engine.validate(request)

    assert isinstance(response, RiskValidationResponse)
    events = TimescaleAdapter(account_id=account).events()["events"]
    assert all("type" in event and "payload" in event for event in events)

