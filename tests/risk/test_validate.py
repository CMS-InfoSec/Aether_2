from __future__ import annotations


from collections.abc import Generator
from typing import Any

import pytest

from services.common.adapters import TimescaleAdapter
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

from services.universe.repository import MarketSnapshot, UniverseRepository


@pytest.fixture(autouse=True)
def reset_state() -> Generator[None, None, None]:
    TimescaleAdapter.reset()
    original_snapshots = list(UniverseRepository._market_snapshots.values())  # type: ignore[attr-defined]
    UniverseRepository.seed_market_snapshots(
        [
            MarketSnapshot(
                base_asset="BTC",
                quote_asset="USD",
                market_cap=1.0e9,
                global_volume_24h=5.0e8,
                kraken_volume_24h=2.5e8,
                volatility_30d=0.5,
            ),
            MarketSnapshot(
                base_asset="ETH",
                quote_asset="USD",
                market_cap=1.6e9,
                global_volume_24h=3.0e8,
                kraken_volume_24h=1.6e8,
                volatility_30d=0.45,
            ),
            MarketSnapshot(
                base_asset="SOL",
                quote_asset="USD",
                market_cap=1.1e9,
                global_volume_24h=2.6e8,
                kraken_volume_24h=1.3e8,
                volatility_30d=0.5,
            ),
        ]
    )
    yield
    TimescaleAdapter.reset()
    UniverseRepository.seed_market_snapshots(original_snapshots)




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


def test_engine_accepts_trade_within_limits() -> None:
    account = "company"
    engine = RiskEngine(account_id=account)
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
    engine = RiskEngine(account_id=account)
    request = make_request(account_id=account, instrument="DOGE-USD")

    response = engine.validate(request)

    assert response.valid is False
    assert any("approved trading universe" in reason for reason in response.reasons)


    events = TimescaleAdapter(account_id=account).events()
    assert any(event["type"] == "universe_rejection" for event in events["events"])



def test_engine_flags_var_breach_and_records_event() -> None:
    account = "company"
    engine = RiskEngine(account_id=account)
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
        engine = RiskEngine(account_id=account)
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
    engine = RiskEngine(account_id=account)
    request = make_request(account_id=account, instrument="DOGE-USD")

    response = engine.validate(request)

    assert isinstance(response, RiskValidationResponse)
    events = TimescaleAdapter(account_id=account).events()["events"]
    assert all("type" in event and "payload" in event for event in events)

