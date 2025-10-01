from __future__ import annotations


from collections.abc import Generator

import pytest

from services.common.adapters import TimescaleAdapter
from services.common.schemas import RiskValidationRequest, RiskValidationResponse
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
                volume_24h=5.0e8,
                volatility_30d=0.5,
            ),
            MarketSnapshot(
                base_asset="ETH",
                quote_asset="USD",
                market_cap=8.0e8,
                volume_24h=3.0e8,
                volatility_30d=0.45,
            ),
            MarketSnapshot(
                base_asset="SOL",
                quote_asset="USD",
                market_cap=7.5e8,
                volume_24h=2.6e8,
                volatility_30d=0.5,
            ),
        ]
    )
    yield
    TimescaleAdapter.reset()
    UniverseRepository.seed_market_snapshots(original_snapshots)



def make_request(**overrides: float | str | dict[str, float]) -> RiskValidationRequest:
    payload: dict[str, float | str | dict[str, float]] = {
        "account_id": "admin-eu",
        "instrument": "ETH-USD",
        "net_exposure": 100_000.0,
        "gross_notional": 25_000.0,
        "projected_loss": 10_000.0,
        "projected_fee": 1_000.0,
        "var_95": 50_000.0,
        "spread_bps": 10.0,
        "latency_ms": 100.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }
    payload.update(overrides)
    return RiskValidationRequest(**payload)  # type: ignore[arg-type]


def test_engine_accepts_trade_within_limits() -> None:
    account = "admin-eu"
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
    account = "admin-eu"
    engine = RiskEngine(account_id=account)
    request = make_request(account_id=account, instrument="DOGE-USD")

    response = engine.validate(request)

    assert response.valid is False
    assert any("approved trading universe" in reason for reason in response.reasons)


    events = TimescaleAdapter(account_id=account).events()
    assert any(event["type"] == "universe_rejection" for event in events["events"])



def test_engine_flags_var_breach_and_records_event() -> None:
    account = "admin-eu"
    engine = RiskEngine(account_id=account)
    request = make_request(account_id=account, var_95=500_000.0)

    response = engine.validate(request)

    assert response.valid is False
    assert any("VaR" in reason for reason in response.reasons)


    events = TimescaleAdapter(account_id=account).events()
    assert any(event["type"] == "var_breach" for event in events["events"])
    assert any(event["type"] == "risk_rejected" for event in events["events"])



def test_engine_honors_kill_switch_and_short_circuits() -> None:
    account = "admin-eu"
    adapter = TimescaleAdapter(account_id=account)

    original_config = adapter.load_risk_config()
    try:
        TimescaleAdapter._risk_configs[account]["kill_switch"] = True  # type: ignore[attr-defined]
        engine = RiskEngine(account_id=account)
        request = make_request(account_id=account)


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
    account = "admin-eu"
    engine = RiskEngine(account_id=account)
    request = make_request(account_id=account, instrument="DOGE-USD")

    response = engine.validate(request)

    assert isinstance(response, RiskValidationResponse)
    events = TimescaleAdapter(account_id=account).events()["events"]
    assert all("type" in event and "payload" in event for event in events)

