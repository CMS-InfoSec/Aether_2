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



def make_request(
    *,
    account_id: str = "admin-eu",
    instrument: str = "ETH-USD",
    net_exposure: float = 100_000.0,
    gross_notional: float = 25_000.0,
    projected_loss: float = 10_000.0,
    projected_fee: float = 1_000.0,
    var_95: float = 50_000.0,
    spread_bps: float = 10.0,
    latency_ms: float = 100.0,
    loss_to_date: float = 5_000.0,
    fee_to_date: float = 500.0,
) -> RiskValidationRequest:
    price = 2_000.0
    quantity = max(gross_notional / price, 0.0001)
    fee = {"currency": "USD", "maker": 0.1, "taker": 0.2}

    payload: dict[str, object] = {
        "account_id": account_id,
        "intent": {
            "policy_decision": {
                "request": {
                    "account_id": account_id,
                    "order_id": "order-001",
                    "instrument": instrument,
                    "side": "BUY",
                    "quantity": quantity,
                    "price": price,
                    "fee": fee,
                },
                "response": {
                    "approved": True,
                    "reason": None,
                    "effective_fee": fee,
                    "expected_edge_bps": 25.0,
                    "fee_adjusted_edge_bps": 24.5,
                    "selected_action": "maker",
                    "action_templates": [
                        {
                            "name": "maker",
                            "venue_type": "maker",
                            "edge_bps": 25.0,
                            "fee_bps": 5.0,
                            "confidence": 0.9,
                        }
                    ],
                    "confidence": {
                        "model_confidence": 0.9,
                        "state_confidence": 0.85,
                        "execution_confidence": 0.88,
                    },
                    "features": [0.1, 0.2, 0.3],
                    "book_snapshot": {
                        "mid_price": price,
                        "spread_bps": spread_bps,
                        "imbalance": 0.05,
                    },
                    "state": {
                        "regime": "normal",
                        "volatility": 0.4,
                        "liquidity_score": 0.8,
                        "conviction": 0.6,
                    },
                    "take_profit_bps": 30.0,
                    "stop_loss_bps": 15.0,
                },
            },
            "metrics": {
                "net_exposure": net_exposure,
                "gross_notional": gross_notional,
                "projected_loss": projected_loss,
                "projected_fee": projected_fee,
                "var_95": var_95,
                "spread_bps": spread_bps,
                "latency_ms": latency_ms,
            },
        },
        "portfolio_state": {
            "nav": 1_000_000.0,
            "loss_to_date": loss_to_date,
            "fee_to_date": fee_to_date,
            "instrument_exposure": {instrument: 100_000.0},
        },
    }

    return RiskValidationRequest(**payload)


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

