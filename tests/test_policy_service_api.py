from __future__ import annotations

from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

import policy_service
from services.common.schemas import ActionTemplate, ConfidenceMetrics
from services.models.model_server import Intent


@pytest.fixture(name="client")
def _client() -> TestClient:
    return TestClient(policy_service.app)


def _intent(
    *,
    edge_bps: float,
    maker_edge: float,
    taker_edge: float,
    approved: bool,
    selected: str,
    reason: str | None = None,
) -> Intent:
    return Intent(
        edge_bps=edge_bps,
        confidence=ConfidenceMetrics(
            model_confidence=0.8,
            state_confidence=0.78,
            execution_confidence=0.76,
            overall_confidence=0.8,
        ),
        take_profit_bps=25.0,
        stop_loss_bps=12.0,
        selected_action=selected,
        action_templates=[
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=maker_edge,
                fee_bps=0.0,
                confidence=0.9,
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=taker_edge,
                fee_bps=0.0,
                confidence=0.85,
            ),
        ],
        approved=approved,
        reason=reason,
    )


def test_policy_decide_approves_when_edge_beats_costs(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    recorded: dict[str, object] = {}

    async def _fake_fee(account_id: str, symbol: str, liquidity: str, notional: float) -> float:
        recorded.update(
            {
                "account_id": account_id,
                "symbol": symbol,
                "liquidity": liquidity,
                "notional": notional,
            }
        )
        return 4.5

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(
        policy_service,
        "predict_intent",
        lambda **_: _intent(
            edge_bps=22.0,
            maker_edge=18.0,
            taker_edge=12.0,
            approved=True,
            selected="maker",
        ),
    )

    payload = {
        "account_id": "company",
        "symbol": "btc-usd",
        "side": "buy",
        "qty": 0.1234567,
        "price": 30120.4567,
        "impact_bps": 1.0,
        "features": {"alpha": 0.4, "beta": -0.1, "gamma": 2.8},
        "book_snapshot": {"mid_price": 30125.4, "spread_bps": 2.4, "imbalance": 0.05},
    }

    response = client.post("/policy/decide", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["approved"] is True
    assert data["action"] == "maker"
    assert data["side"] == "buy"
    assert data["qty"] == pytest.approx(0.1235)
    assert data["price"] == pytest.approx(30120.5)
    assert data["limit_px"] == pytest.approx(30120.5)
    assert data["effective_fee_bps"] == pytest.approx(4.5)
    assert data["expected_edge_bps"] == pytest.approx(18.0)
    assert data["expected_cost_bps"] == pytest.approx(7.9)

    expected_notional = float(Decimal("30120.5") * Decimal("0.1235"))
    assert recorded == {
        "account_id": "company",
        "symbol": "BTC-USD",
        "liquidity": "maker",
        "notional": pytest.approx(expected_notional),
    }


def test_policy_decide_rejects_when_costs_exceed_edge(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    async def _fake_fee(*_: object, **__: object) -> float:
        return 9.0

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(
        policy_service,
        "predict_intent",
        lambda **_: _intent(
            edge_bps=10.0,
            maker_edge=8.0,
            taker_edge=6.0,
            approved=True,
            selected="maker",
        ),
    )

    payload = {
        "account_id": "company",
        "symbol": "BTC-USD",
        "side": "sell",
        "qty": 0.25,
        "price": 25000.0,
        "impact_bps": 3.0,
        "features": [0.1, 0.2],
        "book_snapshot": {"mid_price": 25010.0, "spread_bps": 4.0, "imbalance": -0.2},
    }

    response = client.post("/policy/decide", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["approved"] is False
    assert data["action"] == "hold"
    assert data["side"] == "none"
    assert data["qty"] == 0.0
    assert data["reason"] == "Fee-adjusted edge non-positive"


def test_health_endpoints(client: TestClient) -> None:
    health = client.get("/health")
    ready = client.get("/ready")

    assert health.status_code == 200
    assert ready.status_code == 200
    assert health.json() == {"status": "ok"}
    assert ready.json() == {"status": "ready"}

