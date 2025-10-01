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


def _intent() -> Intent:
    return Intent(
        edge_bps=22.0,
        confidence=ConfidenceMetrics(
            model_confidence=0.8,
            state_confidence=0.78,
            execution_confidence=0.76,
            overall_confidence=0.8,
        ),
        take_profit_bps=25.0,
        stop_loss_bps=12.0,
        selected_action="maker",
        action_templates=[
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=18.0,
                fee_bps=0.0,
                confidence=0.9,
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=12.0,
                fee_bps=0.0,
                confidence=0.85,
            ),
        ],
        approved=True,
        reason=None,
    )


def test_policy_service_decision(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    async def _fake_fee(account_id: str, symbol: str, liquidity: str, notional: float) -> float:
        assert account_id == "company"
        assert symbol == "BTC-USD"
        assert liquidity == "maker"
        expected_notional = float(Decimal("30120.5") * Decimal("0.1235"))
        assert notional == pytest.approx(expected_notional)
        return 4.5

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: _intent())

    payload = {
        "account_id": "company",
        "symbol": "BTC-USD",
        "side": "buy",
        "qty": 0.1234567,
        "price": 30120.4567,
        "impact_bps": 1.0,
        "features": [0.4, -0.1, 2.8],
        "book_snapshot": {"mid_price": 30125.4, "spread_bps": 2.4, "imbalance": 0.05},
    }

    response = client.post("/policy/decide", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["approved"] is True
    assert body["action"] == "maker"
    assert body["qty"] == pytest.approx(0.1235)
    assert body["price"] == pytest.approx(30120.5)
    assert body["expected_edge_bps"] == pytest.approx(18.0)
    assert body["effective_fee_bps"] == pytest.approx(4.5)
    assert body["expected_cost_bps"] == pytest.approx(7.9)
