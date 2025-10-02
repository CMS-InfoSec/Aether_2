"""Unit tests for the policy decision services."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

import tests.factories as factories
from policy_service import app as decision_app
from services.policy import policy_service as intent_service


class DummyIntent:
    """Simple intent payload used to stub the model server."""

    def __init__(self, *, approved: bool = True, edge: float = 30.0, reason: str | None = None):
        self.confidence = factories.confidence(overall_confidence=0.9)
        self.action_templates = factories.action_templates()
        self.selected_action = "maker"
        self.approved = approved
        self.reason = reason
        self.edge_bps = edge
        self.take_profit_bps = 80.0
        self.stop_loss_bps = 40.0


@pytest.fixture
def policy_test_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Create a test client with the model server stubbed out."""

    stub_intent = DummyIntent()

    def fake_predict_intent(**_: Any) -> DummyIntent:
        return stub_intent

    async def fake_fetch(*args: Any, **kwargs: Any) -> Decimal:
        del args, kwargs
        return Decimal("5.0")

    monkeypatch.setattr("policy_service.predict_intent", fake_predict_intent)
    monkeypatch.setattr("policy_service._fetch_effective_fee", fake_fetch)
    return TestClient(decision_app)


def test_policy_decision_approves_when_fee_adjusted_positive(policy_test_client: TestClient) -> None:
    request = factories.policy_decision_request()
    response = policy_test_client.post("/policy/decide", json=request.model_dump(mode="json"))
    payload = response.json()

    assert response.status_code == 200
    assert payload["approved"] is True
    assert pytest.approx(payload["effective_fee"]["maker"], rel=1e-3) == 5.0
    assert pytest.approx(payload["fee_adjusted_edge_bps"], rel=1e-3) == 25.0


def test_policy_decision_rejects_when_fee_erases_edge(monkeypatch: pytest.MonkeyPatch) -> None:
    request = factories.policy_decision_request()

    def fake_predict_intent(**_: Any) -> DummyIntent:
        return DummyIntent(approved=True, edge=4.0)

    fees: List[Dict[str, Any]] = []

    async def fake_fetch(
        account_id: str, symbol: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        notional_decimal = notional if isinstance(notional, Decimal) else Decimal(str(notional))
        fees.append({
            "account": account_id,
            "symbol": symbol,
            "liquidity": liquidity,
            "notional": notional_decimal,
        })
        return Decimal("6.0") if liquidity == "maker" else Decimal("8.0")

    monkeypatch.setattr("policy_service.predict_intent", fake_predict_intent)
    monkeypatch.setattr("policy_service._fetch_effective_fee", fake_fetch)

    client = TestClient(decision_app)
    response = client.post("/policy/decide", json=request.model_dump(mode="json"))
    payload = response.json()

    assert response.status_code == 200
    assert payload["approved"] is False
    assert payload["reason"] == "Fee-adjusted edge non-positive"
    assert {entry["liquidity"] for entry in fees} == {"maker", "taker"}
    assert fees[0]["symbol"] == request.instrument


def test_policy_decision_requires_book_snapshot(policy_test_client: TestClient) -> None:
    request = factories.policy_decision_request(book_snapshot=None)
    response = policy_test_client.post("/policy/decide", json=request.model_dump(mode="json"))
    assert response.status_code == 422


def test_policy_intent_service_validates_admin_account() -> None:
    client = TestClient(intent_service.app)

    request = {
        "account_id": "intruder",
        "symbol": "BTC-USD",
        "features": [1, 2, 3],
        "book_snapshot": {},
        "account_state": {},
    }

    response = client.post("/policy/decide", json=request)
    assert response.status_code == 422


def test_policy_intent_service_returns_model_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    stub_intent = {
        "action": "enter",
        "side": "buy",
        "qty": 1.0,
        "preference": "maker",
        "type": "limit",
        "limit_px": 10.0,
        "tif": "GTC",
        "tp": None,
        "sl": None,
        "expected_edge_bps": 12.0,
        "expected_cost_bps": 4.0,
        "confidence": 0.9,
    }

    def fake_predict_intent(**_: Any) -> Dict[str, Any]:
        return stub_intent

    monkeypatch.setattr(intent_service, "models", SimpleNamespace(predict_intent=fake_predict_intent))

    client = TestClient(intent_service.app)
    request = {
        "account_id": "company",
        "symbol": "BTC-USD",
        "features": [1, 2, 3],
        "book_snapshot": {"mid": 1.0},
        "account_state": {"drift_score": 0.1},
    }

    response = client.post("/policy/decide", json=request)
    assert response.status_code == 200
    assert response.json()["action"] == "enter"
