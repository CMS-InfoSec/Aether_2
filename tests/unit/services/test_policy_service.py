"""Unit tests for the policy decision services."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Dict, List
from types import SimpleNamespace

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


def test_policy_intent_cost_estimators_return_non_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeAdapter:
        def __init__(self, account_id: str, **_: Any) -> None:
            self.account_id = account_id

        def fee_tiers(self, pair: str) -> List[Dict[str, float]]:
            assert pair == "BTC-USD"
            return [
                {"tier": "base", "maker": 4.2, "taker": 6.5, "notional_threshold": 0.0},
                {"tier": "vip", "maker": 2.1, "taker": 3.0, "notional_threshold": 50.0},
            ]

    class FakeImpactStore:
        async def impact_curve(self, **_: Any) -> List[Dict[str, float]]:
            return [
                {"size": 1.0, "impact_bps": 1.5},
                {"size": 25.0, "impact_bps": 6.0},
            ]

    fee_client = intent_service.FeeServiceClient(adapter_factory=FakeAdapter, cache_ttl_seconds=1.0)
    slippage_client = intent_service.SlippageEstimator(
        impact_store=FakeImpactStore(), cache_ttl_seconds=1.0
    )
    monkeypatch.setattr(intent_service, "fee_service", fee_client)
    monkeypatch.setattr(intent_service, "slippage_estimator", slippage_client)
    intent_payload = {
        "action": "enter",
        "side": "buy",
        "qty": 10.0,
        "preference": "maker",
        "type": "limit",
        "expected_edge_bps": 40.0,
        "expected_cost_bps": 0.0,
        "confidence": 0.9,
    }
    monkeypatch.setattr(
        intent_service,
        "models",
        SimpleNamespace(predict_intent=lambda **_: intent_payload),
    )

    request = intent_service.PolicyDecisionRequest(account_id="company", symbol="BTC-USD")
    response = intent_service.decide_policy_intent(request)

    diagnostics = response.diagnostics
    assert diagnostics["fee_bps"] > 0.0
    assert diagnostics["slippage_bps"] > 0.0
    assert response.expected_cost_bps == pytest.approx(
        diagnostics["fee_bps"] + diagnostics["slippage_bps"]
    )


def test_fee_service_selects_base_tier_when_below_threshold() -> None:
    tiers = [
        {"tier": "base", "maker": 4.2, "taker": 6.5, "notional_threshold": 0.0},
        {"tier": "vip", "maker": 2.1, "taker": 3.0, "notional_threshold": 50.0},
    ]

    estimate = intent_service.FeeServiceClient._select_tier(
        tiers,
        liquidity="taker",
        size=10.0,
    )

    assert estimate == pytest.approx(6.5)


def test_fee_service_falls_back_to_zero_when_unavailable() -> None:
    class BrokenAdapter:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("unreachable")

    client = intent_service.FeeServiceClient(adapter_factory=BrokenAdapter, cache_ttl_seconds=1.0)
    estimate = client.fee_bps_estimate(
        account_id="company", symbol="BTC-USD", liquidity="maker", size=5.0
    )
    assert estimate == 0.0


def test_slippage_estimator_falls_back_to_zero_when_unavailable() -> None:
    class BrokenStore:
        async def impact_curve(self, **_: Any) -> List[Dict[str, float]]:
            raise RuntimeError("impact unavailable")

    estimator = intent_service.SlippageEstimator(
        impact_store=BrokenStore(), cache_ttl_seconds=1.0, async_timeout=0.1
    )
    slippage = estimator.estimate_slippage_bps(
        account_id="company", symbol="BTC-USD", side="buy", size=5.0
    )
    assert slippage == 0.0
