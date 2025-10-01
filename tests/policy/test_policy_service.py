from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

if "metrics" not in sys.modules:
    metrics_stub = types.ModuleType("metrics")
    def _setup_metrics(app):
        if not any(route.path == "/metrics" for route in app.routes):
            @app.get("/metrics")  # pragma: no cover - simple stub endpoint
            def _metrics_endpoint():
                return {"status": "ok"}

    metrics_stub.setup_metrics = _setup_metrics
    metrics_stub.record_abstention_rate = lambda *args, **kwargs: None
    metrics_stub.record_drift_score = lambda *args, **kwargs: None
    sys.modules["metrics"] = metrics_stub

from services.common.schemas import ActionTemplate, ConfidenceMetrics, PolicyDecisionResponse

import policy_service


@dataclass
class DataclassIntent:
    edge_bps: float
    confidence: ConfidenceMetrics
    take_profit_bps: float
    stop_loss_bps: float
    selected_action: str
    action_templates: list[ActionTemplate]
    approved: bool
    reason: str | None = None


@pytest.fixture(name="client")
def _client() -> TestClient:
    return TestClient(policy_service.app)


def test_decide_policy_accepts_dataclass_intent(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    intent = DataclassIntent(
        edge_bps=20.0,
        confidence=ConfidenceMetrics(
            model_confidence=0.4,
            state_confidence=0.4,
            execution_confidence=0.4,
            overall_confidence=0.4,
        ),
        take_profit_bps=30.0,
        stop_loss_bps=10.0,
        selected_action="maker",
        action_templates=[
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=20.0,
                fee_bps=0.0,
                confidence=0.5,
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=18.0,
                fee_bps=0.0,
                confidence=0.45,
            ),
        ],
        approved=True,
    )

    async def _fake_fee(account_id: str, symbol: str, liquidity: str, notional: float) -> float:
        return {"maker": 4.0, "taker": 7.0}[liquidity]

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: intent)

    payload = {
        "account_id": str(uuid4()),
        "order_id": "unit-test",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 0.5,
        "price": 100.0,
        "fee": {"currency": "USD", "maker": 4.0, "taker": 6.0},
        "features": [0.1, -0.2, 0.3],
        "book_snapshot": {"mid_price": 100.5, "spread_bps": 2.0, "imbalance": 0.1},
        "state": {
            "regime": "expansion",
            "volatility": 0.2,
            "liquidity_score": 0.9,
            "conviction": 0.7,
        },
        "confidence": {
            "model_confidence": 0.9,
            "state_confidence": 0.85,
            "execution_confidence": 0.88,
        },
    }

    response = client.post("/policy/decide", json=payload)
    assert response.status_code == 200

    assert response.json() == {
        "action": expected_intent.action,
        "side": expected_intent.side,
        "qty": expected_intent.qty,
        "preference": expected_intent.preference,
        "type": expected_intent.type,
        "limit_px": expected_intent.limit_px,
        "tif": expected_intent.tif,
        "tp": expected_intent.tp,
        "sl": expected_intent.sl,
        "expected_edge_bps": expected_intent.expected_edge_bps,
        "expected_cost_bps": expected_intent.expected_cost_bps,
        "confidence": expected_intent.confidence,
    }


def test_metrics_endpoint_exposed_after_import():
    client = TestClient(policy_service.app)

    response = client.get("/metrics")

    assert response.status_code == 200

