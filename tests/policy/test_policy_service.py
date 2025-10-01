from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

from fastapi.testclient import TestClient

if "metrics" not in sys.modules:
    metrics_stub = types.ModuleType("metrics")
    metrics_stub.setup_metrics = lambda app: None
    metrics_stub.record_abstention_rate = lambda *args, **kwargs: None
    metrics_stub.record_drift_score = lambda *args, **kwargs: None
    sys.modules["metrics"] = metrics_stub

from services.policy import policy_service


@dataclass
class DataclassIntent:
    action: str
    side: str
    qty: float
    preference: str
    type: str
    limit_px: float | None
    tif: str | None
    tp: float | None
    sl: float | None
    expected_edge_bps: float
    expected_cost_bps: float
    confidence: float


def test_decide_policy_accepts_dataclass_intent(monkeypatch):
    expected_intent = DataclassIntent(
        action="enter",
        side="buy",
        qty=5.0,
        preference="maker",
        type="limit",
        limit_px=101.5,
        tif="GTC",
        tp=120.0,
        sl=95.0,
        expected_edge_bps=25.0,
        expected_cost_bps=6.5,
        confidence=0.87,
    )

    monkeypatch.setattr(
        policy_service,
        "models",
        SimpleNamespace(predict_intent=lambda **_: expected_intent),
    )

    client = TestClient(policy_service.app)

    payload = {
        "account_id": str(uuid4()),
        "symbol": "BTC-USD",
        "features": [0.1, -0.2, 0.3],
        "book_snapshot": {"bid": 30_000, "ask": 30_010},
        "account_state": {"positions": {}},
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
