import importlib.util
import sys
import sysconfig
from pathlib import Path

if (
    "secrets" not in sys.modules
    or "tests/secrets" in str(getattr(sys.modules.get("secrets"), "__file__", ""))
):
    stdlib_path = Path(sysconfig.get_paths()["stdlib"]) / "secrets.py"
    spec = importlib.util.spec_from_file_location("secrets", stdlib_path)
    if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        sys.modules["secrets"] = module

from fastapi.testclient import TestClient

from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.policy.main import app

client = TestClient(app)


def _reset_state() -> None:
    KafkaNATSAdapter._event_store.clear()
    TimescaleAdapter._telemetry.clear()


def _base_payload() -> dict:
    return {
        "account_id": "admin-eu",
        "order_id": "42",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 100.0,
        "features": [1.0, -0.2, 0.4],
    }


def test_fee_adjusted_edge_abstention_when_costs_dominate():
    _reset_state()
    payload = _base_payload()
    payload.update(
        {
            "fee": {"currency": "USD", "maker": 8.0, "taker": 12.0},
            "expected_edge_bps": 6.0,
            "book_snapshot": {"mid_price": 30_000.0, "spread_bps": 8.0, "imbalance": 0.0},
            "state": {
                "regime": "neutral",
                "volatility": 0.45,
                "liquidity_score": 0.6,
                "conviction": 0.4,
            },
            "confidence": {
                "model_confidence": 0.9,
                "state_confidence": 0.9,
                "execution_confidence": 0.9,
            },
        }
    )

    response = client.post("/policy/decide", json=payload, headers={"X-Account-ID": payload["account_id"]})
    body = response.json()

    assert response.status_code == 200
    assert body["approved"] is False
    assert body["selected_action"] == "abstain"
    assert body["reason"] == "Fee-adjusted edge non-positive"
    assert body["fee_adjusted_edge_bps"] <= 0



def test_prefers_maker_when_edge_higher_after_fees():
    _reset_state()
    payload = _base_payload()
    payload.update(
        {
            "fee": {"currency": "USD", "maker": 4.0, "taker": 9.0},
            "expected_edge_bps": 18.0,
            "book_snapshot": {"mid_price": 30_000.0, "spread_bps": 2.0, "imbalance": 0.1},
            "state": {
                "regime": "neutral",
                "volatility": 0.3,
                "liquidity_score": 0.8,
                "conviction": 0.65,
            },
        }
    )

    response = client.post("/policy/decide", json=payload, headers={"X-Account-ID": payload["account_id"]})
    body = response.json()

    assert response.status_code == 200
    assert body["approved"] is True
    assert body["selected_action"] == "maker"
    maker_template = next(template for template in body["action_templates"] if template["name"] == "maker")
    taker_template = next(template for template in body["action_templates"] if template["name"] == "taker")
    assert maker_template["edge_bps"] > taker_template["edge_bps"]
    assert body["fee_adjusted_edge_bps"] == maker_template["edge_bps"]



def test_confidence_gating_blocks_execution():
    _reset_state()
    payload = _base_payload()
    payload.update(
        {
            "fee": {"currency": "USD", "maker": 3.0, "taker": 6.0},
            "expected_edge_bps": 14.0,
            "book_snapshot": {"mid_price": 30_000.0, "spread_bps": 35.0, "imbalance": 0.0},
            "state": {
                "regime": "neutral",
                "volatility": 0.5,
                "liquidity_score": 0.2,
                "conviction": 0.4,
            },
            "confidence": {
                "model_confidence": 0.2,
                "state_confidence": 0.2,
                "execution_confidence": 0.2,
            },
        }
    )

    response = client.post("/policy/decide", json=payload, headers={"X-Account-ID": payload["account_id"]})
    body = response.json()

    assert response.status_code == 200
    assert body["approved"] is False
    assert body["reason"] == "Confidence below threshold"
    assert body["fee_adjusted_edge_bps"] == 0.0
    assert body["selected_action"] == "abstain"
