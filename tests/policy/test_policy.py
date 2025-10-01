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

from services.common.security import ADMIN_ACCOUNTS
from services.policy.main import app

client = TestClient(app)


def test_policy_decide_all_admin_accounts_authorized():
    payload = {
        "account_id": "admin-eu",
        "order_id": "1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 100.0,
        "fee": {"currency": "USD", "maker": 5.0, "taker": 8.0},
        "features": [18.0, 4.0, -2.0],
        "book_snapshot": {"mid_price": 30_000.0, "spread_bps": 3.0, "imbalance": 0.15},
        "state": {
            "regime": "neutral",
            "volatility": 0.35,
            "liquidity_score": 0.75,
            "conviction": 0.6,
        },
        "expected_edge_bps": 20.0,
        "take_profit_bps": 30.0,
        "stop_loss_bps": 12.0,
        "confidence": {
            "model_confidence": 0.65,
            "state_confidence": 0.6,
            "execution_confidence": 0.64,
        },
    }
    for account in ADMIN_ACCOUNTS:
        payload["account_id"] = account
        response = client.post("/policy/decide", json=payload, headers={"X-Account-ID": account})
        assert response.status_code == 200
        data = response.json()
        assert data["effective_fee"] == payload["fee"]
        assert "fee_adjusted_edge_bps" in data
        assert "confidence" in data
        assert data["confidence"]["overall_confidence"] >= 0


def test_policy_decide_rejects_non_admin_account():
    payload = {
        "account_id": "shadow-account",
        "order_id": "1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 100.0,
        "fee": {"currency": "USD", "maker": 5.0, "taker": 8.0},
    }
    response = client.post("/policy/decide", json=payload, headers={"X-Account-ID": "shadow-account"})
    assert response.status_code == 403
