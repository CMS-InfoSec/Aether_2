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
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }
    for account in ADMIN_ACCOUNTS:
        payload["account_id"] = account
        response = client.post("/policy/decide", json=payload, headers={"X-Account-ID": account})
        assert response.status_code == 200
        data = response.json()
        assert set(data.keys()) == {"approved", "reason", "effective_fee"}
        assert data["effective_fee"] == payload["fee"]


def test_policy_decide_rejects_non_admin_account():
    payload = {
        "account_id": "shadow-account",
        "order_id": "1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 100.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }
    response = client.post("/policy/decide", json=payload, headers={"X-Account-ID": "shadow-account"})
    assert response.status_code == 403
