from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS
from services.secrets.main import app

client = TestClient(app)


def test_secrets_kraken_authorized_accounts():
    payload = {"account_id": "admin-eu"}
    for account in ADMIN_ACCOUNTS:
        payload["account_id"] = account
        response = client.post("/secrets/kraken", json=payload, headers={"X-Account-ID": account})
        assert response.status_code == 200
        data = response.json()
        assert set(data.keys()) == {"account_id", "api_key", "api_secret", "fee"}
        assert data["account_id"] == account


def test_secrets_kraken_rejects_non_admin():
    payload = {"account_id": "shadow"}
    response = client.post("/secrets/kraken", json=payload, headers={"X-Account-ID": "shadow"})
    assert response.status_code == 403
