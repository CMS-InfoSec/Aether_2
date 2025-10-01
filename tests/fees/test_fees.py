from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS
from services.fees.main import app

client = TestClient(app)


def test_fees_effective_authorized_accounts():
    for account in ADMIN_ACCOUNTS:
        response = client.get("/fees/effective", headers={"X-Account-ID": account})
        assert response.status_code == 200
        data = response.json()
        assert set(data.keys()) == {"account_id", "effective_from", "fee"}
        assert data["account_id"] == account


def test_fees_effective_rejects_non_admin():
    response = client.get("/fees/effective", headers={"X-Account-ID": "shadow"})
    assert response.status_code == 403
