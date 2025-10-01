from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS
from services.universe.main import app

client = TestClient(app)


def test_universe_approved_authorized_accounts():
    for account in ADMIN_ACCOUNTS:
        response = client.get("/universe/approved", headers={"X-Account-ID": account})
        assert response.status_code == 200
        data = response.json()
        assert set(data.keys()) == {"account_id", "instruments", "fee_overrides"}
        assert data["account_id"] == account
        assert isinstance(data["instruments"], list)
        assert isinstance(data["fee_overrides"], dict)


def test_universe_approved_rejects_non_admin():
    response = client.get("/universe/approved", headers={"X-Account-ID": "shadow"})
    assert response.status_code == 403
