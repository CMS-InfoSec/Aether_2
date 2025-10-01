from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS
from services.universe.main import app
from services.universe.repository import UniverseRepository

client = TestClient(app)


def test_universe_approved_authorized_accounts():
    UniverseRepository.reset()
    repo = UniverseRepository(account_id="admin-eu")
    expected_instruments = repo.approved_universe()
    expected_overrides = {}
    for instrument in expected_instruments:
        override = repo.fee_override(instrument)
        if override:
            expected_overrides[instrument] = override

    for account in ADMIN_ACCOUNTS:
        response = client.get("/universe/approved", headers={"X-Account-ID": account})
        assert response.status_code == 200
        data = response.json()
        assert set(data.keys()) == {"account_id", "instruments", "fee_overrides"}
        assert data["account_id"] == account
        assert data["instruments"] == expected_instruments
        assert data["fee_overrides"] == expected_overrides


def test_universe_approved_rejects_non_admin():
    response = client.get("/universe/approved", headers={"X-Account-ID": "shadow"})
    assert response.status_code == 403
