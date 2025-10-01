from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS
from services.fees.main import app
from services.universe.repository import UniverseRepository

client = TestClient(app)


def test_fees_effective_authorized_accounts():
    UniverseRepository.reset()
    UniverseRepository.seed_fee_overrides(
        {
            "default": {"currency": "USD", "maker": 0.05, "taker": 0.1},
        }
    )

    for account in ADMIN_ACCOUNTS:
        response = client.get("/fees/effective", headers={"X-Account-ID": account})
        assert response.status_code == 200
        data = response.json()
        assert set(data.keys()) == {"account_id", "effective_from", "fee"}
        assert data["account_id"] == account
        assert data["fee"] == {"currency": "USD", "maker": 0.05, "taker": 0.1}


def test_fees_effective_rejects_non_admin():
    response = client.get("/fees/effective", headers={"X-Account-ID": "shadow"})
    assert response.status_code == 403
