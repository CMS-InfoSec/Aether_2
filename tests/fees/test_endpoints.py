from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.fees.main import app

ADMIN_ACCOUNTS = ["admin-alpha", "admin-beta", "admin-gamma"]


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.mark.parametrize("account_id", ADMIN_ACCOUNTS)
def test_get_effective_fees_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    response = client.get(
        "/fees/effective",
        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-Id": account_id},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id
    assert body["isolation_segment"] == "seg-fees"
    assert body["fee_schedule"]["maker_bps"] == 5.0


def test_get_effective_fees_rejects_non_admin(client: TestClient) -> None:
    response = client.get(
        "/fees/effective",
        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-Id": "guest"},
    )

    assert response.status_code == 403
