from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.universe.main import app

ADMIN_ACCOUNTS = ["admin-alpha", "admin-beta", "admin-gamma"]


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.mark.parametrize("account_id", ADMIN_ACCOUNTS)
def test_get_universe_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    response = client.get(
        "/universe/approved",
        params={"isolation_segment": "seg-uni", "fee_tier": "standard"},
        headers={"X-Account-Id": account_id},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id
    assert body["symbols"]
    assert all(symbol.startswith(f"{account_id}:") for symbol in body["symbols"])


def test_get_universe_rejects_non_admin(client: TestClient) -> None:
    response = client.get(
        "/universe/approved",
        params={"isolation_segment": "seg-uni", "fee_tier": "standard"},
        headers={"X-Account-Id": "guest"},
    )

    assert response.status_code == 403
