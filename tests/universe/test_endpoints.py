from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.universe.main import app

ADMIN_ACCOUNTS = ["admin-eu", "admin-us", "admin-apac"]


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.mark.parametrize("account_id", ADMIN_ACCOUNTS)
def test_get_universe_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    response = client.get("/universe/approved", headers={"X-Account-ID": account_id})

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id
    assert isinstance(body["instruments"], list)
    assert all(symbol.endswith("-USD") for symbol in body["instruments"])
    assert isinstance(body["fee_overrides"], dict)


def test_get_universe_rejects_non_admin(client: TestClient) -> None:
    response = client.get("/universe/approved", headers={"X-Account-ID": "guest"})

    assert response.status_code == 403
