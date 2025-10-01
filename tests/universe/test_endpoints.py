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
    assert all(symbol.split("-")[-1] in {"USD", "USDT"} for symbol in body["instruments"])
    assert isinstance(body["fee_overrides"], dict)


def test_get_universe_rejects_non_admin(client: TestClient) -> None:
    response = client.get("/universe/approved", headers={"X-Account-ID": "guest"})

    assert response.status_code == 403


def test_get_universe_handles_missing_repository_overrides(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    captured_accounts: list[str] = []

    class StubRepository:
        def __init__(self, account_id: str) -> None:
            captured_accounts.append(account_id)

        def approved_universe(self) -> list[str]:
            return []

        def fee_override(self, instrument: str) -> dict[str, float | str] | None:
            return None

    monkeypatch.setattr("services.common.adapters.UniverseRepository", StubRepository)

    response = client.get("/universe/approved", headers={"X-Account-ID": "admin-us"})

    assert response.status_code == 200
    assert captured_accounts == ["admin-us"]
    body = response.json()
    assert body["instruments"] == ["SOL-USD"]
    assert body["fee_overrides"] == {}
