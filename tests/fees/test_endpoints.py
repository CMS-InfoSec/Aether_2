from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.common.adapters import RedisFeastAdapter
from services.common.security import ADMIN_ACCOUNTS as SERVICE_ADMIN_ACCOUNTS
import services.fees.main as fees_main
from services.fees.main import app

ADMIN_ACCOUNTS = sorted(SERVICE_ADMIN_ACCOUNTS)


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
    assert set(body.keys()) == {"account_id", "effective_from", "fee"}
    assert set(body["fee"].keys()) == {"currency", "maker", "taker"}


def test_get_effective_fees_rejects_non_admin(client: TestClient) -> None:
    response = client.get(
        "/fees/effective",
        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-Id": "guest"},
    )

    assert response.status_code == 403


def test_get_effective_fees_handles_missing_repository_override(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    class StubRepository:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def approved_universe(self) -> list[str]:
            return []

        def fee_override(self, instrument: str) -> dict[str, float | str] | None:
            return None

    monkeypatch.setattr("services.common.adapters.UniverseRepository", StubRepository)

    response = client.get(
        "/fees/effective",
        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-Id": "admin-eu"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["fee"] == {"currency": "USD", "maker": 0.1, "taker": 0.2}


def test_get_effective_fees_uses_repository_override(monkeypatch, client: TestClient) -> None:
    class StubRepository:
        def __init__(self) -> None:
            self.fee_override_calls: list[str] = []

        def approved_universe(self) -> list[str]:
            return []

        def fee_override(self, instrument: str) -> dict[str, float | str] | None:
            self.fee_override_calls.append(instrument)
            return {"currency": "USD", "maker": 0.5, "taker": 0.8}

    instances: list[StubRepository] = []

    def factory(account_id: str, repository: StubRepository | None = None) -> RedisFeastAdapter:
        repo = repository or StubRepository()
        instances.append(repo)
        return RedisFeastAdapter(account_id=account_id, repository=repo)

    monkeypatch.setattr(fees_main, "RedisFeastAdapter", factory)

    response = client.get(
        "/fees/effective",
        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-Id": "admin-eu"},
    )

    assert response.status_code == 200
    assert instances
    stub_repo = instances[-1]
    assert stub_repo.fee_override_calls == ["default"]
    body = response.json()
    assert body["fee"]["maker"] == 0.5
    assert body["fee"]["taker"] == 0.8
