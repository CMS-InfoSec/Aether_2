from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


from services.common.adapters import RedisFeastAdapter
from services.common.security import ADMIN_ACCOUNTS

from services.fees import main as fees_main
from services.fees.main import app
from services.universe.repository import UniverseRepository



@pytest.fixture(autouse=True)
def reset_universe_repository() -> None:
    UniverseRepository.reset()
    yield
    UniverseRepository.reset()



@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.mark.parametrize("account_id", sorted(ADMIN_ACCOUNTS))
def test_get_effective_fees_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    UniverseRepository.seed_fee_overrides(
        {
            "default": {"currency": "USD", "maker": 0.05, "taker": 0.1},
        }
    )

    response = client.get(
        "/fees/effective",
        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-ID": account_id},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id
    assert set(body.keys()) == {"account_id", "effective_from", "fee"}

    assert body["fee"]["currency"] == "USD"
    assert body["fee"]["maker"] == pytest.approx(0.05)
    assert body["fee"]["taker"] == pytest.approx(0.1)



def test_get_effective_fees_rejects_non_admin(client: TestClient) -> None:
    response = client.get(
        "/fees/effective",
        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-ID": "guest"},
    )

    assert response.status_code == 403


def test_get_effective_fees_handles_missing_repository_override(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    class StubRepository:
        def approved_universe(self) -> list[str]:
            return []

        def fee_override(self, instrument: str) -> dict[str, float | str] | None:
            return None

    def adapter_factory(account_id: str) -> RedisFeastAdapter:
        return RedisFeastAdapter(account_id=account_id, repository=StubRepository())

    monkeypatch.setattr(fees_main, "RedisFeastAdapter", adapter_factory)

    response = client.get(
        "/fees/effective",
        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-Id": "company"},
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
        headers={"X-Account-Id": "company"},
    )

    assert response.status_code == 200
    assert instances
    stub_repo = instances[-1]
    assert stub_repo.fee_override_calls == ["default"]
    body = response.json()
    assert body["fee"]["maker"] == 0.5
    assert body["fee"]["taker"] == 0.8


def test_get_effective_fees_uses_factory_when_repository_not_supplied(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    class StubRepository:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id
            self.fee_override_calls: list[str] = []

        def approved_universe(self) -> list[str]:
            return []

        def fee_override(self, instrument: str) -> dict[str, float | str] | None:
            self.fee_override_calls.append(instrument)
            return {"currency": "USD", "maker": 0.25, "taker": 0.4}

    repositories: list[StubRepository] = []

    def repository_factory(account_id: str) -> StubRepository:
        repo = StubRepository(account_id)
        repositories.append(repo)
        return repo

    def adapter_factory(account_id: str) -> RedisFeastAdapter:
        return RedisFeastAdapter(account_id=account_id, repository_factory=repository_factory)

    monkeypatch.setattr(fees_main, "RedisFeastAdapter", adapter_factory)

    response = client.get(
        "/fees/effective",
        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-Id": "director-1"},
    )

    assert response.status_code == 200
    assert repositories and repositories[-1].account_id == "director-1"
    assert repositories[-1].fee_override_calls == ["default"]
    payload = response.json()
    assert payload["fee"]["maker"] == 0.25
    assert payload["fee"]["taker"] == 0.4
