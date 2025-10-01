from fastapi.testclient import TestClient

from services.common.adapters import RedisFeastAdapter
from services.common.security import ADMIN_ACCOUNTS
from services.universe import main as universe_main
from services.universe.main import app

from services.universe.repository import UniverseRepository


client = TestClient(app)


def test_universe_approved_authorized_accounts():
    UniverseRepository.reset()
    for account in ADMIN_ACCOUNTS:
        repo = UniverseRepository(account_id=account)
        adapter = RedisFeastAdapter(account_id=account, repository=repo)
        expected_instruments = adapter.approved_instruments()
        expected_overrides = {}
        for instrument in expected_instruments:
            override = adapter.fee_override(instrument)
            if override:
                expected_overrides[instrument] = override

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


def test_universe_endpoint_uses_repository_calls(monkeypatch):
    class StubRepository:
        def __init__(self) -> None:
            self.approved_calls = 0
            self.fee_override_calls: list[str] = []

        def approved_universe(self) -> list[str]:
            self.approved_calls += 1
            return ["BTC-USD"]

        def fee_override(self, instrument: str) -> dict[str, float | str] | None:
            self.fee_override_calls.append(instrument)
            return {"currency": "USD", "maker": 0.1, "taker": 0.2}

    instances: list[StubRepository] = []

    def factory(account_id: str, repository: StubRepository | None = None) -> RedisFeastAdapter:
        repo = repository or StubRepository()
        instances.append(repo)
        return RedisFeastAdapter(account_id=account_id, repository=repo)

    monkeypatch.setattr(universe_main, "RedisFeastAdapter", factory)

    local_client = TestClient(app)
    response = local_client.get("/universe/approved", headers={"X-Account-ID": "company"})

    assert response.status_code == 200
    assert instances
    stub_repo = instances[-1]
    assert stub_repo.approved_calls == 1
    assert stub_repo.fee_override_calls == ["BTC-USD"]
