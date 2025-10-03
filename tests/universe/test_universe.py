import pytest
from fastapi.testclient import TestClient

from services.common.adapters import RedisFeastAdapter
import pytest
from fastapi.testclient import TestClient

from services.common.adapters import RedisFeastAdapter
from services.common.security import ADMIN_ACCOUNTS
from services.universe import main as universe_main
from services.universe.main import app

from services.universe.repository import UniverseRepository
from tests.universe.conftest import UniverseTimescaleFixture


class NoopFeastStore:
    def get_historical_features(self, **_: object) -> None:  # pragma: no cover - guard
        raise AssertionError("RedisFeastAdapter should not query Feast in universe tests")

    def get_online_features(self, **_: object) -> None:  # pragma: no cover - guard
        raise AssertionError("RedisFeastAdapter should not query Feast in universe tests")


def _prime_adapter_cache(account_id: str) -> None:
    repo = UniverseRepository(account_id=account_id)
    instruments = repo.approved_universe()
    fees = {}
    for symbol in instruments:
        override = repo.fee_override(symbol)
        if override:
            fees[symbol] = override
    if not instruments:
        instruments = ["BTC-USD"]
        fees.setdefault("BTC-USD", {"currency": "USD", "maker": 0.1, "taker": 0.2})
    RedisFeastAdapter._features[account_id] = {"approved": instruments, "fees": fees}


client = TestClient(app)


@pytest.fixture(autouse=True)

def reset_state(monkeypatch: pytest.MonkeyPatch) -> None:
    UniverseRepository.reset()
    RedisFeastAdapter._features.clear()
    RedisFeastAdapter._fee_tiers.clear()
    RedisFeastAdapter._online_feature_store.clear()
    RedisFeastAdapter._feature_expirations.clear()
    RedisFeastAdapter._fee_tier_expirations.clear()
    RedisFeastAdapter._online_feature_expirations.clear()

    class CachedRedisAdapter(RedisFeastAdapter):
        def __init__(self, account_id: str) -> None:
            super().__init__(
                account_id=account_id,
                feature_store_factory=lambda *_: NoopFeastStore(),
            )

    monkeypatch.setattr(universe_main, "RedisFeastAdapter", CachedRedisAdapter)
    yield
    UniverseRepository.reset()
    RedisFeastAdapter._features.clear()
    RedisFeastAdapter._fee_tiers.clear()
    RedisFeastAdapter._online_feature_store.clear()
    RedisFeastAdapter._feature_expirations.clear()
    RedisFeastAdapter._fee_tier_expirations.clear()
    RedisFeastAdapter._online_feature_expirations.clear()


def test_universe_approved_authorized_accounts():

    for account in ADMIN_ACCOUNTS:
        _prime_adapter_cache(account)
        adapter = RedisFeastAdapter(
            account_id=account,
            feature_store_factory=lambda *_: NoopFeastStore(),
        )
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
        RedisFeastAdapter._features[account_id] = {
            "approved": repo.approved_universe(),
            "fees": {
                symbol: repo.fee_override(symbol)
                for symbol in repo.approved_universe()
                if repo.fee_override(symbol)
            },
        }
        return RedisFeastAdapter(
            account_id=account_id,
            repository=repo,
            feature_store_factory=lambda *_: NoopFeastStore(),
        )

    monkeypatch.setattr(universe_main, "RedisFeastAdapter", factory)

    local_client = TestClient(app)
    response = local_client.get("/universe/approved", headers={"X-Account-ID": "company"})

    assert response.status_code == 200
    assert instances
