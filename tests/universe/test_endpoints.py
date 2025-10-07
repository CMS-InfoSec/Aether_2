from __future__ import annotations

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
        raise AssertionError("RedisFeastAdapter should not query Feast during endpoint tests")

    def get_online_features(self, **_: object) -> None:  # pragma: no cover - guard
        raise AssertionError("RedisFeastAdapter should not query Feast during endpoint tests")


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



@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)

def reset_universe_repository(monkeypatch: pytest.MonkeyPatch) -> None:
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



@pytest.mark.parametrize("account_id", sorted(ADMIN_ACCOUNTS))
def test_get_universe_allows_admin_accounts(
    client: TestClient, account_id: str, universe_timescale: UniverseTimescaleFixture
) -> None:
    universe_timescale.add_snapshot(
        base_asset="BTC",
        quote_asset="USD",
        market_cap=1.2e12,
        global_volume_24h=5.0e10,
        kraken_volume_24h=2.5e10,
        volatility_30d=0.45,
    )
    universe_timescale.add_fee_override(
        "BTC-USD", currency="USD", maker=0.1, taker=0.2
    )

    _prime_adapter_cache(account_id)
    adapter = RedisFeastAdapter(
        account_id=account_id,
        feature_store_factory=lambda *_: NoopFeastStore(),
    )
    expected_instruments = adapter.approved_instruments()
    expected_overrides = {}
    for instrument in expected_instruments:
        override = adapter.fee_override(instrument)
        if override:
            expected_overrides[instrument] = override

    response = client.get(
        "/universe/approved",
        headers={"X-Account-ID": account_id},
    )


    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id

    assert isinstance(body["instruments"], list)
    assert all(symbol.endswith("-USD") for symbol in body["instruments"])
    assert isinstance(body["fee_overrides"], dict)



def test_get_universe_rejects_non_admin(client: TestClient) -> None:
    response = client.get(
        "/universe/approved",
        headers={"X-Account-ID": "guest"},
    )


    assert response.status_code == 403



@pytest.mark.parametrize("account_id", sorted(ADMIN_ACCOUNTS))
def test_universe_endpoint_uses_fallback_when_timescale_empty(
    client: TestClient, account_id: str, universe_timescale: UniverseTimescaleFixture
) -> None:


    UniverseRepository.seed_market_snapshots([])
    _prime_adapter_cache(account_id)


    response = client.get(
        "/universe/approved",
        headers={"X-Account-ID": account_id},
    )

    assert response.status_code == 200
    body = response.json()

    adapter = RedisFeastAdapter(
        account_id=account_id,
        feature_store_factory=lambda *_: NoopFeastStore(),
    )
    expected_instruments = adapter.approved_instruments()

    assert body["instruments"] == expected_instruments
    assert all(symbol.endswith("-USD") for symbol in body["instruments"])
    expected_overrides = {}
    for instrument in expected_instruments:
        override = adapter.fee_override(instrument)
        if override:
            expected_overrides[instrument] = override

    assert body["fee_overrides"] == expected_overrides

