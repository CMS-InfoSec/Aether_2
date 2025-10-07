from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.common.adapters import RedisFeastAdapter

from fastapi.testclient import TestClient

from services.common.adapters import RedisFeastAdapter
from services.universe.repository import MarketSnapshot, UniverseRepository

from services.universe import main as universe_main
from tests.universe.conftest import UniverseTimescaleFixture



class NoopFeastStore:
    def get_historical_features(self, **_: object) -> None:  # pragma: no cover - guard
        raise AssertionError("RedisFeastAdapter should not query Feast in this test")

    def get_online_features(self, **_: object) -> None:  # pragma: no cover - guard
        raise AssertionError("RedisFeastAdapter should not query Feast in this test")


def _prime_adapter_cache(account_id: str, repository: UniverseRepository) -> None:
    instruments = repository.approved_universe()
    fees = {}
    for symbol in instruments:
        override = repository.fee_override(symbol)
        if override:
            fees[symbol] = override
    if not instruments:
        instruments = ["BTC-USD"]
        fees.setdefault("BTC-USD", {"currency": "USD", "maker": 0.1, "taker": 0.2})
    RedisFeastAdapter._features[account_id] = {"approved": instruments, "fees": fees}


def setup_function(function: object) -> None:
    UniverseRepository.reset()
    RedisFeastAdapter._features.clear()
    RedisFeastAdapter._fee_tiers.clear()
    RedisFeastAdapter._online_feature_store.clear()
    RedisFeastAdapter._feature_expirations.clear()
    RedisFeastAdapter._fee_tier_expirations.clear()
    RedisFeastAdapter._online_feature_expirations.clear()


def test_non_usd_symbols_are_filtered() -> None:
    UniverseRepository.seed_market_snapshots(
        [
            MarketSnapshot(
                base_asset="BTC",
                quote_asset="USD",
                market_cap=1.2e12,
                global_volume_24h=5.0e10,
                kraken_volume_24h=2.5e10,
                volatility_30d=0.45,
            ),
            MarketSnapshot(
                base_asset="ETH",
                quote_asset="EUR",
                market_cap=4.0e11,
                global_volume_24h=2.5e10,
                kraken_volume_24h=1.2e10,
                volatility_30d=0.50,
            ),
        ]

    )

    repo = UniverseRepository(account_id="company")
    _prime_adapter_cache("company", repo)

    adapter = RedisFeastAdapter(
        account_id="company",
        feature_store_factory=lambda *_: NoopFeastStore(),
    )
    instruments = adapter.approved_instruments()

    assert instruments == ["BTC-USD"]


def test_manual_overrides_are_honored(universe_timescale: UniverseTimescaleFixture) -> None:
    universe_timescale.add_snapshot(
        base_asset="BTC",
        quote_asset="USD",
        market_cap=1.2e12,
        global_volume_24h=5.0e10,
        kraken_volume_24h=2.5e10,
        volatility_30d=0.45,
    )
    universe_timescale.add_snapshot(
        base_asset="DOGE",
        quote_asset="USD",
        market_cap=1.0e7,
        global_volume_24h=1.0e6,
        kraken_volume_24h=5.0e5,
        volatility_30d=0.5,
    )

    repo = UniverseRepository(account_id="company")
    assert "DOGE-USD" not in repo.approved_universe()

    repo.set_manual_override("DOGE-USD", approved=True, actor_id="company", reason="Liquidity waiver")

    _prime_adapter_cache("company", repo)

    adapter = RedisFeastAdapter(
        account_id="company",
        repository=repo,
        feature_store_factory=lambda *_: NoopFeastStore(),
    )
    instruments = adapter.approved_instruments()

    assert "DOGE-USD" in instruments

    audit_entries = UniverseRepository.audit_entries()
    assert audit_entries
    assert audit_entries[-1].action == "universe.manual_override"
    assert audit_entries[-1].after["DOGE-USD"]["approved"] is True


def test_volatility_threshold_filters_low_and_high_risk_assets(
    universe_timescale: UniverseTimescaleFixture,
) -> None:
    universe_timescale.add_snapshot(
        base_asset="ADA",
        quote_asset="USD",
        market_cap=2.0e10,
        global_volume_24h=1.5e9,
        kraken_volume_24h=8.0e8,
        volatility_30d=0.35,
    )
    universe_timescale.add_snapshot(
        base_asset="AVAX",
        quote_asset="USD",
        market_cap=3.5e10,
        global_volume_24h=2.0e9,
        kraken_volume_24h=1.2e9,
        volatility_30d=0.55,
    )

    repo = UniverseRepository(account_id="company")
    approved_universe = repo.approved_universe()

    assert "ADA-USD" not in approved_universe
    assert "AVAX-USD" in approved_universe


class StubUniverseRepository:
    def __init__(self) -> None:
        self._approved = ["BTC-USD"]
        self._fees = {"BTC-USD": {"currency": "USD", "maker": 0.1, "taker": 0.2}}

    def approved_universe(self) -> list[str]:
        return list(self._approved)

    def fee_override(self, instrument: str) -> dict[str, float | str] | None:
        return dict(self._fees[instrument]) if instrument in self._fees else None


def test_adapter_uses_repository_factory_when_not_injected() -> None:
    class FactoryRepository:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id
            self.approved_calls = 0
            self.fee_override_calls: list[str] = []

        def approved_universe(self) -> list[str]:
            self.approved_calls += 1
            return ["BTC-USD"]

        def fee_override(self, instrument: str) -> dict[str, float | str] | None:
            self.fee_override_calls.append(instrument)
            return {"currency": "USD", "maker": 0.2, "taker": 0.3}

    created: list[FactoryRepository] = []

    def factory(account_id: str) -> FactoryRepository:
        repository = FactoryRepository(account_id)
        created.append(repository)
        return repository

    RedisFeastAdapter._features["company"] = {
        "approved": ["BTC-USD"],
        "fees": {"BTC-USD": {"currency": "USD", "maker": 0.2, "taker": 0.3}},
    }

    adapter = RedisFeastAdapter(
        account_id="company",
        repository_factory=factory,
        feature_store_factory=lambda *_: NoopFeastStore(),
    )

    instruments = adapter.approved_instruments()
    assert instruments == ["BTC-USD"]
    assert created and created[-1].account_id == "company"

    fee = adapter.fee_override("BTC-USD")
    assert fee == {"currency": "USD", "maker": 0.2, "taker": 0.3}


def test_fastapi_endpoint_uses_cached_repository(monkeypatch) -> None:
    stub_repository = StubUniverseRepository()

    class StubRedisFeastAdapter(RedisFeastAdapter):
        def __init__(self, account_id: str) -> None:
            RedisFeastAdapter._features[account_id] = {
                "approved": stub_repository.approved_universe(),
                "fees": {
                    symbol: stub_repository.fee_override(symbol)
                    for symbol in stub_repository.approved_universe()
                    if stub_repository.fee_override(symbol)
                },
            }
            super().__init__(
                account_id=account_id,
                repository=stub_repository,
                feature_store_factory=lambda *_: NoopFeastStore(),
            )

    monkeypatch.setattr(universe_main, "RedisFeastAdapter", StubRedisFeastAdapter)

    client = TestClient(universe_main.app)
    response = client.get("/universe/approved", headers={"X-Account-ID": "company"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["account_id"] == "company"
    assert payload["instruments"] == ["BTC-USD"]
    assert payload["fee_overrides"]["BTC-USD"]["maker"] == 0.1
