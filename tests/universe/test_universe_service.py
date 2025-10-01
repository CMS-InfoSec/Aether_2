from __future__ import annotations

from fastapi.testclient import TestClient

from services.common.adapters import RedisFeastAdapter
from services.universe.repository import MarketSnapshot, UniverseRepository
from services.universe import main as universe_main


def setup_function(function: object) -> None:
    UniverseRepository.reset()


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
                quote_asset="USDT",
                market_cap=4.0e11,
                global_volume_24h=2.5e10,
                kraken_volume_24h=1.2e10,
                volatility_30d=0.50,
            ),
        ]
    )

    adapter = RedisFeastAdapter(account_id="company")
    instruments = adapter.approved_instruments()

    assert instruments == ["BTC-USD"]


def test_manual_overrides_are_honored() -> None:
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
                base_asset="DOGE",
                quote_asset="USD",
                market_cap=1.0e7,
                global_volume_24h=1.0e6,
                kraken_volume_24h=5.0e5,
                volatility_30d=0.5,
            ),
        ]
    )

    repo = UniverseRepository(account_id="company")
    assert "DOGE-USD" not in repo.approved_universe()

    repo.set_manual_override("DOGE-USD", approved=True, actor_id="company", reason="Liquidity waiver")

    adapter = RedisFeastAdapter(account_id="company", repository=repo)
    instruments = adapter.approved_instruments()

    assert "DOGE-USD" in instruments

    audit_entries = UniverseRepository.audit_entries()
    assert audit_entries
    assert audit_entries[-1].action == "universe.manual_override"
    assert audit_entries[-1].after["DOGE-USD"]["approved"] is True


def test_volatility_threshold_filters_low_and_high_risk_assets() -> None:
    UniverseRepository.seed_market_snapshots(
        [
            MarketSnapshot(
                base_asset="ADA",
                quote_asset="USD",
                market_cap=2.0e10,
                global_volume_24h=1.5e9,
                kraken_volume_24h=8.0e8,
                volatility_30d=0.35,
            ),
            MarketSnapshot(
                base_asset="AVAX",
                quote_asset="USD",
                market_cap=3.5e10,
                global_volume_24h=2.0e9,
                kraken_volume_24h=1.2e9,
                volatility_30d=0.55,
            ),
        ]
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

    adapter = RedisFeastAdapter(account_id="company", repository_factory=factory)

    instruments = adapter.approved_instruments()
    assert instruments == ["BTC-USD"]
    assert created and created[-1].account_id == "company"
    assert created[-1].approved_calls == 1

    fee = adapter.fee_override("BTC-USD")
    assert fee == {"currency": "USD", "maker": 0.2, "taker": 0.3}
    assert created[-1].fee_override_calls == ["BTC-USD"]


def test_fastapi_endpoint_uses_cached_repository(monkeypatch) -> None:
    stub_repository = StubUniverseRepository()

    class StubRedisFeastAdapter(RedisFeastAdapter):
        def __init__(self, account_id: str) -> None:
            super().__init__(account_id=account_id, repository=stub_repository)

    monkeypatch.setattr(universe_main, "RedisFeastAdapter", StubRedisFeastAdapter)

    client = TestClient(universe_main.app)
    response = client.get("/universe/approved", headers={"X-Account-ID": "company"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["account_id"] == "company"
    assert payload["instruments"] == ["BTC-USD"]
    assert payload["fee_overrides"]["BTC-USD"]["maker"] == 0.1
