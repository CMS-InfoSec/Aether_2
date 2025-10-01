from __future__ import annotations

from services.common.adapters import RedisFeastAdapter
from services.universe.repository import MarketSnapshot, UniverseRepository


def setup_function(function: object) -> None:
    UniverseRepository.reset()


def test_non_usd_symbols_are_filtered() -> None:
    UniverseRepository.seed_market_snapshots(
        [
            MarketSnapshot(
                base_asset="BTC",
                quote_asset="USD",
                market_cap=1.2e12,
                volume_24h=5.0e10,
                volatility_30d=0.45,
            ),
            MarketSnapshot(
                base_asset="ETH",
                quote_asset="USDT",
                market_cap=4.0e11,
                volume_24h=2.5e10,
                volatility_30d=0.50,
            ),
        ]
    )

    adapter = RedisFeastAdapter(account_id="admin-eu")
    instruments = adapter.approved_instruments()

    assert instruments == ["BTC-USD"]


def test_manual_overrides_are_honored() -> None:
    UniverseRepository.seed_market_snapshots(
        [
            MarketSnapshot(
                base_asset="BTC",
                quote_asset="USD",
                market_cap=1.2e12,
                volume_24h=5.0e10,
                volatility_30d=0.45,
            ),
            MarketSnapshot(
                base_asset="DOGE",
                quote_asset="USD",
                market_cap=1.0e7,
                volume_24h=1.0e6,
                volatility_30d=0.5,
            ),
        ]
    )

    repo = UniverseRepository(account_id="admin-eu")
    assert "DOGE-USD" not in repo.approved_universe()

    repo.set_manual_override("DOGE-USD", approved=True, actor_id="admin-eu", reason="Liquidity waiver")

    adapter = RedisFeastAdapter(account_id="admin-eu", repository=repo)
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
                volume_24h=1.5e9,
                volatility_30d=0.35,
            ),
            MarketSnapshot(
                base_asset="AVAX",
                quote_asset="USD",
                market_cap=3.5e10,
                volume_24h=2.0e9,
                volatility_30d=0.55,
            ),
        ]
    )

    repo = UniverseRepository(account_id="admin-eu")
    approved_universe = repo.approved_universe()

    assert "ADA-USD" not in approved_universe
    assert "AVAX-USD" in approved_universe
