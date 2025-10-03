from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.universe.repository import UniverseRepository
from tests.universe.conftest import UniverseTimescaleFixture


@pytest.fixture(autouse=True)
def configure_repository(universe_timescale: UniverseTimescaleFixture) -> None:
    UniverseRepository.reset()
    universe_timescale.rebind()
    universe_timescale.clear()
    yield
    UniverseRepository.reset()
    universe_timescale.rebind()
    universe_timescale.clear()


def test_repository_filters_stale_snapshots(universe_timescale: UniverseTimescaleFixture) -> None:
    fresh_ts = datetime.now(timezone.utc)
    stale_ts = fresh_ts - UniverseRepository.MARKET_SNAPSHOT_MAX_AGE - timedelta(minutes=5)

    universe_timescale.add_snapshot(
        base_asset="BTC",
        quote_asset="USD",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 2,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 2,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
        observed_at=fresh_ts,
    )
    universe_timescale.add_snapshot(
        base_asset="ETH",
        quote_asset="USD",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 2,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 2,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
        observed_at=stale_ts,
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == ["BTC-USD"]


def test_fee_override_requires_fresh_data(universe_timescale: UniverseTimescaleFixture) -> None:
    fresh_ts = datetime.now(timezone.utc)
    stale_ts = fresh_ts - UniverseRepository.FEE_OVERRIDE_MAX_AGE - timedelta(minutes=5)

    universe_timescale.add_snapshot(
        base_asset="BTC",
        quote_asset="USD",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 2,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 2,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
    )
    universe_timescale.add_fee_override(
        "BTC-USD",
        currency="USD",
        maker=0.1,
        taker=0.2,
        updated_at=fresh_ts,
    )
    universe_timescale.add_fee_override(
        "ETH-USD",
        currency="USD",
        maker=0.2,
        taker=0.3,
        updated_at=stale_ts,
    )

    repo = UniverseRepository(account_id="company")

    assert repo.fee_override("BTC-USD") == {"currency": "USD", "maker": 0.1, "taker": 0.2}
    assert repo.fee_override("ETH-USD") is None


def test_manual_override_persists_across_reset(universe_timescale: UniverseTimescaleFixture) -> None:
    universe_timescale.add_snapshot(
        base_asset="BTC",
        quote_asset="USD",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 2,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 2,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
    )

    repo = UniverseRepository(account_id="company")
    repo.set_manual_override("DOGE-USD", approved=True, actor_id="company", reason="liquidity waiver")
    assert "DOGE-USD" in repo.approved_universe()

    audit_entries = UniverseRepository.audit_entries()
    assert audit_entries
    assert audit_entries[-1].after["DOGE-USD"]["approved"] is True

    UniverseRepository.reset()
    universe_timescale.rebind()

    new_repo = UniverseRepository(account_id="company")
    assert "DOGE-USD" in new_repo.approved_universe()

    payloads = universe_timescale.config_payloads()
    assert payloads
    assert any("DOGE-USD" in payload for payload in payloads)
