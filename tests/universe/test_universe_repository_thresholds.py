from __future__ import annotations

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


def test_approves_when_all_thresholds_met(universe_timescale: UniverseTimescaleFixture) -> None:
    universe_timescale.add_snapshot(
        base_asset="BTC",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 3,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 3,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == ["BTC-USD"]


def test_rejects_when_market_cap_below_threshold(
    universe_timescale: UniverseTimescaleFixture,
) -> None:
    universe_timescale.add_snapshot(
        base_asset="ETH",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 0.9,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 3,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 3,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == []


def test_rejects_when_global_volume_below_threshold(
    universe_timescale: UniverseTimescaleFixture,
) -> None:
    universe_timescale.add_snapshot(
        base_asset="SOL",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 0.5,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 3,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == []


def test_rejects_when_kraken_volume_below_threshold(
    universe_timescale: UniverseTimescaleFixture,
) -> None:
    universe_timescale.add_snapshot(
        base_asset="ADA",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 3,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 0.5,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == []


def test_rejects_when_volatility_below_threshold(
    universe_timescale: UniverseTimescaleFixture,
) -> None:
    universe_timescale.add_snapshot(
        base_asset="AVAX",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 3,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 3,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 0.5,
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == []


def test_non_usd_pairs_are_ignored(universe_timescale: UniverseTimescaleFixture) -> None:
    universe_timescale.add_snapshot(
        base_asset="BTC",
        quote_asset="USD",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 3,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 3,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
    )
    universe_timescale.add_snapshot(
        base_asset="ETH",
        quote_asset="EUR",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 3,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 3,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == ["BTC-USD"]


def test_leveraged_tokens_are_ignored(universe_timescale: UniverseTimescaleFixture) -> None:
    universe_timescale.add_snapshot(
        base_asset="BTC",
        quote_asset="USD",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 3,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 3,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
    )
    universe_timescale.add_snapshot(
        base_asset="BTCUP",
        quote_asset="USD",
        market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
        global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 3,
        kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 3,
        volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == ["BTC-USD"]


def test_manual_override_rejects_non_spot(universe_timescale: UniverseTimescaleFixture) -> None:
    repo = UniverseRepository(account_id="company")

    with pytest.raises(ValueError):
        repo.set_manual_override("BTC-PERP", approved=True, actor_id="company")
