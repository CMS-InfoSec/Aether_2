from __future__ import annotations

import pytest

from services.universe.repository import MarketSnapshot, UniverseRepository


@pytest.fixture(autouse=True)
def reset_repository() -> None:
    UniverseRepository.reset()
    yield
    UniverseRepository.reset()


def _snapshot(**overrides: float | str) -> MarketSnapshot:
    defaults: dict[str, float | str] = {
        "base_asset": "ATOM",
        "quote_asset": "USD",
        "market_cap": UniverseRepository.MARKET_CAP_THRESHOLD * 1.5,
        "global_volume_24h": UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 3,
        "kraken_volume_24h": UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 3,
        "volatility_30d": UniverseRepository.VOLATILITY_THRESHOLD * 1.5,
    }
    defaults.update(overrides)
    return MarketSnapshot(**defaults)  # type: ignore[arg-type]


def test_approves_when_all_thresholds_met() -> None:
    UniverseRepository.seed_market_snapshots([_snapshot(base_asset="BTC")])

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == ["BTC-USD"]


def test_rejects_when_market_cap_below_threshold() -> None:
    UniverseRepository.seed_market_snapshots(
        [_snapshot(base_asset="ETH", market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 0.9)]
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == []


def test_rejects_when_global_volume_below_threshold() -> None:
    UniverseRepository.seed_market_snapshots(
        [
            _snapshot(
                base_asset="SOL",
                global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 0.5,
                kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 3,
            )
        ]
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == []


def test_rejects_when_kraken_volume_below_threshold() -> None:
    UniverseRepository.seed_market_snapshots(
        [
            _snapshot(
                base_asset="ADA",
                global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 3,
                kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 0.5,
            )
        ]
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == []


def test_rejects_when_volatility_below_threshold() -> None:
    UniverseRepository.seed_market_snapshots(
        [
            _snapshot(
                base_asset="AVAX",
                volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 0.5,
            )
        ]
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == []


def test_non_usd_pairs_are_ignored() -> None:
    UniverseRepository.seed_market_snapshots(
        [
            _snapshot(base_asset="BTC", quote_asset="USD"),
            _snapshot(base_asset="ETH", quote_asset="USDT"),
        ]
    )

    repo = UniverseRepository(account_id="company")

    assert repo.approved_universe() == ["BTC-USD"]
