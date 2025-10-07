"""Tests for the Kraken WebSocket ingestion configuration guards."""

from __future__ import annotations

import pytest

from services.kraken_ws_ingest import KrakenConfig


def _config(**overrides: object) -> KrakenConfig:
    pairs = overrides.pop("pairs", ["btc-usd"])
    kafka_bootstrap_servers = overrides.pop("kafka_bootstrap_servers", "kafka:9092")
    return KrakenConfig(pairs=list(pairs), kafka_bootstrap_servers=kafka_bootstrap_servers, **overrides)


def test_pairs_normalized_and_deduplicated() -> None:
    config = _config(pairs=["btc/usd", "BTC-USD", "Eth_Usd"])
    assert config.pairs == ["BTC-USD", "ETH-USD"]


@pytest.mark.parametrize(
    "pairs",
    [
        ["BTC-USDT"],
        ["ETH-PERP"],
        ["ADA-MARGIN"],
        [""],
    ],
)
def test_rejects_non_spot_pairs(pairs: list[str]) -> None:
    with pytest.raises(ValueError):
        _config(pairs=pairs)


def test_requires_at_least_one_spot_pair() -> None:
    with pytest.raises(ValueError):
        _config(pairs=["BTC-USDT", "ETH-PERP"])
