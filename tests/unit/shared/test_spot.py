from __future__ import annotations

from shared import spot


def test_is_spot_symbol_accepts_usd_pairs() -> None:
    assert spot.is_spot_symbol("btc-usd")
    assert spot.is_spot_symbol("ETH/USD")


def test_is_spot_symbol_rejects_non_usd_quotes() -> None:
    assert not spot.is_spot_symbol("ETH-USDT")
    assert not spot.is_spot_symbol("BTC-EUR")


def test_filter_spot_symbols_only_returns_usd_pairs() -> None:
    symbols = ["btc-usd", "eth-usdt", "ada-usd", "BTC-PERP", "", None]
    filtered = spot.filter_spot_symbols(symbols)
    assert filtered == ["BTC-USD", "ADA-USD"]


def test_is_spot_symbol_rejects_leveraged_suffixes() -> None:
    assert not spot.is_spot_symbol("ADAUP-USD")
    assert not spot.is_spot_symbol("BTCDOWN-USD")


def test_normalize_spot_symbol_handles_delimiters() -> None:
    assert spot.normalize_spot_symbol(" btc/usd ") == "BTC-USD"
    assert spot.normalize_spot_symbol("eth_usd") == "ETH-USD"
