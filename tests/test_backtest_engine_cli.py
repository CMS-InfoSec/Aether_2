import argparse

import pytest

backtest_engine = pytest.importorskip(
    "backtest_engine", reason="backtest engine requires numpy/pandas", exc_type=ImportError
)


def test_spot_symbol_type_normalizes() -> None:
    result = backtest_engine._spot_symbol("ethusd")
    assert result == "ETH-USD"


def test_spot_symbol_type_rejects_derivatives() -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        backtest_engine._spot_symbol("btc-perp")
