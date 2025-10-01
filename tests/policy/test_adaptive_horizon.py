import logging

import pytest

from services.policy import adaptive_horizon


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    adaptive_horizon._reset_cache()
    yield
    adaptive_horizon._reset_cache()


def test_get_horizon_trend_logs_change(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger=adaptive_horizon.logger.name)
    horizon = adaptive_horizon.get_horizon({"symbol": "BTC-USD", "regime": "trend"})

    assert horizon == 60 * 60
    assert any(record.message == "adaptive_horizon_change" for record in caplog.records)
    record = caplog.records[0]
    assert record.symbol == "BTC-USD"
    assert record.regime == "trend"
    assert record.horizon_seconds == horizon


def test_get_horizon_nested_state(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger=adaptive_horizon.logger.name)
    context = {"instrument": "eth-usd", "state": {"regime": "high_vol"}}
    horizon = adaptive_horizon.get_horizon(context)

    assert horizon == 5 * 60
    assert caplog.records[0].symbol == "ETH-USD"
    assert caplog.records[0].regime == "high_vol"


def test_repeated_calls_do_not_emit_duplicate_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger=adaptive_horizon.logger.name)
    context = {"symbol": "SOL-USD", "regime": "range"}

    first = adaptive_horizon.get_horizon(context)
    assert first == 15 * 60
    assert len(caplog.records) == 1

    caplog.clear()
    second = adaptive_horizon.get_horizon(context)
    assert second == 15 * 60
    assert not caplog.records
