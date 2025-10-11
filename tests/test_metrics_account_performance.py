import importlib

import pytest


def _fresh_metrics_module():
    module = importlib.import_module("metrics")
    return importlib.reload(module)


def test_account_performance_metrics_are_recorded() -> None:
    metrics = _fresh_metrics_module()
    metrics.init_metrics("unit-test")

    metrics.record_account_daily_pnl(
        "acct-1",
        realized=10.0,
        unrealized=5.0,
        fees=2.0,
        service="unit-test",
    )
    metrics.record_account_drawdown("acct-1", 0.12, service="unit-test")
    metrics.record_account_exposure(
        "acct-1",
        symbol="BTC-USD",
        exposure_usd=250_000,
        service="unit-test",
    )

    registry = metrics._REGISTRY  # noqa: SLF001 - accessing registry for assertions

    realized = registry.get_sample_value(
        "account_daily_realized_pnl_usd",
        {"service": "unit-test", "account_id": "acct-1"},
    )
    unrealized = registry.get_sample_value(
        "account_daily_unrealized_pnl_usd",
        {"service": "unit-test", "account_id": "acct-1"},
    )
    net = registry.get_sample_value(
        "account_daily_net_pnl_usd",
        {"service": "unit-test", "account_id": "acct-1"},
    )
    drawdown = registry.get_sample_value(
        "account_drawdown_pct",
        {"service": "unit-test", "account_id": "acct-1"},
    )
    exposure = registry.get_sample_value(
        "account_exposure_usd",
        {
            "service": "unit-test",
            "account_id": "acct-1",
            "symbol": "BTC-USD",
            "symbol_tier": "long_tail",
        },
    )

    assert realized == pytest.approx(10.0)
    assert unrealized == pytest.approx(5.0)
    assert net == pytest.approx(13.0)
    assert drawdown == pytest.approx(0.12)
    assert exposure == pytest.approx(250_000.0)
