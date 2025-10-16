from __future__ import annotations

import builtins
import importlib
import sys
from typing import Iterable

import pytest


def _purge_modules(prefixes: Iterable[str]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name)


def test_cost_monitor_uses_metrics_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cost monitor should expose metrics without prometheus_client."""

    _purge_modules([
        "prometheus_client",
        "metrics",
        "ops.metrics.cost_monitor",
    ])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "prometheus_client" or name.startswith("prometheus_client."):
            raise ModuleNotFoundError("prometheus_client unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    metrics = importlib.import_module("metrics")
    module = importlib.import_module("ops.metrics.cost_monitor")
    importlib.reload(module)

    registry = metrics.CollectorRegistry()
    monitor = module.CostMonitor(config=module.CostMonitorConfig(), registry=registry)

    with monitor._lock:  # noqa: SLF001 - exercising internal state for fallback
        monitor._service_cost_totals["alpha"] = 15.5
        monitor._latest_usage["alpha"] = module.ResourceUsage(pods=2)
        monitor._cost_per_trade_value = 1.2
        monitor._cost_per_pnl_value = 0.3
        monitor._update_metrics_locked()

    snapshot = module.generate_latest(registry)
    assert b"aether_cost_usd_total" in snapshot

    total_value = registry.get_sample_value("aether_cost_usd_total", {"service": "alpha"})
    assert total_value == pytest.approx(15.5)

    per_trade = registry.get_sample_value("aether_cost_per_trade")
    assert per_trade == pytest.approx(1.2)
