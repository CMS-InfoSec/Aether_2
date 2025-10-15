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


def test_latency_metrics_uses_metrics_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Latency metrics should operate when prometheus_client is missing."""

    _purge_modules(["prometheus_client", "metrics", "ops.observability.latency_metrics"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "prometheus_client" or name.startswith("prometheus_client."):
            raise ModuleNotFoundError("prometheus_client unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    metrics = importlib.import_module("metrics")
    module = importlib.import_module("ops.observability.latency_metrics")
    importlib.reload(module)

    registry = metrics.CollectorRegistry()
    latency_metrics = module.LatencyMetrics(registry=registry)

    snapshot = latency_metrics.observe(
        "policy_latency_ms",
        symbol="BTC",
        account_id="inst-42",
        latency_ms=12.0,
    )

    assert snapshot.p95 == pytest.approx(12.0)

    latest = module.generate_latest(registry)
    assert b"latency_p95_alert" in latest

    alert_value = registry.get_sample_value(
        "latency_p95_alert",
        {
            "metric": "policy_latency_ms",
            "symbol_tier": "mega_cap",
            "account_segment": "institutional",
        },
    )
    assert alert_value == 0
