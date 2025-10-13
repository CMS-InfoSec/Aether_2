"""Regression tests for the latency profiler metrics fallback."""

from __future__ import annotations

import asyncio
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


def test_latency_profiler_uses_metrics_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Latency profiling should remain operational without prometheus_client."""

    _purge_modules(["prometheus_client", "metrics", "latency_profiler"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "prometheus_client" or name.startswith("prometheus_client."):
            raise ModuleNotFoundError("prometheus_client unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    metrics = importlib.import_module("metrics")
    latency_profiler = importlib.import_module("latency_profiler")
    importlib.reload(latency_profiler)

    profiler = latency_profiler.LatencyProfiler(
        "test-service",
        database_url="sqlite:///:memory:",
    )

    async def _exercise() -> dict[str, dict[str, float | int | str]]:
        await profiler.observe("GET /ping", 12.5)
        return await profiler.snapshot()

    snapshots = asyncio.run(_exercise())

    observation = snapshots["GET /ping"]
    assert observation["p50"] == pytest.approx(12.5)
    assert observation["count"] == 1

    sample = metrics._REGISTRY.get_sample_value(  # type: ignore[attr-defined]
        "latency_ms",
        {"service": "test-service", "endpoint": "GET /ping"},
    )
    if sample is not None:
        assert sample == pytest.approx(12.5)
    else:
        histogram = profiler._histogram
        assert hasattr(histogram, "_values")
        assert histogram._values.get(("test-service", "GET /ping")) == pytest.approx(12.5)

