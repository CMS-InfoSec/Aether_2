"""Regression coverage for tracing metrics when Prometheus is unavailable."""

from __future__ import annotations

import builtins
import importlib
import sys
from types import SimpleNamespace
from typing import Iterable

import pytest


def _purge_modules(prefixes: Iterable[str]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name, None)


def test_tracing_metrics_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tracing should keep exporting stage latency metrics without prometheus_client."""

    _purge_modules(["prometheus_client", "metrics", "common.utils.tracing"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "prometheus_client" or name.startswith("prometheus_client."):
            raise ModuleNotFoundError("prometheus_client unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    metrics = importlib.import_module("metrics")
    tracing = importlib.import_module("common.utils.tracing")
    importlib.reload(tracing)

    histogram = tracing._STAGE_LATENCY_HISTOGRAM
    assert histogram is not None

    tracing.SpanExportResult = SimpleNamespace(SUCCESS=True)  # type: ignore[attr-defined]

    exporter = tracing._PrometheusSpanExporter()
    span = SimpleNamespace(
        attributes={"trading.stage": "ingest"},
        start_time=0,
        end_time=1_500_000_000,
    )

    histogram.labels(stage="book").observe(0.25)
    exporter.export([span])

    assert hasattr(histogram, "_values")
    values = getattr(histogram, "_values", {})
    assert values.get(("book",)) == pytest.approx(0.25)
    assert values.get(("ingest",)) == pytest.approx(1.5)
