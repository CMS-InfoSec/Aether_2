"""Regression tests for the health probe metrics fallback."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType


def _purge_modules(prefix: str) -> None:
    for name in list(sys.modules):
        if name == prefix or name.startswith(f"{prefix}."):
            sys.modules.pop(name, None)


def test_health_probe_imports_without_prometheus(monkeypatch) -> None:
    """The health probe should rely on the metrics fallback when Prometheus is absent."""

    def _missing_dependency(*_: object, **__: object) -> ModuleType:
        raise ModuleNotFoundError("prometheus_client unavailable")

    monkeypatch.setattr(
        "shared.dependency_loader.load_dependency", _missing_dependency
    )
    _purge_modules("prometheus_client")
    _purge_modules("metrics")
    _purge_modules("health_probe")

    module = importlib.import_module("health_probe")

    module.PIPELINE_LATENCY_GAUGE.set(123.0)
    module.WS_BOOK_STALENESS_GAUGE.set(456.0)
    module.TRADING_BLOCK_GAUGE.set(1.0)

    assert getattr(module.PIPELINE_LATENCY_GAUGE, "_values", {}) == {(): 123.0}
    assert getattr(module.WS_BOOK_STALENESS_GAUGE, "_values", {}) == {(): 456.0}
    assert getattr(module.TRADING_BLOCK_GAUGE, "_values", {}) == {(): 1.0}
