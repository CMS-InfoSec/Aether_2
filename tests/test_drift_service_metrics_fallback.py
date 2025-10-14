"""Regression coverage for the drift service metrics fallback."""
from __future__ import annotations

import builtins
import importlib
import sys
from typing import Iterable

import pytest

pytest.importorskip("fastapi")


def _purge_modules(prefixes: Iterable[str]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name)


def test_drift_service_uses_metrics_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """The drift service should continue exporting metrics without prometheus_client."""

    _purge_modules(["prometheus_client", "metrics", "drift_service"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "prometheus_client" or name.startswith("prometheus_client."):
            raise ModuleNotFoundError("prometheus_client unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    drift_service = importlib.import_module("drift_service")
    importlib.reload(drift_service)

    drift_service.DRIFT_FEATURES_TOTAL.set(5)
    payload = drift_service.generate_latest(drift_service.REGISTRY)

    assert drift_service.REGISTRY.__class__.__name__ == "_FallbackCollectorRegistry"
    assert drift_service.CONTENT_TYPE_LATEST.startswith("text/plain")
    assert b"drift_features_total" in payload

    lines = payload.decode().splitlines()
    data_lines = [line for line in lines if line.startswith("drift_features_total ")]
    assert data_lines, "drift_features_total sample missing from payload"
    _, value = data_lines[0].split(" ", 1)
    assert float(value) == pytest.approx(5.0)

