"""Ensure metrics module remains importable when FastAPI is unavailable."""

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
                sys.modules.pop(name, None)


def test_metrics_import_without_fastapi(monkeypatch: pytest.MonkeyPatch) -> None:
    """metrics.setup_metrics should work with the lightweight FastAPI fallbacks."""

    _purge_modules(["fastapi", "metrics"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    metrics = importlib.import_module("metrics")

    app = metrics.FastAPI()
    metrics.setup_metrics(app, service_name="observer")

    assert any(
        middleware.cls is metrics.RequestTracingMiddleware
        for middleware in app.user_middleware
    )

    metrics_route = next(route for route in app.routes if route.path == "/metrics")
    response = asyncio.run(metrics_route.endpoint())

    assert getattr(response, "media_type", None) == metrics.CONTENT_TYPE_LATEST
    assert metrics.generate_latest(metrics._REGISTRY)
