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


def test_setup_health_checks_without_fastapi(monkeypatch: pytest.MonkeyPatch) -> None:
    """setup_health_checks should work when FastAPI is unavailable."""

    _purge_modules(["fastapi", "shared.health"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    health = importlib.import_module("shared.health")

    app = health.FastAPI()

    observed: list[str] = []

    def sync_check() -> dict[str, str]:
        observed.append("sync")
        return {"detail": "ok"}

    async def async_check() -> None:
        observed.append("async")

    health.setup_health_checks(app, {"sync": sync_check, "async": async_check})

    health_route = next(route for route in app.routes if route.path == "/healthz")
    payload = asyncio.run(health_route.endpoint())

    assert payload["status"] == "ok"
    assert payload["checks"]["sync"] == {"status": "ok", "detail": "ok"}
    assert payload["checks"]["async"] == {"status": "ok"}
    assert observed == ["sync", "async"]
