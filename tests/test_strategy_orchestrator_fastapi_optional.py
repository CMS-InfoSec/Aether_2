"""Strategy orchestrator should remain importable without FastAPI."""

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


def test_strategy_orchestrator_without_fastapi(monkeypatch: pytest.MonkeyPatch) -> None:
    """The module should fall back to the in-repo FastAPI shim when unavailable."""

    _purge_modules([
        "fastapi",
        "strategy_orchestrator",
        "services.common.security",
    ])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    module = importlib.import_module("strategy_orchestrator")

    # The fallback FastAPI shim should expose the same interface used by the module.
    app = module.app
    assert getattr(app, "title", None) == "Strategy Orchestrator"

    routes = {route.path for route in app.routes}
    assert "/strategy/status" in routes
    assert "/strategy/register" in routes
