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


def test_watchdog_without_fastapi(monkeypatch: pytest.MonkeyPatch) -> None:
    """The watchdog service should fall back to the FastAPI shim when missing."""

    _purge_modules(["fastapi", "watchdog"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    module = importlib.import_module("watchdog")

    assert module.FastAPI.__module__ == "services.common.fastapi_stub"
    assert module.Depends.__module__ == "services.common.fastapi_stub"

    app = module.app
    routes = {getattr(route, "path", None) for route in getattr(app, "routes", [])}
    assert "/oversight/status" in routes
