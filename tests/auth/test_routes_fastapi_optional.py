"""Ensure the auth routes remain importable when FastAPI is unavailable."""

from __future__ import annotations

import builtins
import importlib
import sys
from types import ModuleType

import pytest


def _purge_module(name: str) -> None:
    for key in list(sys.modules):
        if key == name or key.startswith(name + "."):
            sys.modules.pop(key, None)


def test_auth_routes_import_without_fastapi(monkeypatch: pytest.MonkeyPatch) -> None:
    """The module should fall back to the in-repo FastAPI stub when missing."""

    _purge_module("fastapi")
    _purge_module("auth.routes")

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object) -> ModuleType:
        if (name == "fastapi" or name.startswith("fastapi.")) and name not in sys.modules:
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    module = importlib.import_module("auth.routes")

    router = getattr(module, "router")
    assert router.__class__.__module__.startswith(
        "services.common.fastapi_stub",
    ), "router should originate from the FastAPI shim when the dependency is missing"
