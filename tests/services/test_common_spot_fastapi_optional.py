from __future__ import annotations

import builtins
import importlib
import sys
from types import ModuleType

import pytest


def _purge_modules(prefixes: tuple[str, ...]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name, None)


def test_spot_requires_only_fastapi_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    """``services.common.spot`` should fall back to the FastAPI shim when needed."""

    prefixes = ("fastapi", "services.common.spot")
    _purge_modules(prefixes)

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object) -> ModuleType:
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    module = importlib.import_module("services.common.spot")

    assert module.HTTPException.__module__.startswith("services.common.fastapi_stub")
    assert module.status.__name__ == "fastapi.status"
