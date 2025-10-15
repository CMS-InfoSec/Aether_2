"""Regression tests for the execution anomaly service FastAPI fallback."""

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


def test_execution_anomaly_imports_without_fastapi(monkeypatch: pytest.MonkeyPatch) -> None:
    """The execution anomaly module should fall back to the FastAPI shim."""

    _purge_modules([
        "fastapi",
        "services.anomaly.execution_anomaly",
    ])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    monkeypatch.setenv("EXECUTION_ANOMALY_DATABASE_URL", "sqlite:///:memory:")

    module = importlib.import_module("services.anomaly.execution_anomaly")

    assert module.FastAPI.__module__ == "services.common.fastapi_stub"
    assert module.Depends.__module__ == "services.common.fastapi_stub"
    assert module.status.__name__ == "fastapi.status"

    assert isinstance(module.app, module.FastAPI)
