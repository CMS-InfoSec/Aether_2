"""Strategy bus should reuse the SQLAlchemy shim when the real dependency is missing."""

from __future__ import annotations

import builtins
import importlib
import sys
from collections.abc import Iterable


def _purge_modules(prefixes: Iterable[str]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(f"{prefix}."):
                sys.modules.pop(name, None)


def _block_sqlalchemy_imports(monkeypatch) -> None:
    original_import = builtins.__import__

    def _guarded_import(name, globals=None, locals=None, fromlist=(), level: int = 0):
        if name.startswith("sqlalchemy") and name not in sys.modules:
            raise ModuleNotFoundError(name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)


def test_strategy_bus_installs_sqlalchemy_stub(monkeypatch) -> None:
    _purge_modules(["sqlalchemy", "strategy_bus", "services.common.sqlalchemy_stub"])
    _block_sqlalchemy_imports(monkeypatch)

    module = importlib.import_module("strategy_bus")

    assert module.SignalBase.__module__ == "services.common.sqlalchemy_stub"
    assert module.sessionmaker.__module__ == "services.common.sqlalchemy_stub"

    # The shim should leave behind a lightweight SQLAlchemy namespace for callers.
    assert "sqlalchemy" in sys.modules
    assert getattr(sys.modules["sqlalchemy"], "__spec__", None) is not None
