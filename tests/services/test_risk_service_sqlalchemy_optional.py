"""Risk service import should fall back to the SQLAlchemy shim when needed."""

from __future__ import annotations

import builtins
import importlib
import sys
from collections.abc import Callable


def _block_sqlalchemy_imports(monkeypatch) -> None:
    original_import: Callable[..., object] = builtins.__import__

    def _guarded_import(  # type: ignore[override]
        name: str, globals=None, locals=None, fromlist=(), level: int = 0
    ) -> object:
        if name.startswith("sqlalchemy") and name not in sys.modules:
            raise ModuleNotFoundError(name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)


def test_risk_service_installs_sqlalchemy_stub(monkeypatch) -> None:
    """Risk service should reuse the in-repo SQLAlchemy stub when imports fail."""

    for module_name in list(sys.modules):
        if module_name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    monkeypatch.delitem(sys.modules, "risk_service", raising=False)
    _block_sqlalchemy_imports(monkeypatch)

    module = importlib.import_module("risk_service")

    assert module.create_engine.__module__ == "services.common.sqlalchemy_stub"

    engine = module.create_engine("sqlite://")
    assert getattr(engine, "url", "").startswith("sqlite")
