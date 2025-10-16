"""Override service should operate with the SQLAlchemy shim installed."""

from __future__ import annotations

import builtins
import importlib
import sys
from collections.abc import Callable
from datetime import datetime, timezone


def _block_sqlalchemy_imports(monkeypatch) -> None:
    original_import: Callable[..., object] = builtins.__import__

    def _guarded_import(  # type: ignore[override]
        name: str, globals=None, locals=None, fromlist=(), level: int = 0
    ) -> object:
        if name.startswith("sqlalchemy") and name not in sys.modules:
            raise ModuleNotFoundError(name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)


def test_override_service_installs_sqlalchemy_stub(monkeypatch) -> None:
    """Importing the override service should install the SQLAlchemy shim."""

    for module_name in list(sys.modules):
        if module_name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    monkeypatch.delitem(sys.modules, "override_service", raising=False)

    monkeypatch.setenv("OVERRIDE_DATABASE_URL", "sqlite:///:memory:")

    _block_sqlalchemy_imports(monkeypatch)

    module = importlib.import_module("override_service")

    assert module.create_engine.__module__ == "services.common.sqlalchemy_stub"
    assert module.SessionLocal.__module__ == "services.common.sqlalchemy_stub"

    session = module.SessionLocal()
    assert session.bind is not None
    assert getattr(session.bind, "url", "").startswith("sqlite")

    entry = module.OverrideLogEntry(
        intent_id="intent",
        account_id="acct",
        actor="ops",
        decision=module.OverrideDecision.APPROVE,
        reason="stub",
        ts=datetime.now(timezone.utc),
    )

    session.merge(entry)
    assert entry in getattr(session, "_merged_instances", [])
