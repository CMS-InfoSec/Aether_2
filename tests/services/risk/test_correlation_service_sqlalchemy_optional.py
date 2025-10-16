"""Correlation service should fall back to the SQLAlchemy shim when needed."""

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


def test_correlation_service_uses_inmemory_persistence(monkeypatch) -> None:
    """Importing the correlation service should install the shim-backed fallback."""

    for module_name in list(sys.modules):
        if module_name.startswith("sqlalchemy") or module_name.startswith(
            "services.risk.correlation_service"
        ):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    monkeypatch.delitem(sys.modules, "services.risk", raising=False)

    _block_sqlalchemy_imports(monkeypatch)

    module = importlib.import_module("services.risk.correlation_service")

    assert module.SQLALCHEMY_AVAILABLE is False
    assert isinstance(module.correlation_persistence, module._InMemoryPersistence)

    module._prime_price_cache(symbols=("BTC-USD",), lookback=5)
