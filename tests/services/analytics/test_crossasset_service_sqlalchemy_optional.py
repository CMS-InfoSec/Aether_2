"""Regression tests covering the cross-asset service SQLAlchemy fallback."""

from __future__ import annotations

import builtins
import importlib
import sys
from collections.abc import Callable
from datetime import datetime, timezone


def _block_sqlalchemy_imports(monkeypatch) -> None:
    """Prevent the real SQLAlchemy package from loading during the test."""

    original_import: Callable[..., object] = builtins.__import__

    def _guarded_import(name: str, globals=None, locals=None, fromlist=(), level=0):  # type: ignore[override]
        if name.startswith("sqlalchemy") and name not in sys.modules:
            raise ModuleNotFoundError(name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)


def test_crossasset_service_uses_local_store_when_sqlalchemy_missing(monkeypatch, tmp_path) -> None:
    """The cross-asset service should fall back to the shimmed local store."""

    for module_name in list(sys.modules):
        if module_name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    monkeypatch.delitem(sys.modules, "services.analytics.crossasset_service", raising=False)
    _block_sqlalchemy_imports(monkeypatch)

    monkeypatch.setenv("ANALYTICS_STATE_DIR", str(tmp_path))

    module = importlib.import_module("services.analytics.crossasset_service")

    assert module.SQLALCHEMY_STUB is True
    assert module.SQLALCHEMY_AVAILABLE is False
    assert module.USE_LOCAL_STORE is True
    assert module.ENGINE is None
    assert module.SESSION_FACTORY is None

    store = module._ensure_local_store()
    store.seed_series(
        "BTC/USD",
        (
            {
                "bucket_start": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "close": 100.0,
            },
        ),
    )

    closes, observed_ts = store.load_series("BTC/USD", window=5)
    assert len(closes) == 5
    assert observed_ts is not None
