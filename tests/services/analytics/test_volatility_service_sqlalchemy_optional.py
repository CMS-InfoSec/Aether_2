from __future__ import annotations

import builtins
import importlib
import sys
from collections.abc import Callable


def _block_sqlalchemy_imports(monkeypatch) -> None:
    """Prevent real SQLAlchemy from being imported during the test."""

    original_import: Callable[..., object] = builtins.__import__

    def _guarded_import(name: str, globals=None, locals=None, fromlist=(), level=0):  # type: ignore[override]
        if name.startswith("sqlalchemy") and name not in sys.modules:
            raise ModuleNotFoundError(name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)


def test_volatility_service_installs_sqlalchemy_stub(monkeypatch) -> None:
    """The volatility service should fall back to the in-repo SQLAlchemy shim."""

    for module_name in list(sys.modules):
        if module_name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    monkeypatch.delitem(sys.modules, "services.analytics.volatility_service", raising=False)
    _block_sqlalchemy_imports(monkeypatch)

    monkeypatch.setenv(
        "ANALYTICS_DATABASE_URL",
        "postgresql://stub:stub@localhost:5432/volatility",
    )

    module = importlib.import_module("services.analytics.volatility_service")

    assert module.create_engine.__module__ == "services.common.sqlalchemy_stub"

    session_factory = module._initialise_database()

    assert module.ENGINE is not None
    assert module.SESSION_FACTORY is session_factory

    session = session_factory()
    assert session.bind is module.ENGINE

    assert module.app.state.volatility_engine is module.ENGINE
    assert module.app.state.volatility_sessionmaker is session_factory

    assert module.DATABASE_URL is not None
    rendered_url = module.DATABASE_URL.render_as_string(hide_password=False)
    assert rendered_url == "postgresql+psycopg2://stub:stub@localhost:5432/volatility"
    assert getattr(module.ENGINE, "_aether_url", None) == rendered_url
