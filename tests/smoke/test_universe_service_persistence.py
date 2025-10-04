import importlib
import sys
from importlib import util
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine as _sa_create_engine, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import StaticPool

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("sqlalchemy")

_REAL_CREATE_ENGINE = _sa_create_engine
_SERVICE_MODULE = "services.universe.universe_service"


class _EngineProxy:
    """Proxy that mimics a PostgreSQL engine while delegating to SQLite."""

    def __init__(self, inner: Engine) -> None:
        self._inner = inner
        self.url = make_url("postgresql+psycopg2://universe:test@localhost/universe")

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)

    def dispose(self) -> None:  # pragma: no cover - passthrough
        self._inner.dispose()


def _reload_service(monkeypatch: pytest.MonkeyPatch, sqlite_url: str) -> Any:
    monkeypatch.setenv("UNIVERSE_DATABASE_URL", "postgresql://universe:test@localhost/universe")

    def _patched_create_engine(url: str, **kwargs: Any) -> _EngineProxy:  # type: ignore[override]
        assert url.startswith("postgresql")
        inner = _REAL_CREATE_ENGINE(
            sqlite_url,
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        return _EngineProxy(inner)

    monkeypatch.setattr("sqlalchemy.create_engine", _patched_create_engine, raising=False)

    importlib.import_module("services")

    universe_pkg = "services.universe"
    if universe_pkg not in sys.modules:
        pkg_spec = util.spec_from_file_location(
            universe_pkg,
            ROOT / "services" / "universe" / "__init__.py",
        )
        if pkg_spec and pkg_spec.loader:
            package = util.module_from_spec(pkg_spec)
            sys.modules[universe_pkg] = package
            pkg_spec.loader.exec_module(package)

    if _SERVICE_MODULE in sys.modules:
        del sys.modules[_SERVICE_MODULE]

    spec = util.spec_from_file_location(
        _SERVICE_MODULE,
        ROOT / "services" / "universe" / "universe_service.py",
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("Failed to load universe service module for testing")

    module = util.module_from_spec(spec)
    sys.modules[_SERVICE_MODULE] = module
    spec.loader.exec_module(module)
    module.ensure_database()
    return module


def _install_migration_stub(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    def _stub() -> None:
        from sqlalchemy.dialects.postgresql import JSONB
        from sqlalchemy.ext.compiler import compiles

        @compiles(JSONB, "sqlite")
        def _compile_jsonb_sqlite(type_, compiler, **kw):  # pragma: no cover - SQLAlchemy hook
            return "JSON"

        module.Base.metadata.create_all(bind=module.ENGINE)

    monkeypatch.setattr(module, "run_migrations", _stub)


@pytest.mark.smoke
def test_universe_overrides_survive_restart_and_are_shared(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sqlite_path = tmp_path / "universe-service.db"
    module = _reload_service(monkeypatch, f"sqlite:///{sqlite_path}")
    _install_migration_stub(module, monkeypatch)
    module.run_migrations()
    module.ENGINE.dispose()

    with module.SessionLocal() as session:
        request = module.OverrideRequest(symbol="btc/usd", enabled=True, reason="restart")
        response = module.override_symbol(request, session=session, actor_account="ops-admin")
        stored_event_id = response.audit_event_id

    module.ENGINE.dispose()
    if _SERVICE_MODULE in sys.modules:
        del sys.modules[_SERVICE_MODULE]

    module_reloaded = _reload_service(monkeypatch, f"sqlite:///{sqlite_path}")
    _install_migration_stub(module_reloaded, monkeypatch)
    module_reloaded.run_migrations()
    module_reloaded.ENGINE.dispose()

    with module_reloaded.SessionLocal() as session:
        overrides = module_reloaded._latest_manual_overrides(session)
        assert "BTC-USD" in overrides

        audit_entries = session.execute(select(module_reloaded.AuditLog)).scalars().all()
        assert stored_event_id in [entry.event_id for entry in audit_entries]

    other_engine = _REAL_CREATE_ENGINE(f"sqlite:///{sqlite_path}", future=True)
    try:
        with other_engine.connect() as connection:
            rows = connection.execute(text("SELECT entity_id FROM audit_log"))
            entity_ids = [row[0] for row in rows]
        assert entity_ids == ["BTC-USD"]
    finally:
        other_engine.dispose()
