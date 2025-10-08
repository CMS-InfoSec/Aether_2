"""Regression tests for the config service database configuration guards."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MODULE_PATH = ROOT / "config_service.py"


def _load_module(module_name: str) -> object:
    """Import ``config_service`` under *module_name* and return the module."""

    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ModuleNotFoundError(module_name)

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def _dispose_module(module_name: str) -> None:
    module = sys.modules.pop(module_name, None)
    engine = getattr(module, "ENGINE", None)
    if engine is not None and hasattr(engine, "dispose"):
        engine.dispose()


def test_config_service_requires_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONFIG_DATABASE_URL", raising=False)

    module_name = "tests.config_service_missing_dsn"
    module = _load_module(module_name)
    try:
        with pytest.raises(RuntimeError, match="CONFIG_DATABASE_URL environment variable is required"):
            module._initialise_database(module.app)  # type: ignore[attr-defined]
    finally:
        _dispose_module(module_name)


def test_config_service_normalizes_timescale_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONFIG_ALLOW_SQLITE_FOR_TESTS", raising=False)
    monkeypatch.setenv("CONFIG_DATABASE_URL", "timescale://user:pass@example.com/config")

    captured: dict[str, object] = {}

    from sqlalchemy import create_engine as real_create_engine
    from sqlalchemy.pool import StaticPool

    def _fake_create_engine(url: str, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return real_create_engine(
            "sqlite:///:memory:",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )

    monkeypatch.setattr("sqlalchemy.create_engine", _fake_create_engine)

    module_name = "tests.config_service_timescale"
    module = _load_module(module_name)
    try:
        module._initialise_database(module.app)  # type: ignore[attr-defined]
        assert str(captured["url"]).startswith("postgresql+psycopg2://")
    finally:
        _dispose_module(module_name)


def test_config_service_rejects_sqlite_without_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONFIG_ALLOW_SQLITE_FOR_TESTS", raising=False)
    monkeypatch.setenv("CONFIG_DATABASE_URL", "sqlite:///./config.db")

    module_name = "tests.config_service_sqlite"
    module = _load_module(module_name)
    try:
        with pytest.raises(
            RuntimeError,
            match="Config service database URL must use a PostgreSQL/Timescale compatible scheme",
        ):
            module._initialise_database(module.app)  # type: ignore[attr-defined]
    finally:
        _dispose_module(module_name)


def test_config_service_allows_sqlite_when_flag_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONFIG_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("CONFIG_DATABASE_URL", "sqlite+pysqlite:///:memory:")

    module_name = "tests.config_service_sqlite_allowed"
    module = _load_module(module_name)
    try:
        module._initialise_database(module.app)  # type: ignore[attr-defined]
        engine = module.app.state.db_engine  # type: ignore[attr-defined]
        assert str(engine.url).startswith("sqlite+pysqlite:///:memory:")
    finally:
        _dispose_module(module_name)
