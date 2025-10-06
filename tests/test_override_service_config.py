"""Regression tests for override service database configuration guards."""

from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest

import sqlalchemy


@pytest.fixture
def stub_create_engine(monkeypatch):
    """Patch ``sqlalchemy.create_engine`` to avoid external connections."""

    real_create_engine = sqlalchemy.create_engine

    def _fake_create_engine(url: str, **kwargs):  # type: ignore[override]
        engine = real_create_engine("sqlite+pysqlite:///:memory:", future=True)
        engine.captured_url = url  # type: ignore[attr-defined]
        engine.captured_kwargs = kwargs  # type: ignore[attr-defined]
        return engine

    monkeypatch.setattr(sqlalchemy, "create_engine", _fake_create_engine)


def _reload_override_service(monkeypatch) -> SimpleNamespace:
    sys.modules.pop("override_service", None)
    module = importlib.import_module("override_service")
    return module  # type: ignore[return-value]


def test_override_service_requires_database_url(monkeypatch):
    monkeypatch.delenv("OVERRIDE_DATABASE_URL", raising=False)
    monkeypatch.delenv("OVERRIDE_ALLOW_SQLITE_FOR_TESTS", raising=False)
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    sys.modules.pop("override_service", None)
    with pytest.raises(RuntimeError, match="OVERRIDE_DATABASE_URL must be configured"):
        importlib.import_module("override_service")

    sys.modules.pop("override_service", None)


def test_override_service_allows_sqlite_with_flag(monkeypatch, stub_create_engine, tmp_path):
    sqlite_url = f"sqlite:///{tmp_path / 'override.db'}"
    monkeypatch.setenv("OVERRIDE_DATABASE_URL", sqlite_url)
    monkeypatch.setenv("OVERRIDE_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    module = _reload_override_service(monkeypatch)
    try:
        assert str(module.ENGINE.captured_url) == sqlite_url  # type: ignore[attr-defined]
        kwargs = module.ENGINE.captured_kwargs  # type: ignore[attr-defined]
        assert kwargs["connect_args"]["check_same_thread"] is False
    finally:
        module.ENGINE.dispose()
        sys.modules.pop("override_service", None)


def test_override_service_rejects_sqlite_without_flag(monkeypatch):
    sqlite_url = "sqlite:///./override.db"
    monkeypatch.setenv("OVERRIDE_DATABASE_URL", sqlite_url)
    monkeypatch.delenv("OVERRIDE_ALLOW_SQLITE_FOR_TESTS", raising=False)
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    sys.modules.pop("override_service", None)
    with pytest.raises(RuntimeError, match="Override service database URL"):
        importlib.import_module("override_service")

    sys.modules.pop("override_service", None)


def test_override_service_normalizes_timescale_url(monkeypatch, stub_create_engine):
    monkeypatch.setenv("OVERRIDE_DATABASE_URL", "timescale://user:pass@host/db")
    monkeypatch.delenv("OVERRIDE_ALLOW_SQLITE_FOR_TESTS", raising=False)
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    module = _reload_override_service(monkeypatch)
    try:
        normalized = str(module.ENGINE.captured_url)  # type: ignore[attr-defined]
        assert normalized.startswith("postgresql+psycopg2://")
    finally:
        module.ENGINE.dispose()
        sys.modules.pop("override_service", None)

