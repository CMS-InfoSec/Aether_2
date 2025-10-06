"""Configuration regression tests for the execution anomaly service."""

from __future__ import annotations

import importlib
import sys

import pytest


MODULE = "services.anomaly.execution_anomaly"
_CONFIG_VARS = (
    "EXECUTION_ANOMALY_DATABASE_URL",
    "ANALYTICS_DATABASE_URL",
    "DATABASE_URL",
)


def _import_module() -> object:
    sys.modules.pop(MODULE, None)
    return importlib.import_module(MODULE)


def test_requires_database_url_outside_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    """Production imports must fail when no DSN is configured."""

    for var in _CONFIG_VARS:
        monkeypatch.delenv(var, raising=False)

    original_pytest = sys.modules.pop("pytest", None)

    with pytest.raises(RuntimeError, match="Execution anomaly database DSN is not configured"):
        _import_module()

    sys.modules.pop(MODULE, None)
    if original_pytest is not None:
        sys.modules["pytest"] = original_pytest


def test_allows_sqlite_when_pytest_present(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local tests may rely on sqlite when pytest is active."""

    for var in _CONFIG_VARS:
        monkeypatch.delenv(var, raising=False)

    monkeypatch.setenv("EXECUTION_ANOMALY_DATABASE_URL", "sqlite:///:memory:")

    module = _import_module()
    try:
        assert getattr(module, "DATABASE_URL") == "sqlite:///:memory:"
    finally:
        sys.modules.pop(MODULE, None)


def test_normalizes_timescale_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    """Timescale DSNs are normalised for SQLAlchemy usage."""

    for var in _CONFIG_VARS:
        monkeypatch.delenv(var, raising=False)

    monkeypatch.setenv(
        "EXECUTION_ANOMALY_DATABASE_URL",
        "timescale://user:secret@db.internal:5432/anomalies",
    )

    import sqlalchemy

    captured: dict[str, str] = {}
    real_create_engine = sqlalchemy.create_engine

    def _fake_create_engine(url: str, **options: object):  # type: ignore[override]
        captured["url"] = url
        return real_create_engine("sqlite+pysqlite:///:memory:", **options)

    monkeypatch.setattr(sqlalchemy, "create_engine", _fake_create_engine)

    module = _import_module()
    try:
        assert getattr(module, "DATABASE_URL") == (
            "postgresql+psycopg2://user:secret@db.internal:5432/anomalies"
        )
        assert captured["url"] == "postgresql+psycopg2://user:secret@db.internal:5432/anomalies"
    finally:
        sys.modules.pop(MODULE, None)
