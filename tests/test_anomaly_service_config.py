"""Regression tests covering anomaly service database configuration guards."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest
from sqlalchemy import create_engine as real_create_engine
from sqlalchemy.pool import StaticPool


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "anomaly_service.py"


def _load_module(module_name: str) -> ModuleType:
    """Import ``anomaly_service`` under *module_name* and return the module."""

    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
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


def _stub_create_engine(monkeypatch: pytest.MonkeyPatch, captured: dict[str, object]) -> None:
    """Replace ``sqlalchemy.create_engine`` with a deterministic stub."""

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


def test_anomaly_service_requires_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANOMALY_DATABASE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)

    module_name = "tests.anomaly_missing_dsn"
    with pytest.raises(
        RuntimeError,
        match="ANOMALY_DATABASE_URL or TIMESCALE_DSN must be set to a PostgreSQL/Timescale DSN",
    ):
        _load_module(module_name)
    assert module_name not in sys.modules


def test_anomaly_service_normalizes_timescale_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANOMALY_DATABASE_URL", "timescale://user:pass@example.com/anomaly")
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)

    captured: dict[str, object] = {}
    _stub_create_engine(monkeypatch, captured)

    module_name = "tests.anomaly_timescale_config"
    _load_module(module_name)
    try:
        assert str(captured["url"]).startswith("postgresql+psycopg2://")
    finally:
        _dispose_module(module_name)


def test_anomaly_service_rejects_sqlite_without_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANOMALY_DATABASE_URL", "sqlite:///./anomaly.db")
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)

    original_pytest = sys.modules.get("pytest")
    try:
        if "pytest" in sys.modules:
            del sys.modules["pytest"]
        module_name = "tests.anomaly_sqlite_rejected"
        with pytest.raises(
            RuntimeError,
            match="Anomaly service database DSN must use a PostgreSQL/Timescale compatible scheme",
        ):
            _load_module(module_name)
        assert module_name not in sys.modules
    finally:
        if original_pytest is not None:
            sys.modules["pytest"] = original_pytest


def test_anomaly_service_allows_sqlite_during_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANOMALY_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)

    captured: dict[str, object] = {}
    _stub_create_engine(monkeypatch, captured)

    module_name = "tests.anomaly_sqlite_allowed"
    _load_module(module_name)
    try:
        assert str(captured["url"]).startswith("sqlite+pysqlite:///:memory:")
    finally:
        _dispose_module(module_name)
