"""Configuration regression tests for the training service."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Dict

import pytest

from sqlalchemy import create_engine as real_create_engine
from sqlalchemy.pool import StaticPool


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "training_service.py"


def _load_training_module(module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive safeguard
        raise ModuleNotFoundError(module_name)

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def _dispose_training_module(module_name: str) -> None:
    module = sys.modules.pop(module_name, None)
    if module is None:
        return

    engine = getattr(module, "ENGINE", None)
    if engine is not None and hasattr(engine, "dispose"):
        try:
            engine.dispose()
        except Exception:  # pragma: no cover - defensive cleanup
            pass


@pytest.fixture(autouse=True)
def _reset_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRAINING_DATABASE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("TRAINING_ALLOW_SQLITE_FOR_TESTS", raising=False)


def test_training_service_requires_database_url() -> None:
    module_name = "tests.training_service_missing_dsn"
    with pytest.raises(RuntimeError, match="must be configured"):
        _load_training_module(module_name)


def test_training_service_normalizes_timescale_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "TRAINING_DATABASE_URL",
        "timescale://user:pass@example.com:5432/training",
    )

    captured: Dict[str, Any] = {}

    def _fake_create_engine(url: str, **kwargs: Any):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return real_create_engine(
            "sqlite+pysqlite:///:memory:",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )

    monkeypatch.setattr("sqlalchemy.create_engine", _fake_create_engine)

    module_name = "tests.training_service_timescale"
    module = _load_training_module(module_name)
    try:
        assert str(captured["url"]).startswith("postgresql+psycopg2://")
        assert module.DATABASE_URL.startswith("postgresql+psycopg2://")  # type: ignore[attr-defined]
    finally:
        _dispose_training_module(module_name)


def test_training_service_rejects_sqlite_without_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRAINING_DATABASE_URL", "sqlite:///./training.db")

    module_name = "tests.training_service_sqlite_rejected"
    with pytest.raises(RuntimeError, match="PostgreSQL/Timescale compatible scheme"):
        _load_training_module(module_name)


def test_training_service_allows_sqlite_when_flag_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRAINING_DATABASE_URL", "sqlite:///./training.db")
    monkeypatch.setenv("TRAINING_ALLOW_SQLITE_FOR_TESTS", "1")

    captured: Dict[str, Any] = {}

    def _fake_create_engine(url: str, **kwargs: Any):
        captured["url"] = url
        return real_create_engine(
            "sqlite+pysqlite:///:memory:",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )

    monkeypatch.setattr("sqlalchemy.create_engine", _fake_create_engine)

    module_name = "tests.training_service_sqlite_allowed"
    module = _load_training_module(module_name)
    try:
        assert str(captured["url"]).startswith("sqlite")
        assert module.DATABASE_URL.startswith("sqlite")  # type: ignore[attr-defined]
    finally:
        _dispose_training_module(module_name)
