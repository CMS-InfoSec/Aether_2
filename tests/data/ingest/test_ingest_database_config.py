"""Configuration regression tests for data ingestion scripts."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from types import ModuleType
from typing import Tuple

import pytest


MODULE_MATRIX: Tuple[Tuple[str, str], ...] = (
    ("data.ingest.feature_jobs", "FEATURE_JOBS_ALLOW_SQLITE_FOR_TESTS"),
    ("data.ingest.coingecko_job", "COINGECKO_ALLOW_SQLITE_FOR_TESTS"),
    ("data.ingest.kraken_ws", "KRAKEN_WS_ALLOW_SQLITE_FOR_TESTS"),
)


def _import_fresh(module_name: str) -> ModuleType:
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


@pytest.fixture(autouse=True)
def _disable_optional_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent optional heavy dependencies from being imported during tests."""

    real_find_spec = importlib.util.find_spec

    def _guarded_find_spec(name: str, package: str | None = None):  # type: ignore[override]
        if name in {"pandas", "numpy", "feast", "requests", "aiohttp"}:
            return None
        return real_find_spec(name, package)

    monkeypatch.setattr(importlib.util, "find_spec", _guarded_find_spec)
    for module_name in (
        "pandas",
        "numpy",
        "numpy.random",
        "numpy.random._pickle",
        "feast",
        "requests",
        "aiohttp",
    ):
        sys.modules.pop(module_name, None)


@pytest.mark.parametrize("module_name, _", MODULE_MATRIX)
def test_ingest_scripts_require_database_url(
    monkeypatch: pytest.MonkeyPatch, module_name: str, _: str
) -> None:
    """Ingestion modules must fail fast when DATABASE_URL is absent."""

    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        _import_fresh(module_name)
    sys.modules.pop(module_name, None)


@pytest.mark.parametrize("module_name, _", MODULE_MATRIX)
def test_ingest_scripts_normalize_timescale_urls(
    monkeypatch: pytest.MonkeyPatch, module_name: str, _: str
) -> None:
    """Timescale URLs should normalise to psycopg-compatible SQLAlchemy URIs."""

    monkeypatch.setenv("DATABASE_URL", "timescale://user:secret@example.com/aether")
    module = _import_fresh(module_name)
    try:
        database_url = getattr(module, "DATABASE_URL")
    finally:
        sys.modules.pop(module_name, None)
    assert database_url.startswith("postgresql+psycopg2://")


@pytest.mark.parametrize("module_name, sqlite_flag", MODULE_MATRIX)
def test_ingest_scripts_reject_sqlite_outside_pytest(
    monkeypatch: pytest.MonkeyPatch, module_name: str, sqlite_flag: str
) -> None:
    """SQLite DSNs must not be accepted when pytest (or explicit flag) is absent."""

    monkeypatch.setenv("DATABASE_URL", "sqlite:///./ingest.db")
    saved_pytest = sys.modules.pop("pytest", None)
    try:
        with pytest.raises(RuntimeError, match="PostgreSQL/Timescale"):
            _import_fresh(module_name)
    finally:
        if saved_pytest is not None:
            sys.modules["pytest"] = saved_pytest
        sys.modules.pop(module_name, None)


@pytest.mark.parametrize("module_name, sqlite_flag", MODULE_MATRIX)
def test_ingest_scripts_allow_sqlite_when_flag_enabled(
    monkeypatch: pytest.MonkeyPatch, module_name: str, sqlite_flag: str
) -> None:
    """Explicit overrides may permit sqlite for local testing."""

    monkeypatch.setenv("DATABASE_URL", "sqlite+pysqlite:///:memory:")
    saved_pytest = sys.modules.pop("pytest", None)
    monkeypatch.setenv(sqlite_flag, "1")
    try:
        module = _import_fresh(module_name)
        database_url = getattr(module, "DATABASE_URL")
    finally:
        if saved_pytest is not None:
            sys.modules["pytest"] = saved_pytest
        monkeypatch.delenv(sqlite_flag, raising=False)
        sys.modules.pop(module_name, None)
    assert database_url == "sqlite+pysqlite:///:memory:"

