"""Configuration regression tests for the capital optimizer service."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

import pytest


def _reload_capital_optimizer() -> ModuleType:
    sys.modules.pop("capital_optimizer", None)
    return importlib.import_module("capital_optimizer")


def test_capital_optimizer_requires_configured_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    """Production imports must fail when no database DSN is configured."""

    for key in (
        "CAPITAL_OPTIMIZER_DB_URL",
        "CAPITAL_OPTIMIZER_DATABASE_URL",
        "TIMESCALE_DSN",
        "DATABASE_URL",
    ):
        monkeypatch.delenv(key, raising=False)

    original_pytest = sys.modules.pop("pytest", None)
    try:
        with pytest.raises(RuntimeError) as excinfo:
            _reload_capital_optimizer()
        assert "Capital optimizer database DSN is not configured" in str(excinfo.value)
    finally:
        if original_pytest is not None:
            sys.modules["pytest"] = original_pytest
        sys.modules.pop("capital_optimizer", None)


def test_capital_optimizer_rejects_sqlite_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SQLite DSNs must be rejected when pytest is not present."""

    monkeypatch.setenv("CAPITAL_OPTIMIZER_DB_URL", "sqlite:///./capital_optimizer.db")

    original_pytest = sys.modules.pop("pytest", None)
    try:
        with pytest.raises(RuntimeError) as excinfo:
            _reload_capital_optimizer()
        assert "PostgreSQL/Timescale" in str(excinfo.value)
    finally:
        if original_pytest is not None:
            sys.modules["pytest"] = original_pytest
        sys.modules.pop("capital_optimizer", None)


def test_capital_optimizer_normalises_timescale_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    """Timescale DSNs should normalise to psycopg-compatible URLs."""

    for key in (
        "CAPITAL_OPTIMIZER_DB_URL",
        "CAPITAL_OPTIMIZER_DATABASE_URL",
        "DATABASE_URL",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("TIMESCALE_DSN", "timescale://user:pass@host:5432/db")

    module = _reload_capital_optimizer()

    try:
        assert module._database_url() == "postgresql+psycopg2://user:pass@host:5432/db"
    finally:
        sys.modules.pop("capital_optimizer", None)
