"""Configuration regression tests for the compliance scanner service."""

from __future__ import annotations

import importlib
import sys

import pytest

MODULE_NAME = "compliance_scanner"
_ENV_KEYS = (
    "COMPLIANCE_DATABASE_URL",
    "RISK_DATABASE_URL",
    "TIMESCALE_DSN",
    "DATABASE_URL",
)


def _reset_module(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    sys.modules.pop(MODULE_NAME, None)


def _restore_pytest(original: object | None) -> None:
    if original is not None:
        sys.modules["pytest"] = original


def test_compliance_scanner_requires_configured_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    """Even under pytest a database URL must be provided."""

    _reset_module(monkeypatch)

    with pytest.raises(RuntimeError, match="must be configured even under pytest"):
        importlib.import_module(MODULE_NAME)

    sys.modules.pop(MODULE_NAME, None)


def test_compliance_scanner_accepts_timescale_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    """Timescale/PostgreSQL URLs are normalised for SQLAlchemy use."""

    _reset_module(monkeypatch)
    monkeypatch.setenv(
        "COMPLIANCE_DATABASE_URL",
        "timescale://user:pass@timescale.example.com:5432/compliance",
    )

    module = importlib.import_module(MODULE_NAME)
    try:
        assert module._DB_URL.startswith("postgresql+")
    finally:
        sys.modules.pop(MODULE_NAME, None)


def test_compliance_scanner_rejects_sqlite_outside_pytest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production imports must not fall back to sqlite URLs."""

    _reset_module(monkeypatch)
    monkeypatch.setenv("COMPLIANCE_DATABASE_URL", "sqlite:///./compliance.db")
    original_pytest = sys.modules.pop("pytest", None)

    try:
        with pytest.raises(RuntimeError, match="PostgreSQL/Timescale compatible"):
            importlib.import_module(MODULE_NAME)
    finally:
        _restore_pytest(original_pytest)
        sys.modules.pop(MODULE_NAME, None)
