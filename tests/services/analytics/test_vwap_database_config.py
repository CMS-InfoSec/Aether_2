"""Regression tests for VWAP analytics database configuration guards."""

from __future__ import annotations

import importlib
import sys

import pytest


MODULE_PATH = "services.analytics.vwap_service"


def _reload_vwap_module() -> object:
    sys.modules.pop(MODULE_PATH, None)
    return importlib.import_module(MODULE_PATH)


def _clear_database_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "VWAP_DATABASE_URL",
        "TIMESCALE_DATABASE_URI",
        "TIMESCALE_DSN",
        "DATABASE_URL",
    ):
        monkeypatch.delenv(key, raising=False)


def test_vwap_database_requires_configuration_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_database_env(monkeypatch)
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    module = _reload_vwap_module()

    with pytest.raises(RuntimeError, match="VWAP analytics database DSN is not configured"):
        module.VWAPAnalyticsService()


def test_vwap_database_normalizes_timescale_uris(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_database_env(monkeypatch)
    monkeypatch.setenv(
        "TIMESCALE_DATABASE_URI",
        "timescale://user:secret@localhost:5432/analytics",
    )

    module = _reload_vwap_module()

    resolved = module.VWAPAnalyticsService._database_url()
    assert resolved.startswith("postgresql+psycopg2://user:secret@localhost:5432/analytics")


def test_vwap_database_rejects_sqlite_outside_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_database_env(monkeypatch)
    monkeypatch.setenv("VWAP_DATABASE_URL", "sqlite:///./analytics.db")
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    module = _reload_vwap_module()

    with pytest.raises(RuntimeError, match="must use a PostgreSQL/Timescale compatible scheme"):
        module.VWAPAnalyticsService._database_url()

