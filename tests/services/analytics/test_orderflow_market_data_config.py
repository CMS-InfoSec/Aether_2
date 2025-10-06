from __future__ import annotations

import importlib
import sys

import pytest


MODULE_PATH = "services.analytics.orderflow_service"


def _reload_module() -> object:
    sys.modules.pop(MODULE_PATH, None)
    return importlib.import_module(MODULE_PATH)


def _clear_market_data_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_var in (
        "ORDERFLOW_MARKET_DATA_URL",
        "ANALYTICS_MARKET_DATA_URL",
        "MARKET_DATA_DATABASE_URL",
        "ANALYTICS_DATABASE_URL",
        "DATABASE_URL",
    ):
        monkeypatch.delenv(env_var, raising=False)


def test_market_data_falls_back_to_sqlite_in_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_market_data_env(monkeypatch)

    module = _reload_module()

    dsn = module._resolve_market_data_dsn()
    assert dsn.startswith("sqlite"), dsn


def test_market_data_requires_configuration_outside_pytest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_market_data_env(monkeypatch)
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    with pytest.raises(RuntimeError, match="Orderflow market data DSN is not configured"):
        _reload_module()


def test_market_data_rejects_sqlite_outside_pytest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_market_data_env(monkeypatch)
    monkeypatch.setenv("ORDERFLOW_MARKET_DATA_URL", "sqlite:///orderflow.db")
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    with pytest.raises(RuntimeError, match="PostgreSQL/Timescale"):
        _reload_module()


def test_market_data_normalises_timescale_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_market_data_env(monkeypatch)
    monkeypatch.setenv(
        "ORDERFLOW_MARKET_DATA_URL",
        "timescale://analytics:secret@db:5432/market_data",
    )

    module = _reload_module()

    dsn = module._resolve_market_data_dsn()
    assert dsn.startswith("postgresql+"), dsn
