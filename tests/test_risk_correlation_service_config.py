from __future__ import annotations

import importlib
import sys
import types
from dataclasses import dataclass
from pathlib import Path

import pytest


def _install_alert_manager_stub() -> types.ModuleType | None:
    """Provide a minimal alert manager implementation for module reloads."""

    original = sys.modules.get("services.alert_manager")
    module = types.ModuleType("services.alert_manager")

    @dataclass(slots=True)
    class _RiskEvent:
        event_type: str
        severity: str
        description: str
        labels: dict[str, str] | None = None

    class _AlertManager:
        def __init__(self, *_, **__) -> None:
            pass

        def handle_risk_event(self, *_: object, **__: object) -> None:  # pragma: no cover - noop
            return None

    module.AlertManager = _AlertManager  # type: ignore[attr-defined]
    module.RiskEvent = _RiskEvent  # type: ignore[attr-defined]
    module.get_alert_metrics = lambda: object()  # type: ignore[attr-defined]

    sys.modules["services.alert_manager"] = module
    return original


def _install_pandas_stub() -> types.ModuleType | None:
    """Provide a light-weight pandas stub so imports succeed without numpy."""

    original = sys.modules.get("pandas")
    module = types.ModuleType("pandas")

    class _Frame:  # pragma: no cover - placeholder for import-time use only
        def __init__(self, *_, **__):
            pass

        def pct_change(self, *_, **__):
            return self

        def dropna(self, *_, **__):
            return self

        def corr(self, *_, **__):
            return self

        def iterrows(self):
            return iter(())

    class _Series:  # pragma: no cover - placeholder for import-time use only
        def __init__(self, *_, **__):
            pass

    module.DataFrame = _Frame  # type: ignore[attr-defined]
    module.Series = _Series  # type: ignore[attr-defined]
    module.Index = tuple  # type: ignore[attr-defined]

    sys.modules["pandas"] = module
    return original


def _reload_correlation_service() -> None:
    sys.modules.pop("services.risk.correlation_service", None)
    importlib.import_module("services.risk.correlation_service")


@pytest.fixture(autouse=True)
def _ensure_services_package(monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        monkeypatch.syspath_prepend(str(project_root))


def test_correlation_service_requires_configured_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    original_alert_manager = _install_alert_manager_stub()
    original_pandas = _install_pandas_stub()

    monkeypatch.delenv("RISK_CORRELATION_DATABASE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("RISK_MARKETDATA_URL", raising=False)
    monkeypatch.delenv("RISK_MARKETDATA_DATABASE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DATABASE_URI", raising=False)

    original_pytest = sys.modules.pop("pytest", None)
    try:
        with pytest.raises(RuntimeError, match="Risk correlation database DSN"):
            _reload_correlation_service()
    finally:
        if original_pytest is not None:
            sys.modules["pytest"] = original_pytest
        if original_alert_manager is not None:
            sys.modules["services.alert_manager"] = original_alert_manager
        else:
            sys.modules.pop("services.alert_manager", None)
        if original_pandas is not None:
            sys.modules["pandas"] = original_pandas
        else:
            sys.modules.pop("pandas", None)
        sys.modules.pop("services.risk.correlation_service", None)


def test_correlation_service_rejects_sqlite_dsn_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    original_alert_manager = _install_alert_manager_stub()
    original_pandas = _install_pandas_stub()

    monkeypatch.setenv("RISK_CORRELATION_DATABASE_URL", "sqlite:///./correlations.db")
    monkeypatch.delenv("RISK_MARKETDATA_URL", raising=False)
    monkeypatch.delenv("RISK_MARKETDATA_DATABASE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DATABASE_URI", raising=False)

    original_pytest = sys.modules.pop("pytest", None)
    try:
        with pytest.raises(RuntimeError, match="PostgreSQL"):
            _reload_correlation_service()
    finally:
        if original_pytest is not None:
            sys.modules["pytest"] = original_pytest
        if original_alert_manager is not None:
            sys.modules["services.alert_manager"] = original_alert_manager
        else:
            sys.modules.pop("services.alert_manager", None)
        if original_pandas is not None:
            sys.modules["pandas"] = original_pandas
        else:
            sys.modules.pop("pandas", None)
        sys.modules.pop("services.risk.correlation_service", None)
