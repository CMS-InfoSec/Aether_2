"""Regression tests for the cross-asset analytics service configuration guards."""

from __future__ import annotations

import importlib
import sys

import pytest


def _reset_prometheus_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    import prometheus_client
    from prometheus_client import metrics as metrics_module
    from prometheus_client import registry as registry_module

    fresh_registry = registry_module.CollectorRegistry()
    monkeypatch.setattr(registry_module, "REGISTRY", fresh_registry)
    monkeypatch.setattr(metrics_module, "REGISTRY", fresh_registry, raising=False)
    monkeypatch.setattr(prometheus_client, "REGISTRY", fresh_registry, raising=False)

    class _GaugeStub:
        def __init__(self, *args: object, **kwargs: object) -> None:  # pragma: no cover - trivial
            pass

        def labels(self, **kwargs: object) -> "_GaugeStub":  # pragma: no cover - trivial
            return self

        def set(self, value: object) -> None:  # pragma: no cover - trivial
            return None

    monkeypatch.setattr(prometheus_client, "Gauge", _GaugeStub)


MODULE = "services.analytics.crossasset_service"
_CONFIG_VARS = ("CROSSASSET_DATABASE_URL", "ANALYTICS_DATABASE_URL", "DATABASE_URL")


def _import_module() -> object:
    sys.modules.pop(MODULE, None)
    return importlib.import_module(MODULE)


def test_requires_database_url_outside_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    """Production imports must fail fast when no DSN is configured."""

    for var in _CONFIG_VARS:
        monkeypatch.delenv(var, raising=False)

    monkeypatch.delitem(sys.modules, "pytest", raising=False)
    _reset_prometheus_registry(monkeypatch)

    sys.modules.pop(MODULE, None)
    with pytest.raises(RuntimeError, match="Cross-asset analytics database DSN is not configured"):
        importlib.import_module(MODULE)
    sys.modules.pop(MODULE, None)


def test_allows_sqlite_when_pytest_present(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local tests may rely on sqlite connections when pytest is active."""

    for var in _CONFIG_VARS:
        monkeypatch.delenv(var, raising=False)

    monkeypatch.setenv("CROSSASSET_DATABASE_URL", "sqlite:///./crossasset.db")

    _reset_prometheus_registry(monkeypatch)
    module = _import_module()
    try:
        assert getattr(module, "DATABASE_URL") == "sqlite:///./crossasset.db"
    finally:
        sys.modules.pop(MODULE, None)


def test_normalizes_timescale_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    """Timescale/PostgreSQL DSNs are normalised for SQLAlchemy usage."""

    for var in _CONFIG_VARS:
        monkeypatch.delenv(var, raising=False)

    monkeypatch.setenv(
        "CROSSASSET_DATABASE_URL",
        "timescale://user:secret@db.internal:5432/crossasset",
    )

    _reset_prometheus_registry(monkeypatch)
    module = _import_module()
    try:
        assert getattr(module, "DATABASE_URL") == (
            "postgresql+psycopg2://user:secret@db.internal:5432/crossasset"
        )
    finally:
        sys.modules.pop(MODULE, None)
