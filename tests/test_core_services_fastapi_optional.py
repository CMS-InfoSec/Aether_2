"""Ensure core service modules tolerate FastAPI being unavailable."""

from __future__ import annotations

import builtins
import importlib
import os
import sys
from dataclasses import dataclass
from types import ModuleType
from typing import Dict, Iterable

import pytest


def _purge_modules(prefixes: Iterable[str]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name, None)


@pytest.mark.parametrize(
    "module_name",
    [
        "auth_service",
        "anomaly_service",
        "compliance_filter",
        "compliance_scanner",
        "behavior_service",
        "exposure_forecast",
        "report_service",
        "advisor_service",
        "capital_allocator",
        "benchmark_service",
        "capital_flow",
        "esg_filter",
        "governance_simulator",
        "policy_service",
        "kill_switch",
        "latency_profiler",
        "model_server",
        "training_service",
        "config_service",
        "config_sandbox",
        "compliance_pack",
        "pack_exporter",
        "taxlots",
        "override_service",
        "capital_optimizer",
        "alert_prioritizer",
        "alt_data",
        "tca_service",
        "sequencer",
        "services.ui.explain_service",
        "services.models.meta_learner",
        "services.models.model_zoo",
        "services.report_service",
        "services.builder.routes",
        "ops.metrics.cost_monitor",
        "signal_graph",
        "time_travel",
        "multiformat_export",
        "drift_service",
        "ml.training.training_service",
        "ml.policy.meta_strategy",
        "ml.features.feature_versioning",
        "ml.monitoring.drift_service",
        "ops.observability.latency_metrics",
        "prompt_refiner",
        "hitl_service",
        "services.risk.circuit_breakers",
        "services.risk.correlation_service",
        "services.risk.cvar_forecast",
        "services.risk.diversification_allocator",
        "services.risk.main",
        "services.risk.nav_forecaster",
        "services.risk.portfolio_risk",
        "services.risk.pretrade_sanity",
        "secrets_service",
        "services.secrets.main",
        "services.universe.main",
        "services.universe.universe_service",
        "sim_mode",
        "scenario_simulator",
        "oms_service",
        "services.core.backpressure",
        "services.core.cache_warmer",
        "services.core.sim_mode",
        "services.core.startup_manager",
        "services.logging_export",
        "services.oms.main",
        "services.oms.reconcile",
        "services.analytics.seasonality_service",
        "services.analytics.vwap_service",
        "services.backtest.stress_engine",
        "services.fees.fee_service",
    ],
)
def test_core_services_import_without_fastapi(
    module_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Modules should fall back to the in-repo FastAPI shim when missing."""

    prefixes = ["fastapi", module_name]
    if module_name.startswith("services.oms."):
        prefixes.append("services.oms")
    if module_name.startswith("services.analytics."):
        prefixes.append("services.analytics")
    if module_name.startswith("services.backtest."):
        prefixes.append("services.backtest")
    if module_name.startswith("services.fees."):
        prefixes.append("services.fees")
    if module_name.startswith("services.risk."):
        prefixes.append("services.risk")
    if module_name.startswith("services.models."):
        prefixes.append("services.models")
    if module_name.startswith("services.secrets."):
        prefixes.append("services.secrets")
    if module_name.startswith("services.universe."):
        prefixes.append("services.universe")
    if module_name.startswith("ml."):
        prefixes.append("ml")
    _purge_modules(prefixes)

    if module_name in {"capital_flow", "esg_filter"}:
        _purge_modules(["sqlalchemy"])

    real_import = builtins.__import__

    blocked_prefixes = ["fastapi"]
    if module_name in {"capital_flow", "esg_filter"}:
        blocked_prefixes.append("sqlalchemy")
    if module_name in {"secrets_service", "services.secrets.main"}:
        blocked_prefixes.append("starlette")
    if module_name.startswith("services.risk."):
        monkeypatch.setenv("RISK_ALLOW_INSECURE_DEFAULTS", "1")

    def _fake_import(name: str, *args: object, **kwargs: object) -> ModuleType:
        for prefix in blocked_prefixes:
            if name == prefix or name.startswith(prefix + "."):
                if name not in sys.modules:
                    raise ModuleNotFoundError(f"{prefix} unavailable")
                break
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    if module_name == "override_service":
        monkeypatch.setenv("OVERRIDE_ALLOW_SQLITE_FOR_TESTS", "1")
        monkeypatch.setenv("OVERRIDE_DATABASE_URL", "sqlite:///override.db")

    if module_name == "config_sandbox":
        monkeypatch.setenv("CONFIG_SANDBOX_ALLOW_SQLITE", "1")
        monkeypatch.setenv("CONFIG_SANDBOX_DATABASE_URL", "sqlite:///sandbox.db")

    if module_name == "multiformat_export":
        _purge_modules(["services.common.adapters"])
        adapters_stub = ModuleType("services.common.adapters")
        adapters_stub.TimescaleAdapter = object  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "services.common.adapters", adapters_stub)

        _purge_modules(["audit_mode"])
        audit_stub = ModuleType("audit_mode")

        @dataclass
        class _AuditorPrincipal:
            account_id: str

        def _require_auditor_identity(*_: object, **__: object) -> _AuditorPrincipal:
            return _AuditorPrincipal(account_id="test-account")

        audit_stub.AuditorPrincipal = _AuditorPrincipal  # type: ignore[attr-defined]
        audit_stub.require_auditor_identity = _require_auditor_identity  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "audit_mode", audit_stub)

    if module_name == "anomaly_service":
        monkeypatch.setenv("ANOMALY_DATABASE_URL", "sqlite:///anomaly.db")

    if module_name == "services.oms.main":
        _purge_modules(["services.common.adapters"])
        adapters_stub = ModuleType("services.common.adapters")

        class _StubTimescaleAdapter:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            @classmethod
            def flush_event_buffers(cls) -> Dict[str, int]:
                return {}

            def record_ack(self, *_: object, **__: object) -> None:
                return None

            def record_usage(self, *_: object, **__: object) -> None:
                return None

            def record_fill(self, *_: object, **__: object) -> None:
                return None

            def record_shadow_fill(self, *_: object, **__: object) -> None:
                return None

            def record_event(self, *_: object, **__: object) -> None:
                return None

        adapters_stub.TimescaleAdapter = _StubTimescaleAdapter  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "services.common.adapters", adapters_stub)

    module = importlib.import_module(module_name)

    if module_name == "capital_flow":
        # Restore the SQLAlchemy stub installed by tests.conftest so subsequent
        # parametrised runs continue to see the shimmed modules.
        from tests import conftest as _test_conftest

        _test_conftest._install_sqlalchemy_stub()

    for attr_name in ("FastAPI", "APIRouter"):
        candidate = getattr(module, attr_name, None)
        if callable(candidate):
            assert getattr(candidate, "__module__", "").startswith(
                "services.common.fastapi_stub"
            ), f"{attr_name} should originate from the FastAPI stub in {module_name}"
            break
    else:  # pragma: no cover - defensive guard if module stops exposing FastAPI types
        pytest.fail(f"{module_name} did not expose FastAPI or APIRouter for verification")
