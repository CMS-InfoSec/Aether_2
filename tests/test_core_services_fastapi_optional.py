"""Ensure core service modules tolerate FastAPI being unavailable."""

from __future__ import annotations

import builtins
import importlib
import os
import sys
from dataclasses import dataclass
from types import ModuleType
from typing import Iterable

import pytest


def _purge_modules(prefixes: Iterable[str]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name, None)


@pytest.mark.parametrize(
    "module_name",
    [
        "anomaly_service",
        "compliance_filter",
        "compliance_scanner",
        "exposure_forecast",
        "report_service",
        "advisor_service",
        "capital_allocator",
        "benchmark_service",
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
        "services.report_service",
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
        "secrets_service",
        "sim_mode",
        "services.core.backpressure",
        "services.core.cache_warmer",
        "services.core.sim_mode",
        "services.core.startup_manager",
        "services.logging_export",
    ],
)
def test_core_services_import_without_fastapi(
    module_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Modules should fall back to the in-repo FastAPI shim when missing."""

    prefixes = ["fastapi", module_name]
    if module_name.startswith("ml."):
        prefixes.append("ml")
    _purge_modules(prefixes)

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object) -> ModuleType:
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
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

    module = importlib.import_module(module_name)

    for attr_name in ("FastAPI", "APIRouter"):
        candidate = getattr(module, attr_name, None)
        if callable(candidate):
            assert getattr(candidate, "__module__", "").startswith(
                "services.common.fastapi_stub"
            ), f"{attr_name} should originate from the FastAPI stub in {module_name}"
            break
    else:  # pragma: no cover - defensive guard if module stops exposing FastAPI types
        pytest.fail(f"{module_name} did not expose FastAPI or APIRouter for verification")
