"""Ensure core service modules tolerate FastAPI being unavailable."""

from __future__ import annotations

import builtins
import importlib
import os
import sys
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
        "pack_exporter",
        "taxlots",
        "override_service",
        "capital_optimizer",
        "alert_prioritizer",
        "alt_data",
        "services.ui.explain_service",
        "services.report_service",
        "ops.metrics.cost_monitor",
    ],
)
def test_core_services_import_without_fastapi(
    module_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Modules should fall back to the in-repo FastAPI shim when missing."""

    _purge_modules(["fastapi", module_name])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object) -> ModuleType:
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    if module_name == "override_service":
        monkeypatch.setenv("OVERRIDE_ALLOW_SQLITE_FOR_TESTS", "1")
        monkeypatch.setenv("OVERRIDE_DATABASE_URL", "sqlite:///override.db")

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
