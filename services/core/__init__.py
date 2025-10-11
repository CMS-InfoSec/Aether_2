"""Lazy exports for core service primitives used by the control plane."""

from __future__ import annotations

import importlib
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Tuple

import sys

__all__ = [
    "backpressure",
    "cache_warmer",
    "sequencer",
    "SequencerResult",
    "TradingSequencer",
    "sim_mode_router",
    "StartupManager",
    "StartupMode",
    "CacheWarmer",
    "CacheWarmupReport",
    "ModuleWarmupResult",
    "register_startup_manager",
    "register_cache_warmer",
    "startup_router",
    "warmup_router",
    "backpressure_app",
    "backpressure_controller",
    "BackpressureStatus",
    "IntentBackpressure",
]

_PACKAGE_ROOT = Path(__file__).resolve().parent
_TESTS_DIR = _PACKAGE_ROOT.parents[2] / "tests" / "services" / "core"

_locations = [str(_PACKAGE_ROOT)]
if _TESTS_DIR.is_dir():  # pragma: no branch - only runs when tests exist
    _locations.append(str(_TESTS_DIR))

__path__ = _locations  # type: ignore[assignment]
_spec: ModuleSpec | None = globals().get("__spec__")
if _spec is not None:
    _spec.submodule_search_locations = list(_locations)


_MODULE_EXPORTS: Dict[str, str] = {
    "backpressure": "services.core.backpressure",
    "cache_warmer": "services.core.cache_warmer",
    "sequencer": "services.core.sequencer",
}

_OBJECT_EXPORTS: Dict[str, Tuple[str, str]] = {
    "BackpressureStatus": ("services.core.backpressure", "BackpressureStatus"),
    "IntentBackpressure": ("services.core.backpressure", "IntentBackpressure"),
    "backpressure_app": ("services.core.backpressure", "app"),
    "backpressure_controller": (
        "services.core.backpressure",
        "backpressure_controller",
    ),
    "SequencerResult": ("services.core.sequencer", "SequencerResult"),
    "TradingSequencer": ("services.core.sequencer", "TradingSequencer"),
    "CacheWarmer": ("services.core.cache_warmer", "CacheWarmer"),
    "CacheWarmupReport": ("services.core.cache_warmer", "CacheWarmupReport"),
    "ModuleWarmupResult": ("services.core.cache_warmer", "ModuleWarmupResult"),
    "register_cache_warmer": ("services.core.cache_warmer", "register"),
    "warmup_router": ("services.core.cache_warmer", "router"),
    "StartupManager": ("services.core.startup_manager", "StartupManager"),
    "StartupMode": ("services.core.startup_manager", "StartupMode"),
    "register_startup_manager": ("services.core.startup_manager", "register"),
    "startup_router": ("services.core.startup_manager", "router"),
}


def _load_module(name: str) -> ModuleType:
    module_name = _MODULE_EXPORTS[name]
    module = importlib.import_module(module_name)
    if getattr(module, "__file__", None) is None:
        sys.modules.pop(module_name, None)
        module = importlib.import_module(module_name)
    globals()[name] = module
    return module


def _load_object(name: str) -> Any:
    module_name, attribute = _OBJECT_EXPORTS[name]
    module = importlib.import_module(module_name)
    try:
        value = getattr(module, attribute)
    except AttributeError:
        sys.modules.pop(module_name, None)
        module = importlib.import_module(module_name)
        value = getattr(module, attribute)
    globals()[name] = value
    return value


def _load_sim_mode_router() -> Any:
    try:
        module = importlib.import_module("services.core.sim_mode")
        router = getattr(module, "router")
    except (ImportError, AttributeError) as exc:  # pragma: no cover - fallback path
        from services.common.fastapi_stub import APIRouter

        fallback = APIRouter(prefix="/sim")

        async def _missing_sim_mode(*args: Any, **kwargs: Any) -> None:
            raise ModuleNotFoundError(
                "Simulation mode router requires SQLAlchemy and related dependencies",
            ) from exc

        fallback.add_api_route("/status", _missing_sim_mode, methods=["GET"])
        router = fallback
    globals()["sim_mode_router"] = router
    return router


def __getattr__(name: str) -> Any:
    if name in _MODULE_EXPORTS:
        return _load_module(name)
    if name in _OBJECT_EXPORTS:
        return _load_object(name)
    if name == "sim_mode_router":
        return _load_sim_mode_router()
    raise AttributeError(name)


def __dir__() -> list[str]:  # pragma: no cover - debug helper
    return sorted(set(globals()) | set(__all__))


# Preload module-level exports so ``from services.core import backpressure`` works
# even when pytest temporarily inserts namespace stubs.
for _module_name in ("backpressure", "cache_warmer", "sequencer"):
    _load_module(_module_name)

for _object_name in (
    "BackpressureStatus",
    "IntentBackpressure",
    "backpressure_app",
    "backpressure_controller",
    "SequencerResult",
    "TradingSequencer",
    "CacheWarmer",
    "CacheWarmupReport",
    "ModuleWarmupResult",
    "register_cache_warmer",
    "warmup_router",
    "StartupManager",
    "StartupMode",
    "register_startup_manager",
    "startup_router",
):
    _load_object(_object_name)
