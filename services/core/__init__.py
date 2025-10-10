"""Core service primitives shared across the control plane."""

from __future__ import annotations

import importlib
import importlib.util
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType
from typing import Any

import sys

from . import backpressure as backpressure
from . import cache_warmer as cache_warmer
from . import sequencer as sequencer
from .backpressure import (
    BackpressureStatus,
    IntentBackpressure,
    app as backpressure_app,
    backpressure_controller,
)
from .cache_warmer import (
    CacheWarmer,
    CacheWarmupReport,
    ModuleWarmupResult,
    register as register_cache_warmer,
    router as warmup_router,
)
from .sequencer import SequencerResult, TradingSequencer
from .sim_mode import router as sim_mode_router
from .startup_manager import (
    StartupManager,
    StartupMode,
    register as register_startup_manager,
    router as startup_router,
)

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
]

_PACKAGE_ROOT = Path(__file__).resolve().parent
_PROJECT_ROOT = _PACKAGE_ROOT.parents[2]
_TESTS_DIR = _PROJECT_ROOT / "tests" / "services" / "core"

if _TESTS_DIR.is_dir():  # pragma: no branch - only runs when tests exist
    locations = list(globals().get("__path__", []) or [])
    test_path = str(_TESTS_DIR)
    if test_path not in locations:
        locations.append(test_path)
        globals()["__path__"] = locations  # type: ignore[assignment]
    spec: ModuleSpec | None = globals().get("__spec__")
    if spec is not None:
        search = list(spec.submodule_search_locations or [])
        if test_path not in search:
            search.append(test_path)
            spec.submodule_search_locations = search


def _load_test_mirror(name: str) -> ModuleType | None:
    """Load ``tests.services.core.<name>`` when the canonical module is absent."""

    mirror_fullname = f"tests.services.core.{name}"
    try:
        return importlib.import_module(mirror_fullname)
    except ModuleNotFoundError:
        pass

    module_path = _TESTS_DIR / f"{name}.py"
    if module_path.exists():
        spec = importlib.util.spec_from_file_location(mirror_fullname, module_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            try:
                sys.modules[mirror_fullname] = module
                spec.loader.exec_module(module)
            except Exception:  # pragma: no cover - optional deps may be absent
                sys.modules.pop(mirror_fullname, None)
                return None
            return module

    package_dir = _TESTS_DIR / name
    if package_dir.is_dir():
        init_file = package_dir / "__init__.py"
        if init_file.exists():
            spec = importlib.util.spec_from_file_location(
                mirror_fullname,
                init_file,
                submodule_search_locations=[str(package_dir)],
            )
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                try:
                    sys.modules[mirror_fullname] = module
                    spec.loader.exec_module(module)
                    module.__path__ = [str(package_dir)]  # type: ignore[attr-defined]
                except Exception:  # pragma: no cover - keep bootstrap resilient
                    sys.modules.pop(mirror_fullname, None)
                    return None
                return module
    return None


def __getattr__(name: str) -> Any:
    if name in {"backpressure", "cache_warmer", "sequencer"}:
        module = importlib.import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module

    fullname = f"{__name__}.{name}"
    try:
        module = importlib.import_module(fullname)
    except ModuleNotFoundError as exc:
        module = _load_test_mirror(name)
        if module is None:
            raise AttributeError(name) from exc
    globals()[name] = module
    return module
