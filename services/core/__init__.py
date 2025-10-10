"""Core service primitives shared across the control plane."""

from __future__ import annotations

import importlib
from typing import Any

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


def __getattr__(name: str) -> Any:
    if name in {"backpressure", "cache_warmer", "sequencer"}:
        module = importlib.import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    raise AttributeError(name)
