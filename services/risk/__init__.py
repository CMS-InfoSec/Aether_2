"""Lazy export loader for the risk services package.

Pytest frequently injects lightweight stubs into ``sys.modules['services.risk']``
when dependency-heavy suites replace individual modules.  Downstream code – and
several integration tests – still expect legacy imports such as
``from services.risk import portfolio_risk`` to succeed even after those stubs
run.  Historically the package provided a conventional ``__init__`` that
implicitly re-exported the canonical submodules, but after the file was emptied
those imports began raising ``ImportError``.

To restore the previous behaviour we lazily import and cache the canonical
submodules on demand.  When pytest has already registered stand-ins we preserve
any overridden attributes while still sourcing the real module from disk.
"""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Dict

_EXPORTED_MODULES: Dict[str, str] = {
    "portfolio_risk": "portfolio_risk",
    "engine": "engine",
    "cvar_forecast": "cvar_forecast",
    "diversification_allocator": "diversification_allocator",
    "circuit_breakers": "circuit_breakers",
    "correlation_service": "correlation_service",
    "nav_forecaster": "nav_forecaster",
    "exit_rules": "exit_rules",
    "position_sizer": "position_sizer",
    "pretrade_sanity": "pretrade_sanity",
    "stablecoin_monitor": "stablecoin_monitor",
}


def __getattr__(name: str) -> ModuleType:
    target = _EXPORTED_MODULES.get(name)
    if target is None:  # pragma: no cover - standard attribute error path
        raise AttributeError(name)

    module = importlib.import_module(f"{__name__}.{target}")
    globals()[name] = module
    return module


def __dir__() -> list[str]:  # pragma: no cover - tooling convenience
    return sorted(list(globals().keys()) + list(_EXPORTED_MODULES.keys()))


__all__ = list(_EXPORTED_MODULES.keys())
