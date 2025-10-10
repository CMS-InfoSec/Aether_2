"""Shared service wiring helpers.

This package collects the compatibility shims that allow the wider codebase to
boot in environments where optional dependencies are missing.  Earlier
iterations attempted to defend against this by inserting clever reload hooks
that swapped stubs back out for the real modules.  In practice the indirection
made the package brittle: legitimate imports such as
``from services.common.security import ADMIN_ACCOUNTS`` started failing because
the loader ended up handing pytest's temporary stubs back to the caller.

The module below focuses on deterministically exposing the production helpers.
Whenever we notice that a stub without a ``__file__`` attribute has been
inserted we simply discard it and import the implementation from disk again.
This keeps the convenience attributes working without making normal imports
fragile.
"""

from __future__ import annotations

from importlib import import_module
import sys
from types import ModuleType
from typing import Any, Dict, Tuple

_SUBMODULE_NAMES: Dict[str, str] = {
    "adapters": "services.common.adapters",
    "config": "services.common.config",
    "security": "services.common.security",
}

_REEXPORTS: Dict[str, Tuple[str, str]] = {
    # Adapter helpers
    "RedisFeastAdapter": ("services.common.adapters", "RedisFeastAdapter"),
    "TimescaleAdapter": ("services.common.adapters", "TimescaleAdapter"),
    "KafkaNATSAdapter": ("services.common.adapters", "KafkaNATSAdapter"),
    "KrakenSecretManager": ("services.common.adapters", "KrakenSecretManager"),
    "PublishError": ("services.common.adapters", "PublishError"),
    # Configuration helpers
    "get_timescale_session": ("services.common.config", "get_timescale_session"),
    "get_redis_client": ("services.common.config", "get_redis_client"),
    "get_kafka_producer": ("services.common.config", "get_kafka_producer"),
    "get_nats_producer": ("services.common.config", "get_nats_producer"),
    "get_feast_client": ("services.common.config", "get_feast_client"),
    # Security helpers
    "ADMIN_ACCOUNTS": ("services.common.security", "ADMIN_ACCOUNTS"),
    "DIRECTOR_ACCOUNTS": ("services.common.security", "DIRECTOR_ACCOUNTS"),
    "reload_admin_accounts": ("services.common.security", "reload_admin_accounts"),
    "set_default_session_store": (
        "services.common.security",
        "set_default_session_store",
    ),
    "require_admin_account": (
        "services.common.security",
        "require_admin_account",
    ),
    "require_authenticated_principal": (
        "services.common.security",
        "require_authenticated_principal",
    ),
    "require_dual_director_confirmation": (
        "services.common.security",
        "require_dual_director_confirmation",
    ),
    "ensure_admin_access": ("services.common.security", "ensure_admin_access"),
}


def _import_real_module(name: str) -> ModuleType:
    """Return the implementation for *name*, dropping pytest stubs when needed."""

    module = sys.modules.get(name)
    if isinstance(module, ModuleType) and getattr(module, "__file__", None):
        return module

    if name in sys.modules:
        # A stub without ``__file__`` was registered – discard it so the real
        # implementation can load.
        del sys.modules[name]

    module = import_module(name)
    return module


def _resolve_attribute(module_name: str, attribute: str) -> Any:
    module = _import_real_module(module_name)
    value = getattr(module, attribute)
    globals()[attribute] = value
    return value


def __getattr__(name: str) -> Any:
    if name in _REEXPORTS:
        module_name, attribute = _REEXPORTS[name]
        value = _resolve_attribute(module_name, attribute)
        globals()[name] = value
        return value
    if name in _SUBMODULE_NAMES:
        module = _import_real_module(_SUBMODULE_NAMES[name])
        globals()[name] = module
        return module
    raise AttributeError(f"module 'services.common' has no attribute '{name}'")


adapters = _import_real_module(_SUBMODULE_NAMES["adapters"])
config = _import_real_module(_SUBMODULE_NAMES["config"])
security = _import_real_module(_SUBMODULE_NAMES["security"])


__all__ = sorted({*(_REEXPORTS.keys()), *(_SUBMODULE_NAMES.keys())})

