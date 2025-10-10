"""Utility helpers to stabilise imports from ``services.common`` during tests."""
from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Dict, Mapping, Tuple

# Modules that routinely receive pytest stubs.  We ensure the real implementations
# are loaded while preserving any overrides that tests intentionally provide.
_COMMON_MODULES = (
    "services.common",
    "services.common.config",
    "services.common.security",
    "services.common.adapters",
)

# Parent attributes that should always refer to the canonical submodules once the
# helpers are loaded.  This keeps ``services.common.config`` style imports working
# even after pytest injects temporary stand-ins.
_PARENT_SUBMODULES: Mapping[str, str] = {
    "config": "services.common.config",
    "security": "services.common.security",
    "adapters": "services.common.adapters",
}

# Convenience re-exports surfaced by ``services.common``.  When the package is
# reloaded we reattach these attributes so downstream imports keep functioning.
_PARENT_REEXPORTS: Mapping[str, Tuple[str, str]] = {
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


def _reload_with_overrides(module_name: str) -> ModuleType:
    """Return the implementation for ``module_name`` preserving stub overrides."""

    existing = sys.modules.get(module_name)
    if existing is None or getattr(existing, "__file__", None):
        module = importlib.import_module(module_name)
        return module

    overrides: Dict[str, object] = {
        key: value
        for key, value in existing.__dict__.items()
        if not key.startswith("__")
    }
    # Drop the stub so the real module can be imported.
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)
    # Re-apply pytest's overrides so tests depending on the stubbed behaviour
    # keep operating against the canonical implementation.
    for key, value in overrides.items():
        setattr(module, key, value)
    return module


def _ensure_fastapi_stub() -> None:
    """Ensure the repository's FastAPI shim is loaded instead of pytest stubs."""

    module = sys.modules.get("fastapi")
    should_reload = False
    if module is None:
        should_reload = True
    elif not isinstance(module, ModuleType) or getattr(module, "__file__", None) is None:
        should_reload = True
    else:
        required_attrs = ("FastAPI", "APIRouter", "status")
        if any(not hasattr(module, attr) for attr in required_attrs):
            should_reload = True

    if should_reload:
        sys.modules.pop("fastapi", None)
        module = importlib.import_module("fastapi")

    status_module = getattr(module, "status", None)
    if isinstance(status_module, ModuleType) and not hasattr(status_module, "HTTP_201_CREATED"):
        importlib.reload(module)


def ensure_common_helpers() -> None:
    """Guarantee the real ``services.common`` helpers are available."""

    loaded: Dict[str, ModuleType] = {}
    for name in _COMMON_MODULES:
        loaded[name] = _reload_with_overrides(name)

    parent = loaded["services.common"]
    for attribute, module_name in _PARENT_SUBMODULES.items():
        setattr(parent, attribute, loaded[module_name])

    for attribute, (module_name, source_attr) in _PARENT_REEXPORTS.items():
        setattr(parent, attribute, getattr(loaded[module_name], source_attr))

    _ensure_fastapi_stub()

