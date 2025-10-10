"""Shared service wiring helpers.

This package collects the compatibility shims that allow the wider codebase to
boot in environments where optional dependencies are missing.  The test suite
historically replaced :mod:`services.common.adapters` or
:mod:`services.common.security` with lightweight :class:`types.ModuleType`
stubs during collection.  If those stubs were inserted before the real modules
had been imported the application code ended up talking to incomplete
implementations and exploded with :class:`ImportError` when it attempted to
reach the production helpers.

To keep the package resilient we lazily proxy the commonly-used helpers instead
of binding them eagerly at import time.  When an attribute such as
``RedisFeastAdapter`` or ``require_admin_account`` is requested we attempt to
retrieve it from the currently-loaded module.  If a stub is present but lacks
the attribute we reload the real module from disk (when available) and cache the
result.  This preserves the convenience imports that the services expect while
remaining compatible with the test harness's dependency injection.
"""

from __future__ import annotations

from importlib import import_module
from importlib.util import find_spec
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
}


def _load_module(name: str) -> ModuleType:
    """Return the module identified by *name*.

    The helper ensures that bare :class:`types.ModuleType` stubs created by the
    test suite never permanently shadow the real implementation.  When a stub
    lacking a ``__file__`` attribute is encountered we reload the module from
    disk so that direct imports (``from services.common.adapters import
    TimescaleAdapter``) resolve to the production helpers rather than the empty
    placeholder.
    """

    module = sys.modules.get(name)
    if isinstance(module, ModuleType):
        if getattr(module, "__file__", None):
            return module
        module = _reload_from_source(name, module)
        if isinstance(module, ModuleType):
            return module
        # ``_reload_from_source`` only returns ``None`` when the import spec
        # cannot be resolved, in which case we fall back to a standard import.
    module = import_module(name)
    return module


def _reload_from_source(name: str, existing: ModuleType | None) -> ModuleType | None:
    """Reload *name* from disk when a stub without attributes is installed."""

    if existing is not None and getattr(existing, "__file__", None):
        return existing

    try:
        spec = find_spec(name)
    except (ValueError, ImportError):  # pragma: no cover - defensive guard
        spec = None
    if spec is None or spec.loader is None:
        loader = None
    else:
        loader = spec.loader

    # Remove the stub so ``import_module`` loads the real implementation.
    sys.modules.pop(name, None)
    try:
        module = import_module(name)
    except Exception:  # pragma: no cover - defensive guard
        # Restore the stub if reloading fails to avoid breaking the caller.
        if existing is not None:
            sys.modules[name] = existing
        raise

    # When ``find_spec`` failed above we still succeed because ``import_module``
    # consults the parent package's ``__path__``.  In that scenario ``loader`` is
    # ``None``; to keep debuggers happy we simply return the imported module.
    if loader is None:
        return module

    return module


def _resolve_attribute(module_name: str, attribute: str) -> Any:
    """Return *attribute* from *module_name*, reloading when required."""

    module = sys.modules.get(module_name)
    if module is not None and hasattr(module, attribute):
        return getattr(module, attribute)

    module = _reload_from_source(module_name, module)
    if module is not None and hasattr(module, attribute):
        return getattr(module, attribute)

    # Final attempt – import the module normally which will succeed for real
    # implementations and propagate the ImportError for missing dependencies.
    module = import_module(module_name)
    if hasattr(module, attribute):
        return getattr(module, attribute)

    raise AttributeError(f"module '{module_name}' has no attribute '{attribute}'")


def __getattr__(name: str) -> Any:
    if name in _REEXPORTS:
        module_name, attribute = _REEXPORTS[name]
        value = _resolve_attribute(module_name, attribute)
        globals()[name] = value
        return value
    if name in _SUBMODULE_NAMES:
        module = _load_module(_SUBMODULE_NAMES[name])
        globals()[name] = module
        return module
    raise AttributeError(f"module 'services.common' has no attribute '{name}'")


# Provide convenient access to the core submodules so ``services.common.config``
# and friends remain available as package attributes.
adapters = _load_module(_SUBMODULE_NAMES["adapters"])
config = _load_module(_SUBMODULE_NAMES["config"])
security = _load_module(_SUBMODULE_NAMES["security"])


__all__ = sorted({*(_REEXPORTS.keys()), *(_SUBMODULE_NAMES.keys())})

