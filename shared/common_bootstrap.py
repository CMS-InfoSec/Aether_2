"""Utility helpers to stabilise imports from ``services.common`` during tests."""
from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Callable, Dict, Mapping, Tuple

try:  # pragma: no cover - defensive import when sitecustomize missing
    from sitecustomize import _ensure_common_namespace, _ensure_services_namespace
except Exception:  # pragma: no cover - sitecustomize not executed
    _ensure_services_namespace = None  # type: ignore[assignment]
    _ensure_common_namespace = None  # type: ignore[assignment]

# Modules that routinely receive pytest stubs.  We ensure the real implementations
# are loaded while preserving any overrides that tests intentionally provide.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_TESTS_ROOT = _PROJECT_ROOT / "tests"

_COMMON_MODULES = (
    "services",
    "services.common",
    "services.common.config",
    "services.common.security",
    "services.common.adapters",
    "services.common.precision",
    "services.common.schemas",
)

# Parent attributes that should always refer to the canonical submodules once the
# helpers are loaded.  This keeps ``services.common.config`` style imports working
# even after pytest injects temporary stand-ins.
_PARENT_SUBMODULES: Mapping[str, str] = {
    "config": "services.common.config",
    "security": "services.common.security",
    "adapters": "services.common.adapters",
    "precision": "services.common.precision",
    "schemas": "services.common.schemas",
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
    if existing is None:
        module = importlib.import_module(module_name)
        return module

    module_file = getattr(existing, "__file__", None)
    if module_file and not str(module_file).startswith(str(_TESTS_ROOT)):
        return existing

    overrides: Dict[str, object] = {
        key: value
        for key, value in existing.__dict__.items()
        if not key.startswith("__")
    }
    # Drop the stub so the real module can be imported.
    sys.modules.pop(module_name, None)
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        module_path = Path(__file__).resolve().parents[1] / (
            module_name.replace(".", "/") + ".py"
        )
        if module_path.exists():
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                return module
        if existing is not None:
            sys.modules[module_name] = existing
            return existing
        raise

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


def _ensure_httpx_module() -> None:
    """Reload the lightweight ``httpx`` shim when pytest leaves placeholders."""

    required_attrs = ("AsyncClient", "Request", "Response", "HTTPError")
    module = sys.modules.get("httpx")
    if isinstance(module, ModuleType) and getattr(module, "__file__", None):
        if all(hasattr(module, attr) for attr in required_attrs):
            return

    overrides: Dict[str, object] = {}
    if isinstance(module, ModuleType):
        overrides = {
            key: value
            for key, value in module.__dict__.items()
            if not key.startswith("__")
        }

    sys.modules.pop("httpx", None)
    module = importlib.import_module("httpx")

    for key, value in overrides.items():
        setattr(module, key, value)


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
    _ensure_httpx_module()

    if _ensure_services_namespace is not None:
        _ensure_services_namespace()
    if _ensure_common_namespace is not None:
        _ensure_common_namespace()

    _install_module_guard()


class _ModuleGuard(dict):
    __slots__ = ("_guarded", "_ensurer", "_installing")

    def __init__(
        self,
        base: Mapping[str, ModuleType],
        guarded: tuple[str, ...],
        ensurer: "Callable[[], None]",
    ) -> None:
        super().__init__(base)
        self._guarded = guarded
        self._ensurer = ensurer
        self._installing = False

    def __setitem__(self, key: str, value: object) -> None:  # type: ignore[override]
        super().__setitem__(key, value)
        if self._installing:
            return
        if key not in self._guarded:
            return
        if isinstance(value, ModuleType):
            module_file = getattr(value, "__file__", None)
            if module_file and not str(module_file).startswith(str(_TESTS_ROOT)):
                return
        self._installing = True
        try:
            self._ensurer()
        finally:
            self._installing = False


_MODULE_GUARD_INSTALLED = False


def _install_module_guard() -> None:
    global _MODULE_GUARD_INSTALLED
    if _MODULE_GUARD_INSTALLED:
        return

    modules = sys.modules
    if isinstance(modules, _ModuleGuard):
        _MODULE_GUARD_INSTALLED = True
        return

    guard = _ModuleGuard(modules, tuple(_COMMON_MODULES), ensure_common_helpers)
    sys.modules = guard  # type: ignore[assignment]
    _MODULE_GUARD_INSTALLED = True

