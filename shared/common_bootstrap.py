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
    "services.analytics",
    "services.oms",
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

    def _reload() -> ModuleType:
        sys.modules.pop("fastapi", None)
        return importlib.import_module("fastapi")

    module = sys.modules.get("fastapi")
    if not isinstance(module, ModuleType) or getattr(module, "__file__", None) is None:
        module = _reload()

    try:
        stub = importlib.import_module("services.common.fastapi_stub")
    except ModuleNotFoundError:
        return

    def _ensure_module(name: str) -> ModuleType:
        submodule = sys.modules.get(name)
        if not isinstance(submodule, ModuleType):
            submodule = ModuleType(name)
            sys.modules[name] = submodule
        return submodule

    # Core exports frequently accessed directly from ``fastapi``.
    for name in ("FastAPI", "APIRouter", "HTTPException", "Request", "Depends"):
        if getattr(module, name, None) is None:
            setattr(module, name, getattr(stub, name))

    # Recreate the ``fastapi.status`` module so ``from fastapi import status`` works
    # even if pytest previously replaced the attribute with a placeholder.
    status_module = getattr(module, "status", None)
    if not isinstance(status_module, ModuleType):
        status_module = getattr(stub, "status", None)
        if isinstance(status_module, ModuleType):
            setattr(module, "status", status_module)

    if isinstance(status_module, ModuleType):
        required_status = (
            "HTTP_200_OK",
            "HTTP_201_CREATED",
            "HTTP_400_BAD_REQUEST",
            "HTTP_401_UNAUTHORIZED",
            "HTTP_403_FORBIDDEN",
            "HTTP_404_NOT_FOUND",
            "HTTP_422_UNPROCESSABLE_ENTITY",
            "HTTP_503_SERVICE_UNAVAILABLE",
        )
        for name in required_status:
            if getattr(status_module, name, None) is None:
                setattr(status_module, name, getattr(stub.status, name))
        sys.modules["fastapi.status"] = status_module

    # Populate common submodules expected by the production code and tests.
    applications_module = _ensure_module("fastapi.applications")
    setattr(module, "applications", applications_module)
    if getattr(applications_module, "FastAPI", None) is None:
        setattr(applications_module, "FastAPI", getattr(stub, "FastAPI", None))

    routing_module = _ensure_module("fastapi.routing")
    setattr(module, "routing", routing_module)
    if getattr(routing_module, "APIRouter", None) is None:
        setattr(routing_module, "APIRouter", getattr(stub, "APIRouter", None))

    encoders_module = _ensure_module("fastapi.encoders")
    setattr(module, "encoders", encoders_module)
    if getattr(encoders_module, "jsonable_encoder", None) is None:
        setattr(encoders_module, "jsonable_encoder", getattr(stub, "jsonable_encoder", None))

    exceptions_module = _ensure_module("fastapi.exceptions")
    setattr(module, "exceptions", exceptions_module)
    for attr in ("HTTPException", "RequestValidationError"):
        if getattr(exceptions_module, attr, None) is None:
            setattr(exceptions_module, attr, getattr(stub, attr, None))

    exception_handlers_module = _ensure_module("fastapi.exception_handlers")
    setattr(module, "exception_handlers", exception_handlers_module)
    handler = getattr(stub, "request_validation_exception_handler", None)
    if getattr(exception_handlers_module, "request_validation_exception_handler", None) is None:
        setattr(exception_handlers_module, "request_validation_exception_handler", handler)

    concurrency_module = _ensure_module("fastapi.concurrency")
    setattr(module, "concurrency", concurrency_module)
    run_in_threadpool = getattr(stub, "run_in_threadpool", None)
    if getattr(concurrency_module, "run_in_threadpool", None) is None:
        setattr(concurrency_module, "run_in_threadpool", run_in_threadpool)

    responses_module = _ensure_module("fastapi.responses")
    setattr(module, "responses", responses_module)
    for attr in ("Response", "JSONResponse", "HTMLResponse", "StreamingResponse"):
        value = getattr(module, attr, None) or getattr(stub, attr, None)
        if value is not None:
            setattr(module, attr, value)
            if getattr(responses_module, attr, None) is None:
                setattr(responses_module, attr, value)

    middleware_module = _ensure_module("fastapi.middleware")
    setattr(module, "middleware", middleware_module)
    cors_module = _ensure_module("fastapi.middleware.cors")
    setattr(middleware_module, "cors", cors_module)
    if getattr(cors_module, "CORSMiddleware", None) is None:
        setattr(cors_module, "CORSMiddleware", getattr(stub, "CORSMiddleware", None))

    testclient_module = _ensure_module("fastapi.testclient")
    setattr(module, "testclient", testclient_module)
    test_client = getattr(module, "TestClient", None) or getattr(stub, "TestClient", None)
    if test_client is not None:
        setattr(module, "TestClient", test_client)
        if getattr(testclient_module, "TestClient", None) is None:
            setattr(testclient_module, "TestClient", test_client)


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

