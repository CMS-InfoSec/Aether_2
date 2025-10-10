"""Repository-local FastAPI compatibility shim.

The real FastAPI dependency is optional in many of the services because the
unit tests focus on the business logic instead of the HTTP layer.  When
FastAPI is unavailable we fall back to the lightweight implementations provided
by :mod:`services.common.fastapi_stub` so modules can still be imported.

To avoid circular imports during package initialisation we load the stub lazily
the first time an attribute is requested.  Subsequent lookups reuse the cached
module and mirror the behaviour of the real FastAPI package as closely as the
fallback allows.
"""

from __future__ import annotations

from importlib import import_module
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType
import sys
from typing import Any, Dict

_STUB: ModuleType | None = None
__all__: list[str] = []


def _populate_submodules(stub: ModuleType) -> None:
    """Expose the expected FastAPI subpackages for the lightweight shim."""

    status_module = getattr(stub, "status", None)
    run_in_threadpool = getattr(stub, "run_in_threadpool", None)
    globals()["status"] = status_module
    globals()["run_in_threadpool"] = run_in_threadpool

    status_exports: Dict[str, object] = {}
    if status_module is not None:
        status_exports = {
            name: getattr(status_module, name)
            for name in dir(status_module)
            if name.isupper()
        }

    submodules = {
        "fastapi.testclient": {"TestClient": getattr(stub, "TestClient", None)},
        "fastapi.responses": {
            "Response": getattr(stub, "Response", None),
            "JSONResponse": getattr(stub, "JSONResponse", None),
            "HTMLResponse": getattr(stub, "HTMLResponse", None),
            "StreamingResponse": getattr(stub, "StreamingResponse", None),
        },
        "fastapi.exception_handlers": {
            "request_validation_exception_handler": getattr(
                stub,
                "request_validation_exception_handler",
                None,
            )
        },
        "fastapi.applications": {"FastAPI": getattr(stub, "FastAPI", None)},
        "fastapi.routing": {"APIRouter": getattr(stub, "APIRouter", None)},
        "fastapi.exceptions": {
            "HTTPException": getattr(stub, "HTTPException", None),
            "RequestValidationError": getattr(stub, "RequestValidationError", None),
        },
        "fastapi.encoders": {"jsonable_encoder": getattr(stub, "jsonable_encoder", None)},
        "fastapi.concurrency": {
            "run_in_threadpool": getattr(stub, "run_in_threadpool", None)
        },
        "fastapi.middleware": {},
        "fastapi.middleware.cors": {
            "CORSMiddleware": getattr(stub, "CORSMiddleware", None)
        },
        "fastapi.status": status_exports,
    }

    for module_name, attributes in submodules.items():
        module = sys.modules.get(module_name)
        if module is None:
            module = ModuleType(module_name)
            sys.modules[module_name] = module
        for attr, value in attributes.items():
            if value is not None:
                setattr(module, attr, value)

    if "fastapi.middleware" in sys.modules and "fastapi.middleware.cors" in sys.modules:
        middleware_module = sys.modules["fastapi.middleware"]
        setattr(middleware_module, "cors", sys.modules["fastapi.middleware.cors"])


def _load_stub_module() -> ModuleType:
    """Load the shared FastAPI stub without assuming package initialisation."""

    module_name = "services.common.fastapi_stub"
    existing = sys.modules.get(module_name)
    if isinstance(existing, ModuleType):
        return existing

    try:
        return import_module(module_name)
    except ModuleNotFoundError:
        stub_path = Path(__file__).resolve().parent.parent / "services" / "common" / "fastapi_stub.py"
        spec = spec_from_file_location(module_name, stub_path)
        if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
            raise
        module = module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
        return module


def _ensure_stub() -> ModuleType:
    """Import and cache the FastAPI stub module on first use."""

    global _STUB, __all__
    if _STUB is None:
        stub = _load_stub_module()
        _STUB = stub
        exports = list(getattr(stub, "__all__", ()))
        __all__ = exports
        for name in exports:
            globals().setdefault(name, getattr(stub, name))
        _populate_submodules(stub)
    return _STUB


def __getattr__(name: str) -> Any:
    stub = _ensure_stub()
    try:
        value = getattr(stub, name)
    except AttributeError as exc:  # pragma: no cover - mirror FastAPI behaviour
        raise AttributeError(f"module 'fastapi' has no attribute '{name}'") from exc
    globals()[name] = value
    return value


def __dir__() -> list[str]:  # pragma: no cover - convenience for introspection
    exports = set(__all__)
    stub = _STUB or None
    if stub is not None:
        exports.update(dir(stub))
    exports.update(globals().keys())
    return sorted(exports)


# Pre-populate the stub for star imports while keeping attribute access lazy.
try:  # pragma: no cover - allow import-time failures when stub is missing
    _ensure_stub()
except Exception:
    # Leave lazy loading to handle the error path so dependency resolution can
    # surface a more descriptive exception later.
    pass

