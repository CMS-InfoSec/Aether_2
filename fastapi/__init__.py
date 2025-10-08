"""Repository-local FastAPI compatibility shim.

The real FastAPI dependency is optional in many of the services because the
unit tests focus on the business logic instead of the HTTP layer.  When
FastAPI is unavailable we fall back to the lightweight implementations provided
by :mod:`services.common.fastapi_stub` so modules can still be imported.
"""

from __future__ import annotations

import importlib.machinery
import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _import_real_fastapi() -> ModuleType | None:
    """Try to load the real FastAPI package if it is installed.

    The repository ships with an in-house stub that mirrors a very small
    portion of FastAPI.  When the genuine dependency is available we want to
    forward all imports to it instead of the stub.  Because this compatibility
    shim shares the same import name we need to manually search *other*
    ``sys.path`` entries for an alternate package and load it from there.
    """

    package_root = Path(__file__).resolve().parent.parent
    stub_module = sys.modules.get(__name__)

    for entry in sys.path:
        try:
            entry_path = Path(entry).resolve()
        except (OSError, RuntimeError):
            continue
        if entry_path == package_root:
            continue

        spec = importlib.machinery.PathFinder.find_spec(__name__, [str(entry_path)])
        if spec is None or spec.loader is None:
            continue
        origin = getattr(spec, "origin", None)
        if origin and Path(origin).resolve() == Path(__file__).resolve():
            continue

        module = importlib.util.module_from_spec(spec)
        try:
            sys.modules[__name__] = module
            spec.loader.exec_module(module)
        except Exception:
            if stub_module is not None:
                sys.modules[__name__] = stub_module
            else:
                sys.modules.pop(__name__, None)
            raise
        else:
            globals().update(module.__dict__)
            globals()["__all__"] = list(getattr(module, "__all__", ()))
            globals()["__path__"] = getattr(module, "__path__", [])
            globals()["__file__"] = getattr(module, "__file__", None)
            return module

    if stub_module is not None:
        sys.modules[__name__] = stub_module
    return None


_real_fastapi = _import_real_fastapi()

if _real_fastapi is None:
    from services.common import fastapi_stub as _stub

    __all__ = list(getattr(_stub, "__all__", ()))

    for name in __all__:
        globals()[name] = getattr(_stub, name)

    # Also expose helper helpers referenced by dotted imports.
    status = getattr(_stub, "status", None)
    run_in_threadpool = getattr(_stub, "run_in_threadpool", None)

    # Ensure commonly imported FastAPI submodules resolve to the stub implementations.
    _submodules = {
        "fastapi.testclient": {"TestClient": getattr(_stub, "TestClient", None)},
        "fastapi.responses": {
            "Response": getattr(_stub, "Response", None),
            "JSONResponse": getattr(_stub, "JSONResponse", None),
            "HTMLResponse": getattr(_stub, "HTMLResponse", None),
            "StreamingResponse": getattr(_stub, "StreamingResponse", None),
        },
        "fastapi.exception_handlers": {
            "request_validation_exception_handler": getattr(
                _stub,
                "request_validation_exception_handler",
                None,
            )
        },
        "fastapi.applications": {"FastAPI": getattr(_stub, "FastAPI", None)},
        "fastapi.routing": {"APIRouter": getattr(_stub, "APIRouter", None)},
        "fastapi.exceptions": {
            "HTTPException": getattr(_stub, "HTTPException", None),
            "RequestValidationError": getattr(_stub, "RequestValidationError", None),
        },
        "fastapi.encoders": {"jsonable_encoder": getattr(_stub, "jsonable_encoder", None)},
        "fastapi.concurrency": {"run_in_threadpool": getattr(_stub, "run_in_threadpool", None)},
        "fastapi.middleware": {},
        "fastapi.middleware.cors": {"CORSMiddleware": getattr(_stub, "CORSMiddleware", None)},
    }

    for module_name, attributes in _submodules.items():
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
