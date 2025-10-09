"""Repository-local FastAPI compatibility shim.

The real FastAPI dependency is optional in many of the services because the
unit tests focus on the business logic instead of the HTTP layer.  When
FastAPI is unavailable we fall back to the lightweight implementations provided
by :mod:`services.common.fastapi_stub` so modules can still be imported.  When
FastAPI *is* installed we delegate to the genuine package to avoid overriding
the runtime implementation.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

from services.common import fastapi_stub as _stub


def _import_real_fastapi() -> ModuleType | None:
    """Import the real FastAPI package, falling back to ``None`` if unavailable."""

    repo_root = str(Path(__file__).resolve().parent.parent)
    original_sys_path = list(sys.path)
    try:
        sys.path = [
            entry
            for entry in sys.path
            if Path(entry).resolve() != Path(repo_root).resolve()
        ]
        return importlib.import_module("fastapi")
    except ModuleNotFoundError:
        return None
    finally:
        sys.path = original_sys_path


_real_fastapi = _import_real_fastapi()

if _real_fastapi is not None:
    module_globals = globals()
    module_globals.update({k: v for k, v in _real_fastapi.__dict__.items()})
    __all__ = list(getattr(_real_fastapi, "__all__", ()))
else:
    __all__ = list(getattr(_stub, "__all__", ()))

    for name in __all__:
        globals()[name] = getattr(_stub, name)

    # Also expose helper helpers referenced by dotted imports.
    status = getattr(_stub, "status", None)
    run_in_threadpool = getattr(_stub, "run_in_threadpool", None)

    # Ensure commonly imported FastAPI submodules resolve to the stub implementations.
    _submodules: dict[str, dict[str, Any]] = {
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
        "fastapi.middleware.cors": {
            "CORSMiddleware": getattr(_stub, "CORSMiddleware", None)
        },
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
