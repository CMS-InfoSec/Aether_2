"""Repository-local FastAPI compatibility shim.

The real FastAPI dependency is optional in many of the services because the
unit tests focus on the business logic instead of the HTTP layer.  When
FastAPI is available we defer to the genuine package; otherwise we fall back to
the lightweight implementations provided by
:mod:`services.common.fastapi_stub` so modules can still be imported.
"""

from __future__ import annotations

import importlib
from importlib import metadata as _metadata
from pathlib import Path
from types import ModuleType
import sys
from typing import Optional


def _attempt_real_fastapi_import(current_module: ModuleType) -> Optional[ModuleType]:
    """Return the installed FastAPI module if it can be imported.

    The repository ships a stub ``fastapi`` package so the services keep
    importing even when the dependency is absent.  When FastAPI *is* installed
    we want to reuse the authentic implementation.  We temporarily prioritise
    the distribution's path on :data:`sys.path` while importing so Python does
    not immediately recurse back into this shim module.
    """

    try:
        distribution = _metadata.distribution("fastapi")
    except _metadata.PackageNotFoundError:
        return None

    distribution_path = distribution.locate_file("")
    if not distribution_path:
        return None

    real_path = str(Path(distribution_path))
    path_was_added = False
    if real_path not in sys.path:
        sys.path.insert(0, real_path)
        path_was_added = True

    try:
        # Remove the shim module during import so :func:`importlib.import_module`
        # does not hand us the partially initialised stub again.
        sys.modules.pop(__name__, None)
        real_fastapi = importlib.import_module(__name__)
    except Exception:
        # Restore the shim in case we need to fall back to it.
        sys.modules.setdefault(__name__, current_module)
        return None
    finally:
        if path_was_added:
            try:
                sys.path.remove(real_path)
            except ValueError:
                pass

    return real_fastapi


_current_module = sys.modules[__name__]
_real_fastapi = _attempt_real_fastapi_import(_current_module)

if _real_fastapi is not None:
    globals().update(_real_fastapi.__dict__)
else:
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
