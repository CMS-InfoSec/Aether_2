"""Repository-local FastAPI compatibility shim.

The real FastAPI dependency is optional in many of the services because the
unit tests focus on the business logic instead of the HTTP layer.  When
FastAPI is unavailable we fall back to the lightweight implementations provided
by :mod:`services.common.fastapi_stub` so modules can still be imported.
"""

from __future__ import annotations

from types import ModuleType
import sys

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
        "JSONResponse": getattr(_stub, "JSONResponse", None),
        "StreamingResponse": getattr(_stub, "StreamingResponse", None),
    },
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
