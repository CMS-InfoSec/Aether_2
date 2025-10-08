"""Repository-local FastAPI compatibility shim.

The real FastAPI dependency is optional in many of the services because the
unit tests focus on the business logic instead of the HTTP layer.  When
FastAPI is unavailable we fall back to the lightweight implementations provided
by :mod:`services.common.fastapi_stub` so modules can still be imported.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
_removed_paths: list[tuple[int, str]] = []

for index in reversed(range(len(sys.path))):
    candidate = sys.path[index]
    try:
        if Path(candidate).resolve() == _repo_root:
            _removed_paths.append((index, candidate))
            del sys.path[index]
    except (OSError, RuntimeError):
        # Paths that cannot be resolved (e.g. due to permissions) are ignored.
        continue

_placeholder_module = sys.modules.get(__name__)
_real_fastapi = None

try:
    if __name__ in sys.modules:
        del sys.modules[__name__]
    _real_fastapi = importlib.import_module(__name__)
except ImportError:
    _real_fastapi = None
finally:
    for index, path in reversed(_removed_paths):
        sys.path.insert(index, path)
    if _real_fastapi is None and _placeholder_module is not None:
        sys.modules[__name__] = _placeholder_module

if _real_fastapi is not None:
    module_globals = globals()
    module_globals.update(_real_fastapi.__dict__)
    module_globals["__all__"] = getattr(
        _real_fastapi,
        "__all__",
        [name for name in module_globals if not name.startswith("_")],
    )
    for helper_name in (
        "_real_fastapi",
        "_placeholder_module",
        "_removed_paths",
        "_repo_root",
        "module_globals",
    ):
        module_globals.pop(helper_name, None)
else:
    from types import ModuleType

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
        "fastapi.concurrency": {
            "run_in_threadpool": getattr(_stub, "run_in_threadpool", None)
        },
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
