"""Repository-local FastAPI compatibility shim.

The real FastAPI dependency is optional in many of the services because the
unit tests focus on the business logic instead of the HTTP layer.  When
FastAPI is unavailable we fall back to the lightweight implementations provided
by :mod:`services.common.fastapi_stub` so modules can still be imported.
"""

from __future__ import annotations

from importlib import util as _importlib_util
from importlib.machinery import PathFinder as _PathFinder
from pathlib import Path as _Path
from types import ModuleType
import sys


def _load_real_fastapi() -> ModuleType | None:
    """Attempt to import the real FastAPI package from outside the repo."""

    this_file = _Path(__file__).resolve()
    repo_root = this_file.parent.parent

    search_path: list[str] = []
    for entry in sys.path:
        if not isinstance(entry, str):
            # Keep non-string entries untouched – the finder will ignore them.
            search_path.append(entry)
            continue

        try:
            resolved = _Path(entry).resolve()
        except (OSError, RuntimeError, ValueError):
            search_path.append(entry)
            continue

        try:
            resolved.relative_to(repo_root)
        except ValueError:
            search_path.append(entry)

    spec = _PathFinder.find_spec("fastapi", search_path)
    if spec is None or spec.origin is None:
        return None

    origin = _Path(spec.origin).resolve()
    if origin == this_file:
        # The finder located this shim again; the real dependency is absent.
        return None

    loader = spec.loader
    if loader is None:
        return None

    module = _importlib_util.module_from_spec(spec)
    if spec.submodule_search_locations is not None:
        module.__path__ = list(spec.submodule_search_locations)

    loader.exec_module(module)
    return module


_real_fastapi = _load_real_fastapi()

if _real_fastapi is not None:
    # Delegate entirely to the actual FastAPI implementation.
    sys.modules[__name__] = _real_fastapi
    globals().update(vars(_real_fastapi))
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
