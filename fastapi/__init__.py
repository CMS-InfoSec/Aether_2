"""Repository-local FastAPI compatibility shim.

The real FastAPI dependency is optional in many of the services because the
unit tests focus on the business logic instead of the HTTP layer.  When
FastAPI is unavailable we fall back to the lightweight implementations provided
by :mod:`services.common.fastapi_stub` so modules can still be imported.
"""

from __future__ import annotations

from importlib.machinery import PathFinder
import importlib.util
from pathlib import Path
from types import ModuleType
import sys


def _resolve_path(entry: str) -> Path | None:
    try:
        return Path(entry).resolve()
    except (OSError, RuntimeError, ValueError):
        return None


_this_module = sys.modules[__name__]
_repo_root = _resolve_path(Path(__file__).resolve().parents[1])
_search_roots = [
    entry
    for entry in sys.path
    if entry and (_resolved := _resolve_path(entry)) is not None and _resolved != _repo_root
]

_cleanup_targets = {
    "_resolve_path": _resolve_path,
    "_search_roots": None,  # placeholder, will be set after comprehension
    "_repo_root": _repo_root,
    "_this_module": _this_module,
}
_cleanup_targets["_search_roots"] = _search_roots

_real_fastapi = None
_spec = PathFinder.find_spec(__name__, _search_roots) if _search_roots else None
if _spec and _spec.loader:
    module = importlib.util.module_from_spec(_spec)
    try:
        sys.modules[__name__] = module
        _spec.loader.exec_module(module)
    except Exception:  # pragma: no cover - defensive fallback
        sys.modules[__name__] = _this_module
    else:
        _real_fastapi = module

if _real_fastapi is not None:
    globals().update(vars(_real_fastapi))
    __all__ = getattr(_real_fastapi, "__all__", [k for k in globals() if not k.startswith("_")])
    __doc__ = _real_fastapi.__doc__
    __file__ = getattr(_real_fastapi, "__file__", __file__)
    __loader__ = _real_fastapi.__loader__
    __package__ = _real_fastapi.__package__
    __path__ = getattr(_real_fastapi, "__path__", None)
    __spec__ = _real_fastapi.__spec__
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


for _name, _value in {
    **_cleanup_targets,
    "_real_fastapi": _real_fastapi,
    "_spec": _spec,
}.items():
    if globals().get(_name) is _value:
        del globals()[_name]

del _cleanup_targets
