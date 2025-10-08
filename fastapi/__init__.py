"""Repository-local FastAPI compatibility shim.

The real FastAPI dependency is optional in many of the services because the
unit tests focus on the business logic instead of the HTTP layer.  When
FastAPI is unavailable we fall back to the lightweight implementations provided
by :mod:`services.common.fastapi_stub` so modules can still be imported.
"""

from __future__ import annotations

from types import ModuleType
import importlib
import sys
from pathlib import Path


def _is_within_repo(path: Path, repo_root: Path) -> bool:
    """Return ``True`` if *path* is inside *repo_root*."""

    try:
        path.relative_to(repo_root)
    except ValueError:
        return False
    else:
        return True


def _load_real_fastapi() -> ModuleType | None:
    """Attempt to import the real FastAPI package if it exists."""

    repo_root = Path(__file__).resolve().parents[1]
    search_paths: list[str] = []
    for entry in sys.path:
        candidate = Path(entry) if entry else Path.cwd()
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        if _is_within_repo(resolved, repo_root):
            continue
        search_paths.append(entry)

    if not search_paths:
        return None

    current_module = sys.modules.get(__name__)
    original_sys_path = list(sys.path)

    try:
        if __name__ in sys.modules:
            sys.modules.pop(__name__)
        sys.path = search_paths
        return importlib.import_module(__name__)
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on environment
        if exc.name == __name__:
            return None
        raise
    finally:
        sys.path = original_sys_path
        if current_module is not None:
            sys.modules[__name__] = current_module


_real_fastapi = _load_real_fastapi()

if _real_fastapi is not None:
    sys.modules[__name__] = _real_fastapi
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
