"""Lightweight FastAPI stand-ins used when the dependency is unavailable.

This module provides minimal shims that allow services to be imported in test
environments where ``fastapi`` is not installed or has been replaced with a
partial stub.  The goal is to avoid ``ImportError`` during module import while
keeping behaviour simple and explicit for unit tests that only need to execute
the business logic.

The implementations intentionally cover only the small surface area exercised
in the test-suite.  They should be good enough to register routes or raise
``HTTPException`` instances but they do not attempt to emulate FastAPI's
runtime behaviour.
"""

from __future__ import annotations

import base64
import json
import os
import uuid
from datetime import date, datetime, timezone
from collections.abc import MutableMapping
from contextlib import ExitStack, asynccontextmanager
from dataclasses import dataclass
from inspect import Parameter, Signature, isclass, iscoroutine, isgenerator, signature
from types import ModuleType, SimpleNamespace
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

try:  # pragma: no cover - prefer the real FastAPI implementation when available
    import importlib.util
    import sys
except Exception:  # pragma: no cover - extremely defensive
    importlib = None  # type: ignore[assignment]
    sys = None  # type: ignore[assignment]

try:  # pragma: no cover - metrics may be unavailable in minimal environments
    import metrics as _metrics_module  # type: ignore[import]
except Exception:  # pragma: no cover - keep the stub usable without metrics
    _metrics_module = None

try:  # pragma: no cover - pydantic may be unavailable in some environments
    from pydantic import ValidationError as PydanticValidationError  # type: ignore
except Exception:  # pragma: no cover - fallback when pydantic is absent
    PydanticValidationError = None  # type: ignore[assignment]

try:  # pragma: no cover - pydantic-core may be unavailable independently of pydantic
    from pydantic_core import ValidationError as CoreValidationError  # type: ignore
except Exception:  # pragma: no cover - fallback when pydantic-core is absent
    CoreValidationError = None  # type: ignore[assignment]


__all__ = [
    "APIRouter",
    "BackgroundTasks",
    "Body",
    "Cookie",
    "Depends",
    "FastAPI",
    "File",
    "Form",
    "Header",
    "HTTPException",
    "HTMLResponse",
    "JSONResponse",
    "Path",
    "Query",
    "Request",
    "Response",
    "StreamingResponse",
    "UploadFile",
    "status",
    "request_validation_exception_handler",
]


class HTTPException(Exception):
    """Simplified ``HTTPException`` carrying an HTTP status code."""

    def __init__(self, status_code: int, detail: Any | None = None) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class BackgroundTasks:
    """Records background tasks and executes them synchronously when desired."""

    def __init__(self) -> None:
        self.tasks: List[tuple[Callable[..., Any], tuple[Any, ...], Dict[str, Any]]] = []

    def add_task(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        self.tasks.append((func, args, kwargs))

    def run(self) -> None:
        for func, args, kwargs in list(self.tasks):
            func(*args, **kwargs)


@dataclass(frozen=True)
class _Dependency:
    dependency: Callable[..., Any] | None


@dataclass(frozen=True)
class _ParameterMarker:
    default: Any
    alias: Optional[str] = None

    def resolve(
        self,
        request: "Request",
        query_params: Dict[str, Any],
        path_params: Dict[str, Any],
        name: str,
        body: Any,
    ) -> Any:
        return self.default


class _HeaderParameter(_ParameterMarker):
    def resolve(
        self,
        request: "Request",
        query_params: Dict[str, Any],
        path_params: Dict[str, Any],
        name: str,
        body: Any,
    ) -> Any:
        header_name = (self.alias or name).lower()
        return request.headers.get(header_name, self.default)


class _QueryParameter(_ParameterMarker):
    def resolve(
        self,
        request: "Request",
        query_params: Dict[str, Any],
        path_params: Dict[str, Any],
        name: str,
        body: Any,
    ) -> Any:
        key = self.alias or name
        return query_params.get(key, self.default)


class _PathParameter(_ParameterMarker):
    def resolve(
        self,
        request: "Request",
        query_params: Dict[str, Any],
        path_params: Dict[str, Any],
        name: str,
        body: Any,
    ) -> Any:
        key = self.alias or name
        return path_params.get(key, self.default)


class _BodyParameter(_ParameterMarker):
    def resolve(
        self,
        request: "Request",
        query_params: Dict[str, Any],
        path_params: Dict[str, Any],
        name: str,
        body: Any,
    ) -> Any:
        return body


def _coerce_value(annotation: Any, value: Any) -> Any:
    """Best-effort conversion of query/path parameters to annotated types."""

    if value is None:
        return None

    if annotation in (Signature.empty, Parameter.empty, Any):
        return value

    origin = get_origin(annotation)
    if origin is Union:
        candidates = [arg for arg in get_args(annotation) if arg is not type(None)]
        if not candidates:
            return value
        for candidate in candidates:
            coerced = _coerce_value(candidate, value)
            if coerced is not None:
                return coerced
        return value

    if annotation is bool and isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
        return value

    if annotation in {int, float} and isinstance(value, str):
        try:
            return annotation(value)
        except (TypeError, ValueError):
            return value

    if annotation is date and isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return value

    if annotation is datetime and isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value

    return value


def _allow_admin_autoload() -> bool:
    """Return ``True`` when widening the admin set has been explicitly enabled."""

    return os.getenv("AETHER_ALLOW_INSECURE_TEST_DEFAULTS") == "1"


def Depends(dependency: Callable[..., Any] | None) -> _Dependency:
    return _Dependency(dependency)


def Body(default: Any = None, *_: Any, **__: Any) -> _ParameterMarker:
    return _BodyParameter(default)


def Query(default: Any = None, *_: Any, alias: Optional[str] = None, **__: Any) -> _QueryParameter:
    return _QueryParameter(default, alias=alias)


def Path(default: Any = None, *_: Any, alias: Optional[str] = None, **__: Any) -> _PathParameter:
    return _PathParameter(default, alias=alias)


def Header(default: Any = None, *_: Any, alias: Optional[str] = None, **__: Any) -> _HeaderParameter:
    return _HeaderParameter(default, alias=alias)


def Cookie(default: Any = None, *_: Any, alias: Optional[str] = None, **__: Any) -> _ParameterMarker:
    return _ParameterMarker(default, alias=alias)


def File(default: Any = None, *_: Any, **__: Any) -> _ParameterMarker:
    return _ParameterMarker(default)


def Form(default: Any = None, *_: Any, **__: Any) -> _ParameterMarker:
    return _ParameterMarker(default)


class UploadFile:
    """Extremely small ``UploadFile`` placeholder."""

    def __init__(self, filename: str | None = None, file: Any | None = None) -> None:
        self.filename = filename or ""
        self.file = file


class _HeaderMapping(MutableMapping[str, Any]):
    """Case-insensitive header mapping mimicking Starlette's behaviour."""

    def __init__(self, initial: Optional[Dict[str, Any]] = None) -> None:
        self._store: Dict[str, Any] = {}
        if initial:
            for key, value in initial.items():
                self[key] = value

    def __getitem__(self, key: str) -> Any:
        return self._store[key.lower()]

    def __setitem__(self, key: str, value: Any) -> None:
        self._store[key.lower()] = value

    def __delitem__(self, key: str) -> None:
        del self._store[key.lower()]

    def __iter__(self) -> Iterator[str]:
        return iter(self._store)

    def __len__(self) -> int:
        return len(self._store)

    def __contains__(self, key: object) -> bool:  # pragma: no cover - defensive
        if isinstance(key, str):
            return key.lower() in self._store
        return False

    def get(self, key: str, default: Any = None) -> Any:
        return self._store.get(key.lower(), default)

    def items(self):  # pragma: no cover - passthrough for debug helpers
        return self._store.items()

    def keys(self):  # pragma: no cover - passthrough for debug helpers
        return self._store.keys()

    def values(self):  # pragma: no cover - passthrough for debug helpers
        return self._store.values()

    def copy(self) -> Dict[str, Any]:  # pragma: no cover - compatibility helper
        return dict(self._store)


class Request:
    """Bare-minimum representation of a request object."""

    def __init__(
        self,
        data: Optional[Dict[str, Any]] = None,
        receive: Any | None = None,
        **kwargs: Any,
    ) -> None:
        headers_override = kwargs.pop("headers", None)
        scope = data or {}
        headers_map: Dict[str, Any] = {}
        if headers_override is not None:
            if isinstance(headers_override, Mapping):
                headers_source = headers_override.items()
            else:
                headers_source = headers_override
        elif isinstance(scope, dict) and "headers" in scope:
            headers_source = scope.get("headers", []) or []
        else:
            headers_source = []

        for key, value in headers_source:
            if isinstance(key, bytes):
                key = key.decode("latin-1")
            if isinstance(value, bytes):
                value = value.decode("latin-1")
            headers_map[str(key)] = value

        self.headers = _HeaderMapping(headers_map)
        app = scope.get("app") if isinstance(scope, dict) else None
        self.app = app
        self.state = getattr(app, "state", SimpleNamespace())
        self.query_params: Dict[str, Any] = scope.get("query_params", {}) if isinstance(scope, dict) else {}
        self.path_params: Dict[str, Any] = scope.get("path_params", {}) if isinstance(scope, dict) else {}
        self.client = SimpleNamespace(host=None, port=None)
        self._scope = scope
        self._receive = receive
        self._body: Any = None

    def set_body(self, body: Any) -> None:
        self._body = body

    def json(self) -> Any:
        return self._body


def _bind_request_id(request: "Request") -> tuple[Optional[object], Optional[str]]:
    """Mirror ``RequestTracingMiddleware`` in environments using the stub."""

    global _metrics_module
    if _metrics_module is None:
        try:  # pragma: no cover - lazy import for test environments
            import metrics as _metrics_module  # type: ignore[import, assignment]
        except Exception:
            return None, None
    if _metrics_module is None or not hasattr(_metrics_module, "_REQUEST_ID"):
        return None, None

    headers = getattr(request, "headers", {}) or {}
    request_id: Optional[str] = None
    if isinstance(headers, Mapping):
        request_id = headers.get("X-Request-ID") or headers.get("x-request-id")

    if not request_id:
        request_id = str(uuid.uuid4())

    token = _metrics_module._REQUEST_ID.set(request_id)
    return token, request_id


def _reset_request_id(token: Optional[object]) -> None:
    if _metrics_module is None or not hasattr(_metrics_module, "_REQUEST_ID"):
        return
    if token is None:
        return
    try:
        _metrics_module._REQUEST_ID.reset(token)
    except Exception:  # pragma: no cover - mismatched token provided
        pass


class Response:
    """Simple container for response data."""

    def __init__(
        self,
        content: Any = None,
        *,
        status_code: int = 200,
        media_type: str | None = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.content = content
        self.status_code = status_code
        self.media_type = media_type
        self.headers = headers or {}


class StreamingResponse(Response):
    """Wrapper mirroring FastAPI's ``StreamingResponse`` signature."""

    def __init__(
        self,
        content: Iterable[Any] | Iterator[Any],
        *,
        media_type: str = "application/octet-stream",
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(content, status_code=200, media_type=media_type, headers=headers)
        self.body_iterator = content


class JSONResponse(Response):
    """JSON response placeholder that stores the provided content."""

    def __init__(
        self,
        content: Any,
        *,
        status_code: int = 200,
        media_type: str = "application/json",
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(content, status_code=status_code, media_type=media_type, headers=headers)


class HTMLResponse(Response):
    """HTML response placeholder matching FastAPI's response helpers."""

    def __init__(
        self,
        content: Any,
        *,
        status_code: int = 200,
        media_type: str = "text/html",
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(content, status_code=status_code, media_type=media_type, headers=headers)


def run_in_threadpool(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Synchronous stand-in for FastAPI's ``run_in_threadpool`` helper."""

    return func(*args, **kwargs)


class CORSMiddleware:
    """No-op middleware used to satisfy import-time dependencies in tests."""

    def __init__(self, app: Any, **options: Any) -> None:
        self.app = app
        self.options = options


class APIRouter:
    """Minimal router recording registered routes for inspection in tests."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.routes: List[Tuple[str, Tuple[str, ...], Callable[..., Any]]] = []
        self.prefix = kwargs.get("prefix", "")
        self.tags = list(kwargs.get("tags", []))
        self.event_handlers: Dict[str, List[Callable[..., Any]]] = {}

    def add_api_route(
        self,
        path: str,
        endpoint: Callable[..., Any],
        *,
        methods: Optional[Iterable[str]] = None,
    ) -> None:
        verbs = tuple(methods) if methods is not None else ("GET",)
        self.routes.append((path, tuple(verb.upper() for verb in verbs), endpoint))

    def route(self, path: str, *, methods: Iterable[str]) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.add_api_route(path, func, methods=methods)
            return func

        return decorator

    def get(self, path: str, **_: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.route(path, methods=("GET",))

    def post(self, path: str, **_: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.route(path, methods=("POST",))

    def delete(self, path: str, **_: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.route(path, methods=("DELETE",))

    def put(self, path: str, **_: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self.route(path, methods=("PUT",))

    def on_event(self, event_type: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.event_handlers.setdefault(event_type, []).append(func)
            return func

        return decorator


class FastAPI:
    """Tiny FastAPI replacement that collects router registrations."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.state = SimpleNamespace()
        self.routes: List["_Route"] = []
        self.user_middleware: List[Any] = []
        self.dependency_overrides: Dict[Any, Callable[..., Any]] = {}
        lifespan = kwargs.get("lifespan")
        if lifespan is None:

            @asynccontextmanager
            async def _default_lifespan(_: "FastAPI"):
                yield

            lifespan = _default_lifespan

        def _lifespan_context(app: "FastAPI"):
            return lifespan(app)

        self.router = SimpleNamespace(lifespan_context=_lifespan_context)
        self.title = kwargs.get("title")
        self.event_handlers: Dict[str, List[Callable[..., Any]]] = {}

    def include_router(self, router: APIRouter, *args: Any, **kwargs: Any) -> None:
        prefix = kwargs.get("prefix", "")
        combined_prefix = _join_path(prefix, router.prefix)
        for path, methods, endpoint in router.routes:
            full_path = _join_path(combined_prefix, path)
            self._register_route(full_path, endpoint, methods)
        for event, handlers in getattr(router, "event_handlers", {}).items():
            existing = self.event_handlers.setdefault(event, [])
            existing.extend(handlers)

    def openapi(self) -> Dict[str, Any]:  # pragma: no cover - exercised via tooling
        """Generate a minimal OpenAPI structure for tooling that expects FastAPI."""

        paths: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for route in self.routes:
            path_item = paths.setdefault(route.path, {})
            for method in route.methods:
                operation_id = getattr(route.endpoint, "__name__", "handler")
                path_item[method.lower()] = {
                    "operationId": f"{operation_id}_{method.lower()}",
                    "responses": {
                        "200": {"description": "Successful Response"}
                    },
                }

        return {
            "openapi": "3.1.0",
            "info": {
                "title": self.title or "FastAPI",
                "version": "stub",
            },
            "paths": paths,
        }

    def add_middleware(self, middleware_cls: Any, **kwargs: Any) -> None:
        self.user_middleware.append(SimpleNamespace(cls=middleware_cls, kwargs=kwargs))

    def _register_route(self, path: str, endpoint: Callable[..., Any], methods: Iterable[str]) -> None:
        route = _Route(path=_normalize_path(path), methods=tuple(methods), endpoint=endpoint)
        self.routes.append(route)

    def add_event_handler(self, event_type: str, handler: Callable[..., Any]) -> None:
        self.event_handlers.setdefault(event_type, []).append(handler)

    def get(self, path: str, **_: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self._register_route(path, func, ("GET",))
            return func

        return decorator

    def post(self, path: str, **_: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self._register_route(path, func, ("POST",))
            return func

        return decorator

    def delete(self, path: str, **_: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self._register_route(path, func, ("DELETE",))
            return func

        return decorator

    def put(self, path: str, **_: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self._register_route(path, func, ("PUT",))
            return func

        return decorator

    def on_event(self, event_type: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.add_event_handler(event_type, func)
            return func

        return decorator

    def middleware(self, *_: Any, **__: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            return func

        return decorator

    def exception_handler(self, *_: Any, **__: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            return func

        return decorator


async def request_validation_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Return a JSON response mirroring FastAPI's validation error payload."""

    if isinstance(exc, RequestValidationError):
        errors = exc.errors
    elif hasattr(exc, "errors"):
        try:
            errors = exc.errors()  # type: ignore[call-arg]
        except Exception:  # pragma: no cover - defensive fallback
            errors = getattr(exc, "errors", [])  # type: ignore[attr-defined]
    else:
        errors = [{"msg": str(exc)}]

    return JSONResponse(
        {"detail": list(errors)},
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    )


_STATUS_CODES = {
    "HTTP_200_OK": 200,
    "HTTP_201_CREATED": 201,
    "HTTP_202_ACCEPTED": 202,
    "HTTP_204_NO_CONTENT": 204,
    "HTTP_400_BAD_REQUEST": 400,
    "HTTP_401_UNAUTHORIZED": 401,
    "HTTP_403_FORBIDDEN": 403,
    "HTTP_404_NOT_FOUND": 404,
    "HTTP_409_CONFLICT": 409,
    "HTTP_412_PRECONDITION_FAILED": 412,
    "HTTP_422_UNPROCESSABLE_ENTITY": 422,
    "HTTP_422_UNPROCESSABLE_CONTENT": 422,
    "HTTP_429_TOO_MANY_REQUESTS": 429,
    "HTTP_500_INTERNAL_SERVER_ERROR": 500,
    "HTTP_502_BAD_GATEWAY": 502,
    "HTTP_503_SERVICE_UNAVAILABLE": 503,
}

status = ModuleType("fastapi.status")
for _name, _code in _STATUS_CODES.items():
    setattr(status, _name, _code)


def jsonable_encoder(value: Any, *args: Any, **kwargs: Any) -> Any:
    """Fallback encoder mirroring FastAPI's helper behaviour."""

    return value


@dataclass
class RequestValidationError(Exception):
    """Placeholder error raised when validation fails under the stub."""

    errors: List[Dict[str, Any]]


class _Route:
    """Internal representation of a registered route."""

    def __init__(self, path: str, methods: Tuple[str, ...], endpoint: Callable[..., Any]) -> None:
        self.path = path
        self.methods = methods
        self.endpoint = endpoint
        self._segments = _split_path(path)

    def matches(self, method: str, url_path: str) -> bool:
        if method.upper() not in self.methods:
            return False
        incoming = _split_path(_normalize_path(url_path))
        if len(incoming) != len(self._segments):
            return False
        for expected, actual in zip(self._segments, incoming):
            if expected.startswith("{") and expected.endswith("}"):
                continue
            if expected != actual:
                return False
        return True

    def extract_path_params(self, url_path: str) -> Dict[str, str]:
        incoming = _split_path(_normalize_path(url_path))
        params: Dict[str, str] = {}
        for expected, actual in zip(self._segments, incoming):
            if expected.startswith("{") and expected.endswith("}"):
                params[expected[1:-1]] = actual
        return params


def _split_path(path: str) -> Tuple[str, ...]:
    if path == "/":
        return ("",)
    trimmed = path.strip("/")
    if not trimmed:
        return ("",)
    return tuple(segment for segment in trimmed.split("/") if segment)


def _normalize_path(path: str) -> str:
    if not path:
        return "/"
    if not path.startswith("/"):
        path = "/" + path
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    return path or "/"


def _join_path(prefix: str, path: str) -> str:
    combined = "/".join(part.strip("/") for part in (prefix, path) if part)
    return "/" + combined if combined else "/"


def _decode_b64url(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _extract_jwt_subject(token: str) -> Optional[str]:
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        payload_bytes = _decode_b64url(parts[1])
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception:  # pragma: no cover - invalid tokens are ignored
        return None
    subject = payload.get("sub")
    if isinstance(subject, str):
        subject = subject.strip()
        if subject:
            return subject
    return None


def _is_pydantic_model(annotation: Any) -> bool:
    try:
        from pydantic import BaseModel  # type: ignore

        return isclass(annotation) and issubclass(annotation, BaseModel)
    except Exception:  # pragma: no cover - pydantic is optional
        return False


def _is_validation_error(exc: Exception) -> bool:
    return (
        (PydanticValidationError is not None and isinstance(exc, PydanticValidationError))
        or (CoreValidationError is not None and isinstance(exc, CoreValidationError))
    )


def _extract_validation_errors(exc: Exception) -> List[Dict[str, Any]]:
    if hasattr(exc, "errors"):
        try:
            errors = exc.errors()  # type: ignore[call-arg]
        except Exception:  # pragma: no cover - defensive fallback
            errors = [{"msg": str(exc)}]
    else:
        errors = [{"msg": str(exc)}]
    return list(errors)


def _to_json_compatible(value: Any) -> Any:
    if isinstance(value, datetime):
        candidate = value.astimezone(timezone.utc)
        return candidate.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "model_dump"):
        return _to_json_compatible(value.model_dump())  # type: ignore[arg-type]
    if isinstance(value, dict):
        return {key: _to_json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_json_compatible(item) for item in value]
    return value


def _dump_response_payload(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        try:
            dumped = value.model_dump(exclude_none=True)  # type: ignore[call-arg]
        except TypeError:
            dumped = value.model_dump()
        return _to_json_compatible(dumped)
    if hasattr(value, "dict"):
        try:
            return _to_json_compatible(value.dict())
        except TypeError:
            return _to_json_compatible(value)
    return _to_json_compatible(value)


def _is_mapping_annotation(annotation: Any) -> bool:
    try:
        from typing import get_origin
    except Exception:  # pragma: no cover - Python <3.8 compatibility safeguard
        return annotation in (dict, Dict, Mapping)

    origin = get_origin(annotation)
    if origin is None:
        return annotation in (dict, Dict, Mapping)
    return origin in (dict, Dict, Mapping)


async def _call_endpoint(
    app: FastAPI,
    func: Callable[..., Any],
    *,
    request: Request,
    body: Any,
    query_params: Dict[str, Any],
    path_params: Dict[str, Any],
    dependency_stack: Optional[ExitStack] = None,
) -> Any:
    stack = dependency_stack or ExitStack()
    owns_stack = dependency_stack is None

    resolved_kwargs: Dict[str, Any] = {}
    func_sig: Signature = signature(func)
    try:
        from typing import get_type_hints  # type: ignore

        resolved_hints = get_type_hints(func, include_extras=True)
    except Exception:  # pragma: no cover - typing helpers may fail for local functions
        resolved_hints = {}

    body_is_mapping = isinstance(body, dict)
    body_mapping = body if body_is_mapping else {}
    body_consumed = False

    for name, param in func_sig.parameters.items():
        annotation = resolved_hints.get(name, param.annotation)
        default = param.default

        if annotation is Request:
            resolved_kwargs[name] = request
            continue

        if isinstance(default, _Dependency):
            dependency = default.dependency
            if dependency is None:
                resolved_kwargs[name] = None
            else:
                override = app.dependency_overrides.get(dependency, dependency)
                value = await _call_endpoint(
                    app,
                    override,
                    request=request,
                    body=body,
                    query_params=query_params,
                    path_params=path_params,
                    dependency_stack=stack,
                )
                resolved_kwargs[name] = value
            continue

        if isinstance(default, _ParameterMarker):
            value = default.resolve(request, query_params, path_params, name, body)
            resolved_kwargs[name] = _coerce_value(annotation, value)
            continue

        if _is_pydantic_model(annotation):
            model_cls: Type[Any] = annotation
            candidate = body_mapping.get(name, body)
            try:
                if isinstance(candidate, model_cls):
                    resolved_kwargs[name] = candidate
                elif isinstance(candidate, dict):
                    resolved_kwargs[name] = model_cls(**candidate)
                elif body_is_mapping and body_mapping:
                    resolved_kwargs[name] = model_cls(**body_mapping)
                else:
                    resolved_kwargs[name] = model_cls(candidate)
            except Exception as exc:  # pragma: no cover - validation path
                if _is_validation_error(exc):
                    raise RequestValidationError(errors=_extract_validation_errors(exc)) from exc
                raise
            continue

        if name in path_params:
            resolved_kwargs[name] = _coerce_value(annotation, path_params[name])
            continue

        if name in query_params:
            resolved_kwargs[name] = _coerce_value(annotation, query_params[name])
            continue

        if name in body_mapping:
            resolved_kwargs[name] = body_mapping[name]
            continue

        if _is_mapping_annotation(annotation):
            if body_is_mapping:
                resolved_kwargs[name] = dict(body_mapping)
            else:
                resolved_kwargs[name] = {}
            continue

        if default is not Parameter.empty:
            resolved_kwargs[name] = default
            continue

        if not body_consumed and not body_is_mapping:
            resolved_kwargs[name] = body
            body_consumed = True
            continue

        resolved_kwargs[name] = None

    if "request" in func_sig.parameters and resolved_kwargs.get("request") is None:
        resolved_kwargs["request"] = request or Request()
    try:
        result = func(**resolved_kwargs)
        if isgenerator(result):
            try:
                yielded = next(result)
            except StopIteration as exc:  # pragma: no cover - generator returned immediately
                result = exc.value
            else:
                stack.callback(result.close)
                result = yielded
        if iscoroutine(result):
            result = await result
        return result
    finally:
        if owns_stack:
            stack.close()


class _ClientResponse:
    def __init__(
        self,
        *,
        status_code: int,
        payload: Any,
        headers: Optional[Dict[str, str]] = None,
        media_type: str | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = dict(headers or {})
        self.media_type = media_type
        if isinstance(payload, (bytes, bytearray)):
            try:
                self.text = payload.decode()
            except Exception:  # pragma: no cover - defensive fallback
                self.text = ""
        elif isinstance(payload, str):
            self.text = payload
        else:
            self.text = ""

    def json(self) -> Any:
        if isinstance(self._payload, (bytes, bytearray)):
            try:
                return json.loads(self._payload.decode())
            except Exception as exc:  # pragma: no cover - defensive fallback
                raise ValueError("Response payload is not valid JSON") from exc
        return self._payload

    def raise_for_status(self) -> None:
        if 400 <= int(self.status_code):
            raise RuntimeError(f"HTTP {self.status_code}: {self.text or self._payload}")


class TestClient:
    """Extremely small synchronous facade over the stub FastAPI application."""

    def __init__(self, app: FastAPI) -> None:
        self.app = app
        self._lifespan_cm = None
        self._entered = False
        self._event_handlers = self._resolve_event_handlers()
        self._ensure_started()

    def _ensure_started(self) -> None:
        if self._entered:
            return
        router = getattr(self.app, "router", None)
        lifespan_factory = getattr(router, "lifespan_context", None)
        if lifespan_factory is not None:
            self._lifespan_cm = lifespan_factory(self.app)
            enter = getattr(self._lifespan_cm, "__aenter__", None)
            if enter is not None:
                _run_async(lambda: enter())
        for handler in self._event_handlers.get("startup", []):
            _run_async(lambda: handler())
        self._entered = True

    def _shutdown(self, exc_type=None, exc=None, tb=None) -> None:
        if not self._entered:
            return
        for handler in reversed(self._event_handlers.get("shutdown", [])):
            _run_async(lambda: handler())
        if self._lifespan_cm is not None:
            exit_ = getattr(self._lifespan_cm, "__aexit__", None)
            if exit_ is not None:
                _run_async(lambda: exit_(exc_type, exc, tb))
        self._lifespan_cm = None
        self._entered = False

    def _resolve_event_handlers(self) -> Dict[str, List[Callable[..., Any]]]:
        """Return the app's event handlers while tolerating bare stubs.

        Several test suites monkeypatch ``fastapi.FastAPI`` with extremely
        small placeholders that only expose ``router`` and ``state``.  Accessing
        ``event_handlers`` on those objects raised ``AttributeError`` which in
        turn broke every consumer of the compatibility shim.  We coerce any
        missing attribute into an empty ``dict`` so lifecycle hooks remain
        optional just like they are in the real FastAPI test client.
        """

        handlers = getattr(self.app, "event_handlers", None)
        if isinstance(handlers, MutableMapping):
            return {str(key): list(value) for key, value in handlers.items()}
        return {}

    def __enter__(self) -> "TestClient":  # pragma: no cover - simple context protocol
        self._ensure_started()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # pragma: no cover - simple context protocol
        self._shutdown(exc_type, exc, tb)
        return False

    def close(self) -> None:
        """Mirror the real TestClient API by exposing an explicit close hook."""
        self._shutdown()

    def _build_request(
        self,
        *,
        headers: Optional[Dict[str, str]],
        params: Optional[Dict[str, Any]],
        path_params: Dict[str, Any],
    ) -> Request:
        request = Request(headers=headers or {})
        request.app = self.app
        request.query_params = dict(params or {})
        request.path_params = dict(path_params)
        return request

    def _find_route(self, method: str, path: str) -> Tuple[_Route, Dict[str, str]]:
        for route in self.app.routes:
            if route.matches(method, path):
                return route, route.extract_path_params(path)
        raise HTTPException(status_code=404, detail=f"No route for {method} {path}")

    def _prepare_body(self, json: Optional[Any]) -> Any:
        if json is None:
            return {}
        return json

    def _handle_call(
        self,
        method: str,
        path: str,
        *,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> _ClientResponse:
        self._ensure_started()
        route, path_params = self._find_route(method, path)
        body = self._prepare_body(json)
        request = self._build_request(headers=headers, params=params, path_params=path_params)
        request.url = SimpleNamespace(path=_normalize_path(path))
        request.method = method
        if hasattr(request, "set_body"):
            request.set_body(body)
        else:  # pragma: no cover - compatibility guard for patched requests
            setattr(request, "_body", body)

        token, request_id = _bind_request_id(request)
        try:
            payload = _run_async(
                lambda: _call_endpoint(
                    self.app,
                    route.endpoint,
                    request=request,
                    body=body,
                    query_params=request.query_params,
                    path_params=path_params,
                )
            )
            status_code = getattr(payload, "status_code", status.HTTP_200_OK)
            if isinstance(payload, Response):
                content = payload.content
                headers = dict(payload.headers)
                if payload.media_type and "content-type" not in {
                    key.lower() for key in headers.keys()
                }:
                    headers["content-type"] = payload.media_type
                if request_id and "x-request-id" not in {
                    key.lower() for key in headers.keys()
                }:
                    headers["x-request-id"] = request_id
                response = _ClientResponse(
                    status_code=status_code,
                    payload=content,
                    headers=headers,
                    media_type=payload.media_type,
                )
            else:
                headers = {"content-type": "application/json"}
                if request_id:
                    headers.setdefault("x-request-id", request_id)
                response = _ClientResponse(
                    status_code=status_code,
                    payload=_dump_response_payload(payload),
                    headers=headers,
                    media_type="application/json",
                )
        except RequestValidationError as exc:
            headers = {"content-type": "application/json"}
            if request_id:
                headers.setdefault("x-request-id", request_id)
            response = _ClientResponse(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                payload={"detail": exc.errors},
                headers=headers,
                media_type="application/json",
            )
        except HTTPException as exc:
            detail = exc.detail if exc.detail is not None else ""
            headers = {"content-type": "application/json"}
            if request_id:
                headers.setdefault("x-request-id", request_id)
            response = _ClientResponse(
                status_code=exc.status_code,
                payload={"detail": detail},
                headers=headers,
                media_type="application/json",
            )
        finally:
            _reset_request_id(token)

        return response

    def get(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> _ClientResponse:
        return self.request("GET", path, params=params, headers=headers)

    def post(
        self,
        path: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> _ClientResponse:
        return self.request("POST", path, json=json, headers=headers, params=params)

    def delete(
        self,
        path: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> _ClientResponse:
        return self.request("DELETE", path, json=json, headers=headers)

    def put(
        self,
        path: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> _ClientResponse:
        return self.request("PUT", path, json=json, headers=headers)

    def request(
        self,
        method: str,
        url: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> _ClientResponse:
        headers = dict(headers or {})
        normalized_method = method.upper()
        security_module = None
        try:  # pragma: no cover - the security module may be unavailable in thin tests
            from services.common import security as security_module  # type: ignore[assignment]
        except Exception:  # pragma: no cover - dependency not present
            security_module = None

        store = getattr(self.app.state, "session_store", None)
        override_present = False
        if security_module is not None:
            dependency = getattr(security_module, "require_admin_account", None)
            if dependency in self.app.dependency_overrides:
                override_present = True
        if not override_present:
            if security_module is not None:
                default_store = getattr(security_module, "_DEFAULT_SESSION_STORE", None)
                if store is None or (default_store is not None and store is not default_store):
                    store = default_store
                    if store is not None:
                        self.app.state.session_store = store
            if store is None:
                try:
                    from auth.service import InMemorySessionStore  # type: ignore[import]
                except Exception:  # pragma: no cover - auth service is optional in some suites
                    store = None
                else:
                    store = InMemorySessionStore()
                    self.app.state.session_store = store
                    if security_module is not None:
                        try:
                            security_module.set_default_session_store(store)
                        except Exception:  # pragma: no cover - defensive guard
                            pass
        account_id = headers.get("X-Account-ID")
        if account_id is None:
            for key, value in headers.items():
                if isinstance(key, str) and key.lower() == "x-account-id":
                    account_id = value
                    break
        if account_id is None and isinstance(json, dict):
            account_id = json.get("account_id")
        if account_id is None and isinstance(params, dict):
            account_id = params.get("account_id")
        provided_auth = headers.get("Authorization") or headers.get("authorization")
        token = None
        if isinstance(provided_auth, str) and provided_auth.lower().startswith("bearer "):
            token = provided_auth.split(" ", 1)[1].strip()
        session = None
        if token and store is not None:
            try:
                session = store.get(token)
            except Exception:  # pragma: no cover - defensive fallback when store misbehaves
                session = None
        lower_header_keys = {key.lower() for key in headers}
        header_account_present = "x-account-id" in lower_header_keys
        if account_id and "x-account-id" not in lower_header_keys:
            if not provided_auth:
                headers["X-Account-ID"] = str(account_id)
                lower_header_keys.add("x-account-id")
            else:
                session_account = (
                    getattr(session, "admin_id", None) if session is not None else None
                )
                if session_account and session_account.strip().lower() == str(account_id).strip().lower():
                    headers["X-Account-ID"] = str(account_id)
                    lower_header_keys.add("x-account-id")
        if not override_present:
            if account_id and security_module is not None and _allow_admin_autoload():
                try:
                    existing = set(getattr(security_module, "ADMIN_ACCOUNTS", set()))
                    sanitized_account = str(account_id).strip()
                    if sanitized_account and sanitized_account not in existing:
                        subject = _extract_jwt_subject(token) if token else None
                        session_admin = getattr(session, "admin_id", None)
                        trusted_sources = {
                            value.strip().lower()
                            for value in (subject, session_admin)
                            if isinstance(value, str) and value.strip()
                        }
                        if sanitized_account.strip().lower() in trusted_sources:
                            updated = set(existing)
                            updated.add(sanitized_account)
                            security_module.reload_admin_accounts(updated)
                except Exception:  # pragma: no cover - defensive guard for minimal stubs
                    pass
            if store is None and security_module is not None:
                store = getattr(security_module, "_DEFAULT_SESSION_STORE", None)
                if store is not None:
                    self.app.state.session_store = store
            if store is not None and account_id:
                if token:
                    if session is None:
                        session = store.get(token)
                    if session is None:
                        if provided_auth:
                            headers["Authorization"] = str(provided_auth)
                        else:
                            session_account = _extract_jwt_subject(token) or str(account_id)
                            session = store.create(str(session_account))
                            headers["Authorization"] = f"Bearer {session.token}"
                elif provided_auth:
                    headers["Authorization"] = str(provided_auth)
                elif header_account_present and normalized_method == "GET":
                    session = store.create(str(account_id))
                    headers["Authorization"] = f"Bearer {session.token}"
        return self._handle_call(normalized_method, url, json=json, headers=headers, params=params)


def _run_async(factory: Callable[[], Any]) -> Any:
    import asyncio

    def _resolve() -> Any:
        return factory()

    candidate = _resolve()
    if not asyncio.iscoroutine(candidate):
        return candidate
    try:
        return asyncio.run(candidate)
    except RuntimeError as exc:
        message = str(exc)
        if "asyncio.run() cannot be called" not in message:
            raise
        new_loop = asyncio.new_event_loop()
        try:
            candidate = _resolve()
            if asyncio.iscoroutine(candidate):
                return new_loop.run_until_complete(candidate)
            return candidate
        finally:
            try:
                new_loop.run_until_complete(new_loop.shutdown_asyncgens())
            finally:
                new_loop.close()


def _install_fastapi_module() -> None:
    if importlib is None or sys is None:  # pragma: no cover - safeguard for exotic platforms
        return
    try:
        spec = importlib.util.find_spec("fastapi")
    except ValueError:
        spec = None
    if spec is not None:
        return

    fastapi_module = ModuleType("fastapi")
    for name in __all__:
        fastapi_module.__dict__[name] = globals()[name]
    fastapi_module.status = status  # ensure shared instance
    fastapi_module.run_in_threadpool = run_in_threadpool

    testclient_module = ModuleType("fastapi.testclient")
    testclient_module.TestClient = TestClient  # type: ignore[attr-defined]

    responses_module = ModuleType("fastapi.responses")
    responses_module.JSONResponse = JSONResponse  # type: ignore[attr-defined]
    responses_module.StreamingResponse = StreamingResponse  # type: ignore[attr-defined]

    exceptions_module = ModuleType("fastapi.exceptions")
    exceptions_module.HTTPException = HTTPException  # type: ignore[attr-defined]
    exceptions_module.RequestValidationError = RequestValidationError  # type: ignore[attr-defined]

    encoders_module = ModuleType("fastapi.encoders")
    encoders_module.jsonable_encoder = jsonable_encoder  # type: ignore[attr-defined]

    concurrency_module = ModuleType("fastapi.concurrency")
    concurrency_module.run_in_threadpool = run_in_threadpool  # type: ignore[attr-defined]

    middleware_module = ModuleType("fastapi.middleware")
    cors_module = ModuleType("fastapi.middleware.cors")
    cors_module.CORSMiddleware = CORSMiddleware  # type: ignore[attr-defined]
    middleware_module.cors = cors_module

    sys.modules.setdefault("fastapi", fastapi_module)
    sys.modules.setdefault("fastapi.testclient", testclient_module)
    sys.modules.setdefault("fastapi.responses", responses_module)
    sys.modules.setdefault("fastapi.exceptions", exceptions_module)
    sys.modules.setdefault("fastapi.encoders", encoders_module)
    sys.modules.setdefault("fastapi.concurrency", concurrency_module)
    sys.modules.setdefault("fastapi.middleware", middleware_module)
    sys.modules.setdefault("fastapi.middleware.cors", cors_module)


_install_fastapi_module()
