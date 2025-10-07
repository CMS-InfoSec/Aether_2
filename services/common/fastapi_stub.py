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

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional


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
    "JSONResponse",
    "Path",
    "Query",
    "Request",
    "Response",
    "StreamingResponse",
    "UploadFile",
    "status",
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


def Depends(dependency: Callable[..., Any]) -> Callable[..., Any]:
    """Return the dependency unchanged, mimicking FastAPI's behaviour."""

    return dependency


def Body(default: Any = None, *_: Any, **__: Any) -> Any:
    return default


def Query(default: Any = None, *_: Any, **__: Any) -> Any:
    return default


def Path(default: Any = None, *_: Any, **__: Any) -> Any:
    return default


def Header(default: Any = None, *_: Any, **__: Any) -> Any:
    return default


def Cookie(default: Any = None, *_: Any, **__: Any) -> Any:
    return default


def File(default: Any = None, *_: Any, **__: Any) -> Any:
    return default


def Form(default: Any = None, *_: Any, **__: Any) -> Any:
    return default


class UploadFile:
    """Extremely small ``UploadFile`` placeholder."""

    def __init__(self, filename: str | None = None, file: Any | None = None) -> None:
        self.filename = filename or ""
        self.file = file


class Request:
    """Bare-minimum representation of a request object."""

    def __init__(self, headers: Optional[Dict[str, Any]] = None) -> None:
        self.headers = headers or {}
        self.state = SimpleNamespace()


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


class APIRouter:
    """Minimal router recording registered routes for inspection in tests."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.routes: List[tuple[str, str, Callable[..., Any]]] = []
        self.prefix = kwargs.get("prefix", "")
        self.tags = list(kwargs.get("tags", []))

    def add_api_route(
        self,
        path: str,
        endpoint: Callable[..., Any],
        *,
        methods: Optional[Iterable[str]] = None,
    ) -> None:
        verbs = tuple(methods) if methods is not None else ("GET",)
        for verb in verbs:
            self.routes.append((verb.upper(), path, endpoint))

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


class FastAPI:
    """Tiny FastAPI replacement that collects router registrations."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.state = SimpleNamespace()
        self.routes: List[Any] = []
        self.user_middleware: List[Any] = []
        self.dependency_overrides: Dict[Any, Callable[..., Any]] = {}
        self.router = SimpleNamespace(lifespan_context=None)
        self.title = kwargs.get("title")

    def include_router(self, router: APIRouter, *args: Any, **kwargs: Any) -> None:
        self.routes.extend(router.routes)

    def add_middleware(self, middleware_cls: Any, **kwargs: Any) -> None:
        self.user_middleware.append(SimpleNamespace(cls=middleware_cls, kwargs=kwargs))

    def _register_route(self, path: str, endpoint: Callable[..., Any], methods: Iterable[str]) -> None:
        for method in methods:
            self.routes.append(SimpleNamespace(path=path, endpoint=endpoint, methods=tuple(methods)))

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

    def on_event(self, *_: Any, **__: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
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


status = SimpleNamespace(
    HTTP_200_OK=200,
    HTTP_201_CREATED=201,
    HTTP_202_ACCEPTED=202,
    HTTP_204_NO_CONTENT=204,
    HTTP_400_BAD_REQUEST=400,
    HTTP_401_UNAUTHORIZED=401,
    HTTP_403_FORBIDDEN=403,
    HTTP_404_NOT_FOUND=404,
    HTTP_409_CONFLICT=409,
    HTTP_412_PRECONDITION_FAILED=412,
    HTTP_422_UNPROCESSABLE_ENTITY=422,
    HTTP_429_TOO_MANY_REQUESTS=429,
    HTTP_500_INTERNAL_SERVER_ERROR=500,
)


def jsonable_encoder(value: Any, *args: Any, **kwargs: Any) -> Any:
    """Fallback encoder mirroring FastAPI's helper behaviour."""

    return value


@dataclass
class RequestValidationError(Exception):
    """Placeholder error raised when validation fails under the stub."""

    errors: List[Dict[str, Any]]
