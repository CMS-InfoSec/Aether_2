"""Correlation ID utilities and middleware."""
from __future__ import annotations

import logging
import uuid
from contextvars import ContextVar, Token
from typing import Any, Awaitable, Callable, Optional

try:  # pragma: no cover - optional dependency for test environments
    from fastapi import Request
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.types import ASGIApp
except ImportError:  # pragma: no cover - fallback for unit tests without FastAPI
    Request = Any  # type: ignore

    class BaseHTTPMiddleware:  # type: ignore
        def __init__(self, app: Any) -> None:
            self.app = app

        async def dispatch(
            self, request: Any, call_next: Callable[[Any], Awaitable[Any]]
        ) -> Any:
            return await call_next(request)

    ASGIApp = Any  # type: ignore

logger = logging.getLogger(__name__)

_correlation_id_ctx: ContextVar[Optional[str]] = ContextVar(
    "correlation_id", default=None
)


def get_correlation_id() -> Optional[str]:
    """Return the correlation ID for the current request context if present."""
    return _correlation_id_ctx.get()


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Middleware that guarantees correlation IDs propagate between services."""

    header_name = "X-Correlation-ID"

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Any]]
    ) -> Any:
        incoming_id = request.headers.get(self.header_name)
        correlation_id = incoming_id or str(uuid.uuid4())
        token = _correlation_id_ctx.set(correlation_id)
        logger.debug("Assigned correlation id %s", correlation_id)
        try:
            response = await call_next(request)
        finally:
            _correlation_id_ctx.reset(token)
        response.headers[self.header_name] = correlation_id
        return response


class CorrelationContext:
    """Helper to temporarily bind correlation IDs for out-of-band operations."""

    def __init__(self, correlation_id: Optional[str] = None) -> None:
        self._correlation_id = correlation_id or str(uuid.uuid4())
        self._token: Token[Optional[str]] | None = None

    def __enter__(self) -> str:
        self._token = _correlation_id_ctx.set(self._correlation_id)
        return self._correlation_id

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if self._token is not None:
            _correlation_id_ctx.reset(self._token)
            self._token = None
