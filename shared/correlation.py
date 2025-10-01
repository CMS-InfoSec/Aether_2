"""Correlation ID utilities and middleware."""
from __future__ import annotations

import logging
import uuid
from contextvars import ContextVar
from typing import Awaitable, Callable, Optional

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

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
        self, request: Request, call_next: Callable[[Request], Awaitable]
    ):
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
        self._token = None

    def __enter__(self) -> str:
        self._token = _correlation_id_ctx.set(self._correlation_id)
        return self._correlation_id

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._token is not None:
            _correlation_id_ctx.reset(self._token)
            self._token = None
