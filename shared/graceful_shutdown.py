"""Utilities for coordinating graceful shutdown behaviour across services."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
import threading
import time
from contextlib import suppress
from datetime import datetime, timezone
from typing import Callable, Iterable, List, Optional, Set

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette import status as http_status


logger = logging.getLogger(__name__)

FlushCallback = Callable[[], None]


def _normalise_path(path: str) -> str:
    """Return a canonical representation for FastAPI request paths."""

    if not path:
        return "/"
    clean = path.split("?", 1)[0]
    if clean != "/" and clean.endswith("/"):
        clean = clean.rstrip("/")
    return clean or "/"


class GracefulShutdownManager:
    """Tracks draining state and coordinates graceful shutdown hooks."""

    def __init__(
        self,
        service_name: str,
        *,
        allowed_paths: Optional[Iterable[str]] = None,
        shutdown_timeout: float = 45.0,
    ) -> None:
        self.service_name = service_name
        self._condition = threading.Condition()
        self._draining = False
        self._inflight = 0
        self._drain_started_at: Optional[datetime] = None
        self._shutdown_timeout = max(shutdown_timeout, 0.0)
        self._allowed_paths: Set[str] = {
            "/ops/drain/start",
            "/ops/drain/status",
            "/metrics",
            "/health",
            "/livez",
            "/readyz",
        }
        if allowed_paths:
            for path in allowed_paths:
                self.allow_path(path)
        self._flush_callbacks: List[FlushCallback] = []

    @property
    def draining(self) -> bool:
        with self._condition:
            return self._draining

    @property
    def inflight(self) -> int:
        with self._condition:
            return self._inflight

    @property
    def shutdown_timeout(self) -> float:
        return self._shutdown_timeout

    def allow_path(self, path: str) -> None:
        self._allowed_paths.add(_normalise_path(path))

    def is_path_allowed(self, path: str) -> bool:
        return _normalise_path(path) in self._allowed_paths

    def register_flush_callback(self, callback: FlushCallback) -> None:
        self._flush_callbacks.append(callback)

    def increment_inflight(self) -> None:
        with self._condition:
            self._inflight += 1

    def decrement_inflight(self) -> None:
        with self._condition:
            if self._inflight > 0:
                self._inflight -= 1
            else:  # pragma: no cover - defensive guard
                self._inflight = 0
            if self._inflight == 0:
                self._condition.notify_all()

    def start_draining(self, *, reason: str = "manual") -> bool:
        callbacks: List[FlushCallback] = []
        with self._condition:
            if self._draining:
                started = False
            else:
                started = True
                self._draining = True
                self._drain_started_at = datetime.now(timezone.utc)
                callbacks = list(self._flush_callbacks)
        if callbacks:
            logger.info("Initiating drain for service %s (reason=%s)", self.service_name, reason)
        for callback in callbacks:
            with suppress(Exception):
                callback()
        return started

    def reset(self) -> None:
        """Clear draining state to allow the service to accept requests again."""

        with self._condition:
            self._draining = False
            self._drain_started_at = None

    def wait_for_inflight(self, timeout: Optional[float] = None) -> bool:
        deadline = None if timeout is None else time.monotonic() + max(timeout, 0.0)
        with self._condition:
            while self._inflight > 0:
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                else:
                    remaining = None
                self._condition.wait(timeout=remaining)
        return True

    async def wait_for_inflight_async(self, timeout: Optional[float] = None) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.wait_for_inflight, timeout)

    def status(self) -> dict:
        with self._condition:
            started_at = self._drain_started_at.isoformat() if self._drain_started_at else None
            elapsed = None
            if self._draining and self._drain_started_at is not None:
                elapsed = (datetime.now(timezone.utc) - self._drain_started_at).total_seconds()
            return {
                "service": self.service_name,
                "draining": self._draining,
                "inflight": self._inflight,
                "started_at": started_at,
                "elapsed_seconds": elapsed,
                "allowed_paths": sorted(self._allowed_paths),
            }


def flush_logging_handlers(*logger_names: str) -> None:
    """Flush handlers for the provided loggers (including the root logger)."""

    if not logger_names:
        logger_names = ("",)
    seen_handlers: Set[int] = set()
    for name in logger_names:
        log = logging.getLogger(name)
        for handler in log.handlers:
            handler_id = id(handler)
            if handler_id in seen_handlers:
                continue
            seen_handlers.add(handler_id)
            with suppress(Exception):
                handler.flush()


def install_sigterm_handler(manager: GracefulShutdownManager) -> None:
    """Register a SIGTERM handler that triggers draining before exit."""

    original = signal.getsignal(signal.SIGTERM)

    def _handle(signum, frame) -> None:  # pragma: no cover - signal path is hard to unit test
        logger.info("SIGTERM received for %s; initiating drain", manager.service_name)
        manager.start_draining(reason="sigterm")
        completed = manager.wait_for_inflight(manager.shutdown_timeout)
        if not completed:
            logger.warning(
                "Timed out waiting for in-flight requests during SIGTERM for %s",
                manager.service_name,
            )
        if callable(original) and original is not _handle:
            original(signum, frame)
        elif original in {signal.SIG_DFL, None}:
            sys.exit(0)

    signal.signal(signal.SIGTERM, _handle)


def setup_graceful_shutdown(
    app: FastAPI,
    *,
    service_name: str,
    allowed_paths: Optional[Iterable[str]] = None,
    shutdown_timeout: float = 45.0,
    logger_instance: Optional[logging.Logger] = None,
) -> GracefulShutdownManager:
    """Configure graceful shutdown middleware, endpoints, and signal handling."""

    manager = GracefulShutdownManager(
        service_name,
        allowed_paths=allowed_paths,
        shutdown_timeout=shutdown_timeout,
    )

    if allowed_paths:
        for path in allowed_paths:
            manager.allow_path(path)
    manager.allow_path("/ops/drain/start")
    manager.allow_path("/ops/drain/status")

    service_logger = logger_instance or logger

    @app.middleware("http")
    async def _drain_guard(request: Request, call_next):
        if manager.draining and not manager.is_path_allowed(request.url.path):
            detail = {
                "detail": f"{service_name} is draining",
                "status": manager.status(),
            }
            return JSONResponse(detail, status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE)
        manager.increment_inflight()
        try:
            response = await call_next(request)
        finally:
            manager.decrement_inflight()
        return response

    @app.post("/ops/drain/start", tags=["ops"])
    async def start_drain(response: Response):
        started = manager.start_draining(reason="api")
        response.status_code = (
            http_status.HTTP_202_ACCEPTED if started else http_status.HTTP_200_OK
        )
        service_logger.info("Drain request acknowledged", extra={"draining": manager.draining})
        return manager.status()

    @app.get("/ops/drain/status", tags=["ops"])
    async def drain_status():
        return manager.status()

    @app.on_event("startup")
    async def _on_startup() -> None:
        manager.reset()
        install_sigterm_handler(manager)

    @app.on_event("shutdown")
    async def _on_shutdown() -> None:
        manager.start_draining(reason="shutdown_event")
        await manager.wait_for_inflight_async(timeout=manager.shutdown_timeout)

    return manager
