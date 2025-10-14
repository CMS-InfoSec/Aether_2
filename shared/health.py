"""Utilities for attaching `/healthz` endpoints to FastAPI applications."""

from __future__ import annotations

import inspect
import logging
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Dict, Mapping, MutableMapping, cast

logger = logging.getLogger(__name__)

try:  # pragma: no cover - exercised when FastAPI is installed
    from fastapi import FastAPI
except Exception:  # pragma: no cover - provide a lightweight stand-in
    logger.warning("FastAPI dependency missing; using health check fallback", exc_info=False)

    class _FallbackRoute(SimpleNamespace):
        def __init__(self, path: str, endpoint: Callable[..., Any]) -> None:
            super().__init__(method="GET", path=path, endpoint=endpoint)

    class _FallbackFastAPI:
        def __init__(self) -> None:
            self.routes: list[Any] = []

        def get(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
            def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                self.routes.append(_FallbackRoute(path, func))
                return func

            return _decorator

    FastAPI = cast(Any, _FallbackFastAPI)

HealthCheck = Callable[[], Awaitable[object | None] | object | None]


async def _execute(check: HealthCheck) -> object | None:
    """Execute *check*, awaiting the result when necessary."""

    result = check()
    if inspect.isawaitable(result):
        return await result  # type: ignore[return-value]
    return result


def setup_health_checks(
    app: FastAPI,
    checks: Mapping[str, HealthCheck] | None = None,
) -> None:
    """Attach a `/healthz` endpoint to *app* executing optional *checks*.

    Each check is invoked when the endpoint is queried. Checks may be synchronous
    or asynchronous callables.  A check passes when it returns without raising an
    exception.  If the return value is a mapping it is merged into the response
    payload for additional diagnostics.
    """

    registry: MutableMapping[str, HealthCheck] = dict(checks or {})

    if any(route.path == "/healthz" for route in app.routes):
        return

    @app.get("/healthz")
    async def healthz() -> Dict[str, Any]:  # pragma: no cover - exercised via tests
        overall = "ok"
        results: Dict[str, Any] = {}
        for name, check in registry.items():
            try:
                payload = await _execute(check)
            except Exception as exc:  # pragma: no cover - defensive logging surface
                overall = "error"
                results[name] = {"status": "error", "error": str(exc)}
                continue

            entry: Dict[str, Any] = {"status": "ok"}
            if isinstance(payload, Mapping):
                entry.update(payload)
            results[name] = entry

        return {"status": overall, "checks": results}


__all__ = ["HealthCheck", "setup_health_checks"]
