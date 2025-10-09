"""Utilities for attaching `/healthz` endpoints to FastAPI applications."""

from __future__ import annotations

import inspect
from typing import Any, Awaitable, Callable, Dict, Mapping, MutableMapping

from fastapi import FastAPI

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
