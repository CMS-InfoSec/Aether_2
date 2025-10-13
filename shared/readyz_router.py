"""Reusable FastAPI router that executes shared readiness probes."""

from __future__ import annotations

import inspect
from collections import OrderedDict
from typing import Any, Awaitable, Callable, Mapping

from fastapi import APIRouter

try:  # pragma: no cover - fallback for lightweight FastAPI stubs
    from fastapi.responses import JSONResponse
except Exception:  # pragma: no cover - degrade gracefully when dependencies missing
    class JSONResponse:  # type: ignore[no-redef]
        """Minimal stand-in used when FastAPI dependencies are unavailable."""

        def __init__(self, content: dict[str, Any], status_code: int = 200) -> None:
            self.content = content
            self.status_code = status_code

        def __iter__(self):  # pragma: no cover - compatibility shim
            yield from self.content
try:  # pragma: no cover - prefer Starlette when available
    from starlette import status
except Exception:  # pragma: no cover - minimal constants for lightweight tests
    class _StatusCodes:
        HTTP_200_OK = 200
        HTTP_503_SERVICE_UNAVAILABLE = 503

    status = _StatusCodes()  # type: ignore[assignment]

from .readiness import ReadinessProbeError

ProbeCallable = Callable[[], Awaitable[Any] | Any]


async def _maybe_await(result: Awaitable[Any] | Any) -> Any:
    if inspect.isawaitable(result):  # pragma: no branch - hot path
        return await result  # type: ignore[return-value]
    return result


class ReadyzRouter:
    """Registry of readiness probes exposed via a shared `/readyz` endpoint."""

    def __init__(self) -> None:
        self._router = APIRouter()
        if not hasattr(self._router, "prefix"):  # pragma: no cover - compatibility with stubs
            setattr(self._router, "prefix", "")
        self._probes: "OrderedDict[str, ProbeCallable]" = OrderedDict()

        @self._router.get("/readyz", include_in_schema=False)
        async def readyz() -> JSONResponse:
            status_label = "ok"
            payload: dict[str, Any] = {}

            for name, probe in self._probes.items():
                try:
                    result = await _maybe_await(probe())
                except ReadinessProbeError as exc:
                    status_label = "error"
                    payload[name] = {"status": "error", "error": str(exc)}
                except Exception as exc:  # pragma: no cover - defensive logging surface
                    status_label = "error"
                    payload[name] = {"status": "error", "error": str(exc)}
                else:
                    entry: dict[str, Any] = {"status": "ok"}
                    if isinstance(result, Mapping):
                        entry.update(result)
                    payload[name] = entry

            http_status = (
                status.HTTP_200_OK
                if status_label == "ok"
                else status.HTTP_503_SERVICE_UNAVAILABLE
            )
            body = {"status": status_label, "checks": payload}
            return JSONResponse(body, status_code=http_status)

    @property
    def router(self) -> APIRouter:
        """Return the underlying FastAPI router."""

        return self._router

    def register_probe(self, name: str, probe: ProbeCallable) -> None:
        """Register a readiness probe executed when `/readyz` is queried."""

        if not callable(probe):  # pragma: no cover - defensive guard
            raise TypeError("probe must be callable")
        self._probes[name] = probe


__all__ = ["ReadyzRouter", "ProbeCallable"]
