"""Reusable FastAPI router that executes shared readiness probes."""

from __future__ import annotations

import inspect
from collections import OrderedDict
from typing import Any, Awaitable, Callable, Mapping

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from starlette import status

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
