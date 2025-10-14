"""API surface for log export status reporting."""

from __future__ import annotations

import datetime as dt
from typing import Any, Callable, TypeVar, cast

try:  # pragma: no cover - prefer FastAPI when available
    from fastapi import APIRouter, HTTPException
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import APIRouter, HTTPException  # type: ignore[assignment]

from logging_export import MissingDependencyError, latest_export


router = APIRouter(prefix="/logging/export", tags=["logging"])

RouteFn = TypeVar("RouteFn", bound=Callable[..., Any])


def _router_get(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``router.get`` to satisfy static analysis."""

    return cast(Callable[[RouteFn], RouteFn], router.get(*args, **kwargs))


@_router_get("/status")
def export_status() -> dict[str, dt.datetime | str | None]:
    """Return metadata describing the most recent log export."""

    try:
        result = latest_export()
    except MissingDependencyError as exc:  # pragma: no cover - exercised via HTTPException
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if result is None:
        return {"last_export": None, "hash": None}

    return {
        "last_export": result.exported_at.astimezone(dt.timezone.utc).isoformat(),
        "hash": result.sha256,
    }


__all__ = ["router", "export_status"]

