"""API surface for log export status reporting."""

from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, HTTPException

from logging_export import MissingDependencyError, latest_export


router = APIRouter(prefix="/logging/export", tags=["logging"])


@router.get("/status")
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

