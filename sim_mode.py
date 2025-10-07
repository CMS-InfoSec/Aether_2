"""FastAPI service exposing simulation mode controls for the platform."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import logging

from fastapi import Body, Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field

from common.schemas.contracts import SimModeEvent
from metrics import setup_metrics
from services.common.adapters import KafkaNATSAdapter
from services.common.security import require_admin_account
from shared.sim_mode import SimModeStatus, sim_mode_repository
from shared.audit_hooks import AuditEvent, load_audit_hooks, log_audit_event_with_fallback


LOGGER = logging.getLogger("sim_mode_service")

app = FastAPI(title="Simulation Mode Service", version="1.0.0")
setup_metrics(app, service_name="sim-mode")


class SimModeStatusResponse(BaseModel):
    active: bool
    reason: Optional[str]
    ts: datetime

    @classmethod
    def from_status(cls, status: SimModeStatus) -> "SimModeStatusResponse":
        return cls(active=status.active, reason=status.reason, ts=status.ts)


class SimModeEnterRequest(BaseModel):
    reason: str = Field(..., min_length=1, description="Why simulation mode is being enabled")


class SimModeTransitionResponse(SimModeStatusResponse):
    actor: str

    @classmethod
    def from_status(cls, status: SimModeStatus, actor: str) -> "SimModeTransitionResponse":
        base = SimModeStatusResponse.from_status(status)
        payload = base.model_dump()
        payload["actor"] = actor
        return cls.model_validate(payload)


async def _publish_event(status: SimModeStatus, actor: str) -> None:
    adapter = KafkaNATSAdapter(account_id="platform")
    event = SimModeEvent(active=status.active, reason=status.reason, ts=status.ts, actor=actor)
    try:
        await adapter.publish("platform.sim_mode", event.model_dump(mode="json"))
    except Exception:
        LOGGER.exception("Failed to publish simulation mode event", extra={"actor": actor})


def _log_audit_transition(before: SimModeStatus, after: SimModeStatus, actor: str, request: Request) -> None:
    audit_hooks = load_audit_hooks()
    event = AuditEvent(
        actor=actor,
        action="sim_mode.transition",
        entity="platform",
        before={"active": before.active, "reason": before.reason, "ts": before.ts.isoformat()},
        after={"active": after.active, "reason": after.reason, "ts": after.ts.isoformat()},
        ip_address=request.client.host if request.client else None,
    )
    log_audit_event_with_fallback(
        audit_hooks,
        LOGGER,
        event,
        failure_message="Failed to record audit trail for simulation mode transition",
        disabled_message="Audit logging disabled; skipping sim_mode.transition",
    )


@app.get("/sim/status", response_model=SimModeStatusResponse)
async def get_status() -> SimModeStatusResponse:
    status_obj = await sim_mode_repository.get_status_async()
    return SimModeStatusResponse.from_status(status_obj)


@app.post("/sim/enter", response_model=SimModeTransitionResponse, status_code=status.HTTP_200_OK)
async def enter_simulation_mode(
    payload: SimModeEnterRequest = Body(...),
    actor: str = Depends(require_admin_account),
    request: Request = None,
) -> SimModeTransitionResponse:
    if request is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Request context unavailable")

    before = await sim_mode_repository.get_status_async(use_cache=False)
    if before.active:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Simulation mode already active")

    after = await sim_mode_repository.set_status_async(True, payload.reason)
    await _publish_event(after, actor)
    _log_audit_transition(before, after, actor, request)
    return SimModeTransitionResponse.from_status(after, actor)


@app.post("/sim/exit", response_model=SimModeTransitionResponse)
async def exit_simulation_mode(
    actor: str = Depends(require_admin_account),
    request: Request = None,
) -> SimModeTransitionResponse:
    if request is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Request context unavailable")

    before = await sim_mode_repository.get_status_async(use_cache=False)
    if not before.active:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Simulation mode already inactive")

    after = await sim_mode_repository.set_status_async(False, None)
    await _publish_event(after, actor)
    _log_audit_transition(before, after, actor, request)
    return SimModeTransitionResponse.from_status(after, actor)


__all__ = ["app"]

