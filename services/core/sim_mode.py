"""Simulation mode toggle endpoints for the control plane."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from common.schemas.contracts import SimModeEvent
from services.common.adapters import KafkaNATSAdapter
from services.common.security import require_admin_account
from shared.sim_mode import SimModeStatus, sim_mode_repository
from shared.simulation import sim_mode_state
from shared.audit_hooks import AuditEvent, load_audit_hooks


LOGGER = logging.getLogger(__name__)

router = APIRouter(prefix="/sim", tags=["Simulation Mode"])


class SimModeStatusResponse(BaseModel):
    """Representation of the persisted simulation mode status."""

    active: bool
    reason: Optional[str]
    ts: datetime

    @classmethod
    def from_status(cls, status: SimModeStatus) -> "SimModeStatusResponse":
        return cls(active=status.active, reason=status.reason, ts=status.ts)


class SimModeEnterRequest(BaseModel):
    """Payload required when entering simulation mode."""

    reason: str = Field(..., min_length=1, description="Why simulation mode is being enabled")


class SimModeTransitionResponse(SimModeStatusResponse):
    """Status response that also includes the actor that performed the change."""

    actor: str

    @classmethod
    def from_status(
        cls, status: SimModeStatus, actor: str
    ) -> "SimModeTransitionResponse":
        payload = SimModeStatusResponse.from_status(status).model_dump()
        payload["actor"] = actor
        return cls.model_validate(payload)


async def _publish_event(status: SimModeStatus, actor: str) -> None:
    adapter = KafkaNATSAdapter(account_id="platform")
    event = SimModeEvent(active=status.active, reason=status.reason, ts=status.ts, actor=actor)
    try:
        await adapter.publish("platform.sim_mode", event.model_dump(mode="json"))
    except Exception:
        LOGGER.exception("Failed to publish simulation mode event", extra={"actor": actor})


async def _sync_runtime_state(active: bool) -> None:
    if active:
        await sim_mode_state.enable()
    else:
        await sim_mode_state.disable()


def _audit_transition(
    before: SimModeStatus, after: SimModeStatus, actor: str, request: Request
) -> None:
    audit_hooks = load_audit_hooks()
    event = AuditEvent(
        actor=actor,
        action="sim_mode.transition",
        entity="platform",
        before={
            "active": before.active,
            "reason": before.reason,
            "ts": before.ts.isoformat(),
        },
        after={
            "active": after.active,
            "reason": after.reason,
            "ts": after.ts.isoformat(),
        },
        ip_address=request.client.host if request.client else None,
    )
    event.log_with_fallback(
        audit_hooks,
        LOGGER,
        failure_message="Failed to record audit log for simulation mode transition",
        disabled_message="Audit logging disabled; skipping sim_mode.transition",
    )


@router.get("/status", response_model=SimModeStatusResponse)
async def get_status() -> SimModeStatusResponse:
    status_obj = await sim_mode_repository.get_status_async()
    await _sync_runtime_state(status_obj.active)
    return SimModeStatusResponse.from_status(status_obj)


@router.post("/enter", response_model=SimModeTransitionResponse)
async def enter_simulation_mode(
    payload: SimModeEnterRequest = Body(...),
    actor: str = Depends(require_admin_account),
    request: Request = None,
) -> SimModeTransitionResponse:
    if request is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Request context unavailable",
        )

    before = await sim_mode_repository.get_status_async(use_cache=False)
    if before.active:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Simulation mode already active",
        )

    after = await sim_mode_repository.set_status_async(True, payload.reason)
    await _sync_runtime_state(True)
    await _publish_event(after, actor)
    _audit_transition(before, after, actor, request)
    return SimModeTransitionResponse.from_status(after, actor)


@router.post("/exit", response_model=SimModeTransitionResponse)
async def exit_simulation_mode(
    actor: str = Depends(require_admin_account),
    request: Request = None,
) -> SimModeTransitionResponse:
    if request is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Request context unavailable",
        )

    before = await sim_mode_repository.get_status_async(use_cache=False)
    if not before.active:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Simulation mode already inactive",
        )

    after = await sim_mode_repository.set_status_async(False, None)
    await _sync_runtime_state(False)
    await _publish_event(after, actor)
    _audit_transition(before, after, actor, request)
    return SimModeTransitionResponse.from_status(after, actor)


__all__ = ["router"]

