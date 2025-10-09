"""FastAPI service exposing simulation mode controls for the platform."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional

import logging

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from common.schemas.contracts import SimModeEvent
from metrics import setup_metrics
from services.common.adapters import KafkaNATSAdapter
from services.common.security import get_admin_accounts, require_admin_account
from shared.sim_mode import SimModeStatus, sim_mode_repository
from shared.audit_hooks import AuditEvent, load_audit_hooks
from shared.health import setup_health_checks


LOGGER = logging.getLogger("sim_mode_service")

app = FastAPI(title="Simulation Mode Service", version="1.0.0")
setup_metrics(app, service_name="sim-mode")


def _health_check_repository() -> None:
    if sim_mode_repository is None:
        raise RuntimeError("sim mode repository unavailable")


setup_health_checks(app, {"repository": _health_check_repository})


class AccountSimModeStatus(BaseModel):
    account_id: str = Field(..., min_length=1)
    active: bool
    reason: Optional[str]
    since: datetime

    @classmethod
    def from_status(cls, status: SimModeStatus) -> "AccountSimModeStatus":
        return cls(
            account_id=status.account_id,
            active=status.active,
            reason=status.reason,
            since=status.ts,
        )


class SimModeStatusEnvelope(BaseModel):
    active: bool
    accounts: list[AccountSimModeStatus]

    @classmethod
    def from_statuses(cls, statuses: Iterable[SimModeStatus]) -> "SimModeStatusEnvelope":
        account_payloads = [AccountSimModeStatus.from_status(status) for status in statuses]
        return cls(active=any(item.active for item in account_payloads), accounts=account_payloads)


class SimModeEnterRequest(BaseModel):
    reason: str = Field(..., min_length=1, description="Why simulation mode is being enabled")
    account_id: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Trading account to toggle (defaults to the authenticated actor)",
    )


class SimModeTransitionResponse(AccountSimModeStatus):
    actor: str

    @classmethod
    def from_status(cls, status: SimModeStatus, actor: str) -> "SimModeTransitionResponse":
        payload = AccountSimModeStatus.from_status(status).model_dump()
        payload["actor"] = actor
        return cls.model_validate(payload)


async def _publish_event(status: SimModeStatus, actor: str) -> None:
    adapter = KafkaNATSAdapter(account_id="platform")
    event = SimModeEvent(
        account_id=status.account_id,
        active=status.active,
        reason=status.reason,
        ts=status.ts,
        actor=actor,
    )
    try:
        await adapter.publish("platform.sim_mode", event.model_dump(mode="json"))
    except Exception:
        LOGGER.exception("Failed to publish simulation mode event", extra={"actor": actor})


def _log_audit_transition(before: SimModeStatus, after: SimModeStatus, actor: str, request: Request) -> None:
    audit_hooks = load_audit_hooks()
    event = AuditEvent(
        actor=actor,
        action="sim_mode.transition",
        entity=after.account_id,
        before={"active": before.active, "reason": before.reason, "ts": before.ts.isoformat()},
        after={"active": after.active, "reason": after.reason, "ts": after.ts.isoformat()},
        ip_address=request.client.host if request.client else None,
    )
    event.log_with_fallback(
        audit_hooks,
        LOGGER,
        failure_message="Failed to record audit trail for simulation mode transition",
        disabled_message="Audit logging disabled; skipping sim_mode.transition",
    )


def _resolve_account(requested: Optional[str], fallback: str) -> str:
    if requested is None:
        return fallback
    candidate = requested.strip()
    if not candidate:
        return fallback
    allowed = get_admin_accounts()
    if candidate not in allowed:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Unknown trading account '{candidate}'")
    return candidate


@app.get("/sim/status", response_model=SimModeStatusEnvelope)
async def get_status(account_id: Optional[str] = Query(default=None, alias="account_id")) -> SimModeStatusEnvelope:
    accounts = [account_id] if account_id else sorted(get_admin_accounts())
    if not accounts:
        return SimModeStatusEnvelope(active=False, accounts=[])

    statuses = await sim_mode_repository.get_many_async(accounts)
    return SimModeStatusEnvelope.from_statuses(statuses)


@app.post("/sim/enter", response_model=SimModeTransitionResponse, status_code=status.HTTP_200_OK)
async def enter_simulation_mode(
    payload: SimModeEnterRequest = Body(...),
    actor: str = Depends(require_admin_account),
    request: Request = None,
) -> SimModeTransitionResponse:
    if request is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Request context unavailable")

    target_account = _resolve_account(payload.account_id, actor)

    before = await sim_mode_repository.get_status_async(target_account, use_cache=False)
    if before.active:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Simulation mode already active")

    after = await sim_mode_repository.set_status_async(target_account, True, payload.reason)
    await _publish_event(after, actor)
    _log_audit_transition(before, after, actor, request)
    return SimModeTransitionResponse.from_status(after, actor)


@app.post("/sim/exit", response_model=SimModeTransitionResponse)
async def exit_simulation_mode(
    actor: str = Depends(require_admin_account),
    request: Request = None,
    account_id: Optional[str] = Query(default=None, alias="account_id"),
) -> SimModeTransitionResponse:
    if request is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Request context unavailable")

    target_account = _resolve_account(account_id, actor)

    before = await sim_mode_repository.get_status_async(target_account, use_cache=False)
    if not before.active:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Simulation mode already inactive")

    after = await sim_mode_repository.set_status_async(target_account, False, None)
    await _publish_event(after, actor)
    _log_audit_transition(before, after, actor, request)
    return SimModeTransitionResponse.from_status(after, actor)


__all__ = ["app"]

