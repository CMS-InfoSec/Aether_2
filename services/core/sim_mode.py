"""Simulation mode toggle endpoints for the control plane."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Awaitable, Callable, Iterable, Optional, ParamSpec, TypeVar, cast

try:  # pragma: no cover - prefer FastAPI when available
    from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (
        APIRouter,
        Body,
        Depends,
        HTTPException,
        Query,
        Request,
        status,
    )  # type: ignore[assignment]

from shared.pydantic_compat import BaseModel, Field

from common.schemas.contracts import SimModeEvent
from services.common.security import get_admin_accounts, require_admin_account
from shared.sim_mode import KafkaNATSAdapter, SimModeStatus, sim_mode_repository
from shared.simulation import sim_mode_state
from shared.audit_hooks import AuditEvent, load_audit_hooks


LOGGER = logging.getLogger(__name__)

router = APIRouter(prefix="/sim", tags=["Simulation Mode"])

P = ParamSpec("P")
R = TypeVar("R")


def _router_get(
    *args: Any, **kwargs: Any
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Return a typed ``router.get`` decorator preserving call signatures."""

    return cast(Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]], router.get(*args, **kwargs))


def _router_post(
    *args: Any, **kwargs: Any
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Return a typed ``router.post`` decorator preserving call signatures."""

    return cast(
        Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]],
        router.post(*args, **kwargs),
    )


class AccountSimModeStatusResponse(BaseModel):
    """Simulation status for a single trading account."""

    account_id: str = Field(..., min_length=1, description="Trading account identifier")
    active: bool
    reason: Optional[str]
    since: datetime = Field(..., description="Timestamp of the last transition")

    @classmethod
    def from_status(cls, status: SimModeStatus) -> "AccountSimModeStatusResponse":
        return cls(
            account_id=status.account_id,
            active=status.active,
            reason=status.reason,
            since=status.ts,
        )


class SimModeStatusEnvelope(BaseModel):
    """Collection of simulation mode statuses across accounts."""

    active: bool = Field(..., description="True when any account is in simulation mode")
    accounts: list[AccountSimModeStatusResponse]

    @classmethod
    def from_statuses(cls, statuses: Iterable[SimModeStatus]) -> "SimModeStatusEnvelope":
        account_payloads = [AccountSimModeStatusResponse.from_status(status) for status in statuses]
        overall_active = any(account.active for account in account_payloads)
        return cls(active=overall_active, accounts=account_payloads)


class SimModeEnterRequest(BaseModel):
    """Payload required when entering simulation mode."""

    reason: str = Field(..., min_length=1, description="Why simulation mode is being enabled")
    account_id: Optional[str] = Field(
        default=None,
        description="Trading account to place in simulation mode (defaults to the caller's account)",
        min_length=1,
    )


class SimModeTransitionResponse(AccountSimModeStatusResponse):
    """Status response that also includes the actor that performed the change."""

    actor: str

    @classmethod
    def from_status(
        cls,
        status: SimModeStatus,
        actor: str | None = None,
    ) -> "SimModeTransitionResponse":
        if actor is None:
            raise ValueError("actor must be provided for SimModeTransitionResponse")
        payload = AccountSimModeStatusResponse.from_status(status).model_dump()
        payload["actor"] = actor
        return cls(**payload)


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


_AUDIT_HOOKS = load_audit_hooks()


async def _sync_runtime_state(account_id: str, active: bool) -> None:
    if active:
        await sim_mode_state.enable(account_id)
    else:
        await sim_mode_state.disable(account_id)


def _audit_transition(
    before: SimModeStatus, after: SimModeStatus, actor: str, request: Request
) -> None:
    audit_hooks = load_audit_hooks()
    event = AuditEvent(
        actor=actor,
        action="sim_mode.transition",
        entity=after.account_id,
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


def _resolve_target_account(requested: Optional[str], fallback: str) -> str:
    if requested is None:
        return fallback
    candidate = requested.strip()
    if not candidate:
        return fallback
    allowed = get_admin_accounts()
    if candidate not in allowed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown trading account '{candidate}'",
        )
    return candidate


@_router_get("/status", response_model=SimModeStatusEnvelope)
async def get_status(account_id: Optional[str] = Query(default=None, alias="account_id")) -> SimModeStatusEnvelope:
    accounts = [account_id] if account_id else sorted(get_admin_accounts())
    if not accounts:
        return SimModeStatusEnvelope(active=False, accounts=[])

    statuses = await sim_mode_repository.get_many_async(accounts)
    for status_obj in statuses:
        await _sync_runtime_state(status_obj.account_id, status_obj.active)
    return SimModeStatusEnvelope.from_statuses(statuses)


@_router_post("/enter", response_model=SimModeTransitionResponse)
async def enter_simulation_mode(
    payload: SimModeEnterRequest = Body(...),
    actor: str = Depends(require_admin_account),
    request: Optional[Request] = None,
) -> SimModeTransitionResponse:
    if request is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Request context unavailable",
        )

    target_account = _resolve_target_account(payload.account_id, actor)

    before = await sim_mode_repository.get_status_async(target_account, use_cache=False)
    if before.active:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Simulation mode already active",
        )

    after = await sim_mode_repository.set_status_async(target_account, True, payload.reason)
    await _sync_runtime_state(target_account, True)
    await _publish_event(after, actor)
    _audit_transition(before, after, actor, request)
    return SimModeTransitionResponse.from_status(after, actor=actor)


@_router_post("/exit", response_model=SimModeTransitionResponse)
async def exit_simulation_mode(
    actor: str = Depends(require_admin_account),
    request: Optional[Request] = None,
    account_id: Optional[str] = Query(default=None, alias="account_id"),
) -> SimModeTransitionResponse:
    if request is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Request context unavailable",
        )

    target_account = _resolve_target_account(account_id, actor)

    before = await sim_mode_repository.get_status_async(target_account, use_cache=False)
    if not before.active:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Simulation mode already inactive",
        )

    after = await sim_mode_repository.set_status_async(target_account, False, None)
    await _sync_runtime_state(target_account, False)
    await _publish_event(after, actor)
    _audit_transition(before, after, actor, request)
    return SimModeTransitionResponse.from_status(after, actor=actor)


__all__ = ["router"]

