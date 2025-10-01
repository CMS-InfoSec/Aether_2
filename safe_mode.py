"""Safe mode control service for orchestrating emergency hedges.

This module exposes a small FastAPI application that can automatically engage
safe mode when the Kraken private WebSocket feed is unavailable for more than
30 seconds. Engaging safe mode instructs downstream services to cancel any
open orders and hedge residual positions back to USD. Operators can also
manually enter or exit safe mode via the provided HTTP endpoints.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, status

from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.common.security import require_admin_account

app = FastAPI(title="Safe Mode Service")


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


SAFE_MODE_WS_TIMEOUT_SECONDS = _env_float("SAFE_MODE_WS_TIMEOUT_SECONDS", 30.0)
SAFE_MODE_MONITOR_INTERVAL_SECONDS = _env_float("SAFE_MODE_MONITOR_INTERVAL_SECONDS", 5.0)
SAFE_MODE_AUTOMATION_ACTOR = "system"
SAFE_MODE_OUTAGE_REASON = "kraken_ws_down"


@dataclass(frozen=True)
class SafeModeEvent:
    """Represents a safe mode state transition."""

    account_id: str
    reason: str
    ts: datetime
    state: str
    actor: Optional[str] = None
    automatic: bool = False

    def to_kafka_payload(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "event": "SAFE_MODE_ENTERED" if self.state == "entered" else "SAFE_MODE_EXITED",
            "account_id": self.account_id,
            "timestamp": self.ts.isoformat(),
            "reason": self.reason,
            "automatic": self.automatic,
        }
        if self.actor:
            payload["actor"] = self.actor
        if self.state == "entered":
            payload["actions"] = ["CANCEL_OPEN_ORDERS", "HEDGE_TO_USD"]
        return payload

    def to_response(self) -> Dict[str, object]:
        return {
            "account_id": self.account_id,
            "state": "engaged" if self.state == "entered" else "released",
            "engaged": self.state == "entered",
            "reason": self.reason,
            "ts": self.ts.isoformat(),
            "automatic": self.automatic,
            "actor": self.actor,
        }


@dataclass
class _SafeModeState:
    engaged: bool
    reason: str
    engaged_at: datetime
    automatic: bool


class SafeModeController:
    """Coordinates safe mode state across accounts."""

    def __init__(
        self,
        *,
        downtime_threshold_seconds: float = SAFE_MODE_WS_TIMEOUT_SECONDS,
        monitor_interval_seconds: float = SAFE_MODE_MONITOR_INTERVAL_SECONDS,
    ) -> None:
        self._downtime_threshold = float(max(0.0, downtime_threshold_seconds))
        self._monitor_interval = float(max(0.1, monitor_interval_seconds))
        self._last_heartbeat: Dict[str, datetime] = {}
        self._states: Dict[str, _SafeModeState] = {}
        self._lock = Lock()
        self._task: Optional[asyncio.Task[None]] = None

    @staticmethod
    def _normalize_account(account_id: str) -> str:
        normalized = account_id.strip().lower()
        if not normalized:
            raise ValueError("Account identifier must not be empty.")
        return normalized

    def record_heartbeat(
        self, account_id: str, *, timestamp: Optional[datetime] = None
    ) -> None:
        """Record a Kraken private WebSocket heartbeat for ``account_id``."""

        normalized = self._normalize_account(account_id)
        ts = timestamp or datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        with self._lock:
            self._last_heartbeat[normalized] = ts

    def enter(
        self,
        account_id: str,
        *,
        reason: str,
        actor: Optional[str] = None,
        automatic: bool = False,
        timestamp: Optional[datetime] = None,
    ) -> SafeModeEvent:
        """Engage safe mode for ``account_id``."""

        normalized = self._normalize_account(account_id)
        ts = timestamp or datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        with self._lock:
            self._states[normalized] = _SafeModeState(
                engaged=True,
                reason=reason,
                engaged_at=ts,
                automatic=automatic,
            )

        timescale = TimescaleAdapter(account_id=normalized)
        timescale.set_safe_mode(engaged=True, reason=reason, actor=actor)

        event = SafeModeEvent(
            account_id=normalized,
            reason=reason,
            ts=ts,
            state="entered",
            actor=actor,
            automatic=automatic,
        )

        KafkaNATSAdapter(account_id=normalized).publish(
            topic="core.safe_mode",
            payload=event.to_kafka_payload(),
        )

        return event

    def exit(
        self,
        account_id: str,
        *,
        reason: str,
        actor: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> SafeModeEvent:
        """Release safe mode for ``account_id``."""

        normalized = self._normalize_account(account_id)
        ts = timestamp or datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        with self._lock:
            self._states[normalized] = _SafeModeState(
                engaged=False,
                reason=reason,
                engaged_at=ts,
                automatic=False,
            )

        TimescaleAdapter(account_id=normalized).set_safe_mode(
            engaged=False,
            reason=reason,
            actor=actor,
        )

        event = SafeModeEvent(
            account_id=normalized,
            reason=reason,
            ts=ts,
            state="exited",
            actor=actor,
            automatic=False,
        )

        KafkaNATSAdapter(account_id=normalized).publish(
            topic="core.safe_mode",
            payload=event.to_kafka_payload(),
        )

        return event

    def check_downtime(
        self, *, current_time: Optional[datetime] = None
    ) -> List[SafeModeEvent]:
        """Check for websocket downtime and engage safe mode when required."""

        now = current_time or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        threshold = timedelta(seconds=self._downtime_threshold)
        to_trigger: List[str] = []

        with self._lock:
            for account_id, heartbeat in list(self._last_heartbeat.items()):
                state = self._states.get(account_id)
                if state and state.engaged:
                    continue
                if now - heartbeat > threshold:
                    to_trigger.append(account_id)

        events: List[SafeModeEvent] = []
        for account_id in to_trigger:
            events.append(
                self.enter(
                    account_id,
                    reason=SAFE_MODE_OUTAGE_REASON,
                    actor=SAFE_MODE_AUTOMATION_ACTOR,
                    automatic=True,
                    timestamp=now,
                )
            )
        return events

    async def _run_loop(self) -> None:
        try:
            while True:
                self.check_downtime()
                await asyncio.sleep(self._monitor_interval)
        except asyncio.CancelledError:  # pragma: no cover - cooperative cancellation
            raise

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_loop(), name="safe-mode-monitor")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:  # pragma: no cover - cooperative cancellation
                pass
            self._task = None

    def reset(self) -> None:
        with self._lock:
            self._last_heartbeat.clear()
            self._states.clear()


controller = SafeModeController()


@app.on_event("startup")
async def _startup_monitor() -> None:  # pragma: no cover - FastAPI lifecycle glue
    await controller.start()


@app.on_event("shutdown")
async def _shutdown_monitor() -> None:  # pragma: no cover - FastAPI lifecycle glue
    await controller.stop()


def _handle_enter(
    account_id: str,
    *,
    reason: str,
    actor: str,
    automatic: bool = False,
) -> Dict[str, object]:
    try:
        event = controller.enter(
            account_id,
            reason=reason,
            actor=actor,
            automatic=automatic,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return event.to_response()


def _handle_exit(account_id: str, *, reason: str, actor: str) -> Dict[str, object]:
    try:
        event = controller.exit(account_id, reason=reason, actor=actor)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return event.to_response()


@app.post("/core/safe_mode/enter")
def enter_safe_mode(
    account_id: str = Query(..., min_length=1),
    reason: str = Query("manual_override", min_length=1),
    actor_account: str = Depends(require_admin_account),
) -> Dict[str, object]:
    """Manually engage safe mode for an account."""

    return _handle_enter(account_id, reason=reason, actor=actor_account)


@app.post("/core/safe_mode/exit")
def exit_safe_mode(
    account_id: str = Query(..., min_length=1),
    reason: str = Query("manual_release", min_length=1),
    actor_account: str = Depends(require_admin_account),
) -> Dict[str, object]:
    """Manually release safe mode for an account."""

    return _handle_exit(account_id, reason=reason, actor=actor_account)

