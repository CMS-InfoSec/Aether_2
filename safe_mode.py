"""Safe mode FastAPI service for orchestrating risk-off controls."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
import logging
from typing import Dict, List, Optional, Union

from fastapi import Body, FastAPI, Header, HTTPException, Request, status

from metrics import increment_safe_mode_triggers, setup_metrics


try:  # pragma: no cover - optional audit dependency
    from common.utils.audit_logger import hash_ip as audit_hash_ip, log_audit as chain_log_audit
except Exception:  # pragma: no cover - degrade gracefully
    chain_log_audit = None  # type: ignore[assignment]

    def audit_hash_ip(_: Optional[str]) -> Optional[str]:  # type: ignore[override]
        return None


LOGGER = logging.getLogger(__name__)


app = FastAPI(title="Safe Mode Service")
setup_metrics(app, service_name="safe-mode")


# ---------------------------------------------------------------------------
# Support data structures
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    """Return a timezone aware ``datetime`` in UTC."""

    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class SafeModeEvent:
    """Represents a transition into or out of safe mode."""

    reason: str
    ts: datetime
    state: str
    actor: Optional[str]

    def to_payload(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "reason": self.reason,
            "timestamp": self.ts.isoformat(),
            "state": self.state,
        }
        if self.actor is not None:
            payload["actor"] = self.actor
        return payload


@dataclass
class SafeModeStatus:
    """Represents the current safe mode state."""

    active: bool
    reason: Optional[str] = None
    since: Optional[datetime] = None
    actor: Optional[str] = None

    def to_response(self) -> Dict[str, object]:
        return {
            "active": self.active,
            "reason": self.reason,
            "since": self.since.isoformat() if self.since else None,
            "actor": self.actor,
        }


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SafeModePersistedState:
    """Serializable representation of the safe mode state."""

    active: bool
    reason: Optional[str]
    timestamp: Optional[datetime]

    def to_dict(self) -> Dict[str, object]:
        return {
            "active": self.active,
            "reason": self.reason,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
        }

    @staticmethod
    def from_dict(payload: Dict[str, object]) -> "SafeModePersistedState":
        raw_ts = payload.get("timestamp")
        ts: Optional[datetime]
        if isinstance(raw_ts, str):
            try:
                ts = datetime.fromisoformat(raw_ts)
            except ValueError:
                ts = None
        else:
            ts = None
        return SafeModePersistedState(
            active=bool(payload.get("active", False)),
            reason=payload.get("reason") if isinstance(payload.get("reason"), str) else None,
            timestamp=ts,
        )


class SafeModeStateStore:
    """Persist safe mode state to disk for crash recovery."""

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self._path = Path(path) if path is not None else Path("safe_mode_state.json")
        self._lock = Lock()

    def load(self) -> SafeModePersistedState:
        with self._lock:
            if not self._path.exists():
                return SafeModePersistedState(active=False, reason=None, timestamp=None)
            try:
                payload = json.loads(self._path.read_text())
            except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                return SafeModePersistedState(active=False, reason=None, timestamp=None)
            if not isinstance(payload, dict):
                return SafeModePersistedState(active=False, reason=None, timestamp=None)
            return SafeModePersistedState.from_dict(payload)

    def save(self, state: SafeModePersistedState) -> None:
        with self._lock:
            try:
                if self._path.parent and not self._path.parent.exists():
                    self._path.parent.mkdir(parents=True, exist_ok=True)
                self._path.write_text(json.dumps(state.to_dict()))
            except OSError:
                return

    def clear(self) -> None:
        with self._lock:
            try:
                self._path.unlink()
            except FileNotFoundError:
                return
            except OSError:
                return


# ---------------------------------------------------------------------------
# Safe mode logging helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SafeModeLogEntry:
    reason: str
    ts: datetime
    actor: Optional[str]
    state: str


_SAFE_MODE_LOG: List[SafeModeLogEntry] = []


def safe_mode_log(reason: str, ts: datetime, actor: Optional[str], state: str) -> None:
    """Record a safe mode state transition in memory."""

    _SAFE_MODE_LOG.append(SafeModeLogEntry(reason=reason, ts=ts, actor=actor, state=state))


def get_safe_mode_log() -> List[Dict[str, object]]:
    """Return a serialisable copy of the log for diagnostics/testing."""

    return [
        {
            "reason": entry.reason,
            "timestamp": entry.ts,
            "actor": entry.actor,
            "state": entry.state,
        }
        for entry in _SAFE_MODE_LOG
    ]


def clear_safe_mode_log() -> None:
    """Reset the in-memory log (useful for tests)."""

    _SAFE_MODE_LOG.clear()


# ---------------------------------------------------------------------------
# Trading control primitives
# ---------------------------------------------------------------------------


class OrderControls:
    """Minimal representation of trading order controls."""

    def __init__(self) -> None:
        self.open_orders: List[str] = []
        self.cancelled_orders: List[str] = []
        self.hedging_only: bool = False

    def cancel_open_orders(self) -> None:
        self.cancelled_orders.extend(self.open_orders)
        self.open_orders.clear()

    def restrict_to_hedging(self) -> None:
        self.hedging_only = True

    def lift_restrictions(self) -> None:
        self.hedging_only = False

    def reset(self) -> None:
        self.open_orders.clear()
        self.cancelled_orders.clear()
        self.hedging_only = False


class IntentGuard:
    """Tracks whether trading intents can be generated."""

    def __init__(self) -> None:
        self.allow_new_intents: bool = True

    def disable(self) -> None:
        self.allow_new_intents = False

    def enable(self) -> None:
        self.allow_new_intents = True

    def reset(self) -> None:
        self.allow_new_intents = True

    def assert_allowed(self) -> None:
        if not self.allow_new_intents:
            raise RuntimeError("Safe mode active; new trading intents are blocked")


class KafkaSafeModePublisher:
    """Publish safe mode events to Kafka (via the in-memory adapter)."""

    def __init__(self, *, account_id: str = "safe_mode", topic: str = "ops.safe_mode") -> None:
        self._account_id = account_id
        self._topic = topic
        self._history: List[Dict[str, object]] = []

    def publish(self, event: SafeModeEvent) -> None:
        payload = event.to_payload()
        try:  # pragma: no cover - adapter import may fail in lightweight environments
            from services.common.adapters import KafkaNATSAdapter  # type: ignore

            KafkaNATSAdapter(account_id=self._account_id).publish(
                topic=self._topic,
                payload=payload,
            )
        except Exception:  # pragma: no cover - fall back to in-memory history
            pass

        self._history.append({"topic": self._topic, "payload": payload})

    def history(self) -> List[Dict[str, object]]:
        return list(self._history)

    def reset(self) -> None:
        self._history.clear()


class SafeModeLogger:
    """Glue layer around ``safe_mode_log`` for dependency injection."""

    @staticmethod
    def log(event: SafeModeEvent) -> None:
        safe_mode_log(event.reason, event.ts, event.actor, event.state)


# ---------------------------------------------------------------------------
# Safe mode controller
# ---------------------------------------------------------------------------


@dataclass
class _SafeModeInternalState:
    active: bool = False
    reason: Optional[str] = None
    since: Optional[datetime] = None
    actor: Optional[str] = None


class SafeModeController:
    """Coordinates safe mode transitions and downstream side-effects."""

    def __init__(
        self,
        *,
        order_controls: Optional[OrderControls] = None,
        intent_guard: Optional[IntentGuard] = None,
        publisher: Optional[KafkaSafeModePublisher] = None,
        logger: Optional[SafeModeLogger] = None,
        state_store: Optional[SafeModeStateStore] = None,
    ) -> None:
        self.order_controls = order_controls or OrderControls()
        self.intent_guard = intent_guard or IntentGuard()
        self._publisher = publisher or KafkaSafeModePublisher()
        self._logger = logger or SafeModeLogger()
        self._state_store = state_store or SafeModeStateStore()
        persisted = self._state_store.load()
        self._state = _SafeModeInternalState(
            active=persisted.active,
            reason=persisted.reason,
            since=persisted.timestamp,
        )
        if persisted.active:
            self.intent_guard.disable()
            self.order_controls.restrict_to_hedging()
        self._lock = Lock()

    # Public API ---------------------------------------------------------

    def enter(self, *, reason: str, actor: Optional[str]) -> SafeModeEvent:
        normalized_reason = reason.strip()
        if not normalized_reason:
            raise ValueError("reason must not be empty")

        ts = _utcnow()
        entered = False
        with self._lock:
            if self._state.active:
                self.intent_guard.disable()
                self.order_controls.restrict_to_hedging()
                self._state_store.save(
                    SafeModePersistedState(
                        active=True,
                        reason=self._state.reason or normalized_reason,
                        timestamp=self._state.since or ts,
                    )
                )
                return SafeModeEvent(
                    reason=self._state.reason or normalized_reason,
                    ts=self._state.since or ts,
                    state="entered",
                    actor=self._state.actor,
                )

            self.order_controls.cancel_open_orders()
            self.order_controls.restrict_to_hedging()
            self.intent_guard.disable()
            self._state = _SafeModeInternalState(
                active=True,
                reason=normalized_reason,
                since=ts,
                actor=actor,
            )

            entered = True


        event = SafeModeEvent(reason=normalized_reason, ts=ts, state="entered", actor=actor)
        if entered:
            increment_safe_mode_triggers(normalized_reason)
        self._publisher.publish(event)
        self._logger.log(event)
        return event

    def exit(self, *, actor: Optional[str]) -> SafeModeEvent:
        ts = _utcnow()
        with self._lock:
            if not self._state.active:
                raise RuntimeError("Safe mode is not currently active")

            reason = self._state.reason or "manual_exit"
            self.intent_guard.enable()
            self.order_controls.lift_restrictions()
            self._state = _SafeModeInternalState(active=False, actor=actor)
            self._state_store.save(
                SafeModePersistedState(active=False, reason=None, timestamp=None)
            )

        event = SafeModeEvent(reason=reason, ts=ts, state="exited", actor=actor)
        self._publisher.publish(event)
        self._logger.log(event)
        return event

    def status(self) -> SafeModeStatus:
        with self._lock:
            return SafeModeStatus(
                active=self._state.active,
                reason=self._state.reason,
                since=self._state.since,
                actor=self._state.actor,
            )

    def reset(self) -> None:
        with self._lock:
            self._state = _SafeModeInternalState()
        self.order_controls.reset()
        self.intent_guard.reset()
        if hasattr(self._publisher, "reset"):
            self._publisher.reset()
        self._state_store.clear()

    def guard_new_intent(self) -> None:
        """Raise if new trading intents should not be produced."""

        self.intent_guard.assert_allowed()

    def kafka_history(self) -> List[Dict[str, object]]:
        if hasattr(self._publisher, "history"):
            return self._publisher.history()
        return []


controller = SafeModeController()


# ---------------------------------------------------------------------------
# FastAPI endpoints
# ---------------------------------------------------------------------------


@app.post("/safe_mode/enter", response_model=Dict[str, object])
def enter_safe_mode(
    request: Request,
    payload: Dict[str, str] = Body(...),
    actor: Optional[str] = Header(default="system", alias="X-Actor"),
) -> Dict[str, object]:
    before_snapshot = dict(controller.status().to_response())
    reason = payload.get("reason", "").strip()
    try:
        controller.enter(reason=reason, actor=actor)
    except ValueError as exc:  # invalid reason
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc
    response = controller.status().to_response()

    if chain_log_audit is not None:
        try:
            ip_hash = audit_hash_ip(request.client.host if request.client else None)
            chain_log_audit(
                actor=actor or "system",
                action="safe_mode.enter",
                entity="safe_mode",
                before=before_snapshot,
                after=dict(response),
                ip_hash=ip_hash,
            )
        except Exception:  # pragma: no cover - defensive best effort
            LOGGER.exception("Failed to record audit log for safe mode entry")

    return response


@app.post("/safe_mode/exit", response_model=Dict[str, object])
def exit_safe_mode(
    request: Request,
    actor: Optional[str] = Header(default="system", alias="X-Actor"),
) -> Dict[str, object]:
    before_snapshot = dict(controller.status().to_response())
    try:
        controller.exit(actor=actor)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    response = controller.status().to_response()

    if chain_log_audit is not None:
        try:
            ip_hash = audit_hash_ip(request.client.host if request.client else None)
            chain_log_audit(
                actor=actor or "system",
                action="safe_mode.exit",
                entity="safe_mode",
                before=before_snapshot,
                after=dict(response),
                ip_hash=ip_hash,
            )
        except Exception:  # pragma: no cover - defensive best effort
            LOGGER.exception("Failed to record audit log for safe mode exit")

    return response


@app.get("/safe_mode/status", response_model=Dict[str, object])
def safe_mode_status() -> Dict[str, object]:
    return controller.status().to_response()
