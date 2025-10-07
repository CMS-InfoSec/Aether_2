"""Safe mode FastAPI service for orchestrating risk-off controls."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
import logging
import os
import sys
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

try:  # pragma: no cover - FastAPI is optional in some test environments
    from fastapi import Body, Depends, FastAPI, HTTPException, Request, status
except ImportError:  # pragma: no cover - fallback when FastAPI is stubbed out
    from services.common.fastapi_stub import (  # type: ignore[misc]
        Body,
        Depends,
        FastAPI,
        HTTPException,
        Request,
        status,
    )

from metrics import increment_safe_mode_triggers, setup_metrics
from services.common.security import require_admin_account
from common.utils.redis import create_redis_from_url
from shared.async_utils import dispatch_async
from shared.audit_hooks import AuditEvent, load_audit_hooks, log_audit_event_with_fallback


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
    """Persist safe mode state using a shared Redis key for crash recovery."""

    _DEFAULT_KEY = "safe-mode:state"

    def __init__(self, redis_client: Optional[Any] = None, *, key: str = _DEFAULT_KEY) -> None:
        self._lock = Lock()
        self._key = key
        self._redis = redis_client or self._create_default_client()

    @staticmethod
    def _create_default_client() -> Any:
        allow_stub = "pytest" in sys.modules

        raw_url = os.getenv("SAFE_MODE_REDIS_URL")
        if raw_url is None or not raw_url.strip():
            if allow_stub:
                raw_url = "redis://localhost:6379/0"
            else:
                raise RuntimeError(
                    "SAFE_MODE_REDIS_URL environment variable must be set before starting the safe mode service"
                )

        redis_url = raw_url.strip()
        client, used_stub = create_redis_from_url(redis_url, decode_responses=True, logger=LOGGER)

        if used_stub and not allow_stub:
            raise RuntimeError(
                "Failed to connect to Redis at SAFE_MODE_REDIS_URL; safe mode requires a reachable Redis instance"
            )

        return client

    def load(self) -> SafeModePersistedState:
        with self._lock:
            try:
                raw_state = self._redis.get(self._key)
            except Exception as exc:  # pragma: no cover - best effort resilience if Redis is unavailable
                LOGGER.warning("Failed to load safe mode state from Redis: %s", exc)
                return SafeModePersistedState(active=False, reason=None, timestamp=None)

            if raw_state is None:
                return SafeModePersistedState(active=False, reason=None, timestamp=None)

            if isinstance(raw_state, bytes):
                try:
                    raw_state = raw_state.decode("utf-8")
                except UnicodeDecodeError:
                    return SafeModePersistedState(active=False, reason=None, timestamp=None)

            try:
                payload = json.loads(raw_state)
            except (TypeError, json.JSONDecodeError):
                return SafeModePersistedState(active=False, reason=None, timestamp=None)

            if not isinstance(payload, dict):
                return SafeModePersistedState(active=False, reason=None, timestamp=None)

            return SafeModePersistedState.from_dict(payload)

    def save(self, state: SafeModePersistedState) -> None:
        serialized = json.dumps(state.to_dict())
        with self._lock:
            try:
                self._redis.set(self._key, serialized)
            except Exception as exc:  # pragma: no cover - best effort resilience if Redis is unavailable
                LOGGER.warning("Failed to persist safe mode state to Redis: %s", exc)

    def clear(self) -> None:
        with self._lock:
            try:
                self._redis.delete(self._key)
            except Exception as exc:  # pragma: no cover - best effort resilience if Redis is unavailable
                LOGGER.warning("Failed to clear safe mode state in Redis: %s", exc)


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


@dataclass(frozen=True)
class _OpenOrder:
    client_id: str
    exchange_order_id: str


class OrderControls:
    """Production order controls backed by OMS/Timescale adapters."""

    _DEFAULT_ACCOUNTS: Sequence[str] = ("company",)

    def __init__(
        self,
        *,
        account_ids: Optional[Sequence[str]] = None,
        exchange_factory: Optional[Callable[[str], "ExchangeAdapter"]] = None,
        timescale_factory: Optional[Callable[[str], "TimescaleAdapter"]] = None,
        order_snapshot_loader: Optional[
            Callable[[str, "TimescaleAdapter"], Iterable[Mapping[str, Any]] | Mapping[str, Any] | None]
        ] = None,
    ) -> None:
        from exchange_adapter import KrakenAdapter, ExchangeAdapter
        from services.common.adapters import TimescaleAdapter

        self._logger = LOGGER.getChild("OrderControls")
        self.open_orders: List[str] = []
        self.cancelled_orders: List[str] = []
        self.hedging_only: bool = False
        # ``only_hedging`` is kept for backwards compatibility with legacy callers.
        self.only_hedging: bool = False
        self._last_reason: Optional[str] = None

        accounts = self._resolve_accounts(account_ids)
        self._accounts: Sequence[str] = accounts

        exchange_factory = exchange_factory or (lambda _: KrakenAdapter())
        timescale_factory = timescale_factory or (
            lambda account: TimescaleAdapter(account_id=account)
        )

        self._exchange_adapters: Dict[str, ExchangeAdapter] = {}
        for account in accounts:
            try:
                self._exchange_adapters[account] = exchange_factory(account)
            except Exception:
                self._logger.exception(
                    "Failed to initialise exchange adapter for safe mode",
                    extra={"account_id": account},
                )
        self._timescale_adapters: Dict[str, Optional[TimescaleAdapter]] = {}
        for account in accounts:
            try:
                self._timescale_adapters[account] = timescale_factory(account)
            except Exception:
                self._logger.exception(
                    "Failed to initialise Timescale adapter for safe mode",
                    extra={"account_id": account},
                )
                self._timescale_adapters[account] = None

        if order_snapshot_loader is None:
            self._order_snapshot_loader = self._default_snapshot_loader
        else:
            self._order_snapshot_loader = order_snapshot_loader

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def cancel_open_orders(self) -> None:
        cancelled: List[str] = []
        for account in self._accounts:
            orders = self._load_open_orders(account)
            if not orders:
                continue
            captured = [order.client_id for order in orders]
            self.open_orders.extend(captured)
            for order in orders:
                try:
                    self._execute_cancel(account, order)
                    cancelled.append(order.client_id)
                except Exception:
                    self._logger.exception(
                        "Failed to cancel order during safe mode entry",
                        extra={
                            "account_id": account,
                            "client_id": order.client_id,
                            "exchange_order_id": order.exchange_order_id,
                        },
                    )
        if cancelled:
            self.cancelled_orders.extend(cancelled)
        self.open_orders.clear()

    def restrict_to_hedging(self, *, reason: Optional[str] = None, actor: Optional[str] = None) -> None:
        self.hedging_only = True
        self.only_hedging = True
        self._last_reason = reason or self._last_reason
        self._set_safe_mode_state(True, reason=reason, actor=actor)

    def lift_restrictions(self, *, reason: Optional[str] = None, actor: Optional[str] = None) -> None:
        self.hedging_only = False
        self.only_hedging = False
        release_reason = reason or self._last_reason
        self._set_safe_mode_state(False, reason=release_reason, actor=actor)

    def reset(self) -> None:
        self.open_orders.clear()
        self.cancelled_orders.clear()
        self.hedging_only = False
        self.only_hedging = False
        self._last_reason = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _resolve_accounts(self, provided: Optional[Sequence[str]]) -> Sequence[str]:
        if provided:
            return tuple(dict.fromkeys(account.strip() for account in provided if account.strip()))
        env_accounts = os.getenv("SAFE_MODE_ACCOUNTS")
        if env_accounts:
            parsed = [account.strip() for account in env_accounts.split(",") if account.strip()]
            if parsed:
                return tuple(dict.fromkeys(parsed))
        env_single = os.getenv("SAFE_MODE_ACCOUNT_ID")
        if env_single:
            normalized = env_single.strip()
            if normalized:
                return (normalized,)
        return tuple(self._DEFAULT_ACCOUNTS)

    def _default_snapshot_loader(
        self, account_id: str, timescale: "TimescaleAdapter"
    ) -> Iterable[Mapping[str, Any]] | Mapping[str, Any] | None:
        try:
            events = timescale.events()
        except Exception:
            self._logger.exception(
                "Failed to fetch Timescale events for safe mode order snapshot",
                extra={"account_id": account_id},
            )
            return None
        acks = events.get("acks") if isinstance(events, dict) else None
        if not isinstance(acks, list):
            return None
        for entry in reversed(acks):
            if isinstance(entry, Mapping):
                open_orders = entry.get("open_orders")
                if open_orders:
                    return open_orders
        return None

    def _load_open_orders(self, account_id: str) -> List[_OpenOrder]:
        timescale = self._timescale_adapters.get(account_id)
        if not timescale:
            return []
        snapshot = self._order_snapshot_loader(account_id, timescale)
        return self._normalize_orders(snapshot)

    def _normalize_orders(
        self, snapshot: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None
    ) -> List[_OpenOrder]:
        if snapshot is None:
            return []
        if isinstance(snapshot, Mapping):
            candidates: Iterable[Any] = snapshot.values()
        elif isinstance(snapshot, Iterable) and not isinstance(snapshot, (str, bytes)):
            candidates = snapshot
        else:
            return []

        orders: List[_OpenOrder] = []
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                continue
            client_id = self._extract_client_id(candidate)
            exchange_id = self._extract_exchange_id(candidate)
            if not client_id or not exchange_id:
                continue
            orders.append(_OpenOrder(client_id=client_id, exchange_order_id=exchange_id))
        return orders

    def _extract_client_id(self, payload: Mapping[str, Any]) -> Optional[str]:
        keys = ("clientOrderId", "client_order_id", "client_id", "userref", "order_id", "id")
        for key in keys:
            value = payload.get(key)
            if value is not None:
                return str(value)
        nested = payload.get("order")
        if isinstance(nested, Mapping):
            for key in keys:
                value = nested.get(key)
                if value is not None:
                    return str(value)
        return None

    def _extract_exchange_id(self, payload: Mapping[str, Any]) -> Optional[str]:
        keys = ("ordertxid", "txid", "order_id", "orderid", "id")
        for key in keys:
            value = payload.get(key)
            if value is not None:
                return str(value)
        nested = payload.get("order")
        if isinstance(nested, Mapping):
            for key in keys:
                value = nested.get(key)
                if value is not None:
                    return str(value)
        return None

    def _execute_cancel(self, account_id: str, order: _OpenOrder) -> None:
        adapter = self._exchange_adapters.get(account_id)
        if adapter is None:
            return
        try:
            coroutine = adapter.cancel_order(
                account_id,
                order.client_id,
                exchange_order_id=order.exchange_order_id,
            )
        except Exception:
            self._logger.exception(
                "Failed to prepare safe mode cancel coroutine",
                extra={
                    "account_id": account_id,
                    "client_id": order.client_id,
                    "exchange_order_id": order.exchange_order_id,
                },
            )
            return
        dispatch_async(
            coroutine,
            context="safe_mode.cancel_order",
            logger=self._logger,
        )

    def _set_safe_mode_state(
        self, engaged: bool, *, reason: Optional[str], actor: Optional[str]
    ) -> None:
        for account, adapter in self._timescale_adapters.items():
            if adapter is None:
                continue
            try:
                adapter.set_safe_mode(engaged=engaged, reason=reason, actor=actor)
            except Exception:
                self._logger.exception(
                    "Failed to update safe mode flag in Timescale",
                    extra={"account_id": account, "engaged": engaged, "reason": reason},
                )


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
        except Exception:  # pragma: no cover - fall back to in-memory history
            LOGGER.debug(
                "Kafka adapter unavailable for safe mode publish", exc_info=True
            )
        else:
            try:
                dispatch_async(
                    KafkaNATSAdapter(account_id=self._account_id).publish(
                        topic=self._topic,
                        payload=payload,
                    ),
                    context="safe_mode.kafka_publish",
                    logger=LOGGER,
                )
            except Exception:  # pragma: no cover - defensive scheduling guard
                LOGGER.exception("Failed to schedule safe mode publish task")

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
            self.order_controls.restrict_to_hedging(
                reason=persisted.reason,
                actor=self._state.actor,
            )
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
                self.order_controls.restrict_to_hedging(
                    reason=self._state.reason or normalized_reason,
                    actor=self._state.actor,
                )
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
            self.order_controls.restrict_to_hedging(
                reason=normalized_reason,
                actor=actor,
            )
            self.intent_guard.disable()
            self._state = _SafeModeInternalState(
                active=True,
                reason=normalized_reason,
                since=ts,
                actor=actor,
            )
            self._state_store.save(
                SafeModePersistedState(
                    active=True,
                    reason=normalized_reason,
                    timestamp=ts,
                )
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
            self.order_controls.lift_restrictions(
                reason=reason,
                actor=actor,
            )
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
    actor_account: str = Depends(require_admin_account),
) -> Dict[str, object]:
    before_snapshot = dict(controller.status().to_response())
    reason = payload.get("reason", "").strip()
    try:
        controller.enter(reason=reason, actor=actor_account)
    except ValueError as exc:  # invalid reason
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc
    response = controller.status().to_response()

    audit_hooks = load_audit_hooks()
    event = AuditEvent(
        actor=actor_account,
        action="safe_mode.enter",
        entity="safe_mode",
        before=before_snapshot,
        after=dict(response),
        ip_address=request.client.host if request.client else None,
    )
    log_audit_event_with_fallback(
        audit_hooks,
        LOGGER,
        event,
        failure_message="Failed to record audit log for safe mode entry",
        disabled_message="Audit logging disabled; skipping safe_mode.enter",
    )

    return response


@app.post("/safe_mode/exit", response_model=Dict[str, object])
def exit_safe_mode(
    request: Request,
    actor_account: str = Depends(require_admin_account),
) -> Dict[str, object]:
    before_snapshot = dict(controller.status().to_response())
    try:
        controller.exit(actor=actor_account)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    response = controller.status().to_response()

    audit_hooks = load_audit_hooks()
    event = AuditEvent(
        actor=actor_account,
        action="safe_mode.exit",
        entity="safe_mode",
        before=before_snapshot,
        after=dict(response),
        ip_address=request.client.host if request.client else None,
    )
    log_audit_event_with_fallback(
        audit_hooks,
        LOGGER,
        event,
        failure_message="Failed to record audit log for safe mode exit",
        disabled_message="Audit logging disabled; skipping safe_mode.exit",
    )

    return response


@app.get("/safe_mode/status", response_model=Dict[str, object])
def safe_mode_status() -> Dict[str, object]:
    return controller.status().to_response()
