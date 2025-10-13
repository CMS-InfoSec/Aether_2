"""Async backpressure controller enforcing prioritised intent throughput."""

from __future__ import annotations

import asyncio
import bisect
import inspect
import itertools
import logging
import os
import time
from collections import Counter as CollectionCounter, defaultdict, deque
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Deque, Dict, Iterable, Mapping, MutableMapping, ParamSpec, TypeVar, cast

from fastapi import FastAPI

from metrics import setup_metrics

from shared.pydantic_compat import BaseModel, Field
from shared.event_bus import KafkaNATSAdapter

from common.schemas.contracts import IntentEvent
from prometheus_client import Counter as PrometheusCounter, Gauge

LOGGER = logging.getLogger(__name__)

BackpressurePublisher = Callable[[str, int, datetime], Awaitable[None] | None]

_BACKPRESSURE_TOPIC = "backpressure.events"

BACKPRESSURE_QUEUE_DEPTH = Gauge(
    "backpressure_queue_depth",
    "Number of intents currently buffered by the backpressure controller.",
)
BACKPRESSURE_QUEUE_DEPTH.set(0.0)

DROPPED_INTENTS_TOTAL = PrometheusCounter(
    "dropped_intents_total",
    "Total number of intents dropped by the backpressure controller.",
    ["account_id"],
)

P = ParamSpec("P")
R = TypeVar("R")


def _app_get(
    application: FastAPI, *args: Any, **kwargs: Any
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    return cast(
        Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]],
        application.get(*args, **kwargs),
    )


def _env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    """Parse an integer from the environment with optional lower bound."""

    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        LOGGER.warning("Invalid value for %s: %r. Using default %d.", name, raw, default)
        return default
    if minimum is not None:
        value = max(minimum, value)
    return value


class BackpressureEvent(BaseModel):
    """Event emitted when intents are dropped due to queue backpressure."""

    account_id: str = Field(..., description="Account associated with the dropped intents")
    dropped_count: int = Field(..., ge=1, description="Number of intents dropped")
    ts: datetime = Field(..., description="Timestamp when the drop occurred")

    def to_payload(self) -> Dict[str, str | int]:
        """Serialise the event for transport over Kafka/NATS."""

        return {
            "type": "backpressure_event",
            "account_id": self.account_id,
            "dropped_count": self.dropped_count,
            "ts": self.ts.isoformat(),
        }


async def _default_publisher(account_id: str, dropped_count: int, ts: datetime) -> None:
    """Publish a backpressure event using the in-memory Kafka/NATS adapter."""

    adapter = KafkaNATSAdapter(account_id=account_id)
    event = BackpressureEvent(account_id=account_id, dropped_count=dropped_count, ts=ts)
    await adapter.publish(_BACKPRESSURE_TOPIC, event.to_payload())


class BackpressureStatus(BaseModel):
    """Snapshot of the current backpressure metrics for observability endpoints."""

    queue_depth: int = Field(..., description="Number of intents currently buffered")
    dropped_intents: Dict[str, int] = Field(
        default_factory=dict,
        description="Mapping of account id → intents dropped due to backpressure",
    )


@dataclass(order=True)
class QueueItem:
    """Internal representation of queued intents with ordering metadata."""

    sort_key: tuple[int, float, int] = field(init=False, repr=False)
    priority: int = field(compare=False)
    enqueued_at: float = field(compare=False)
    sequence: int = field(compare=False)
    hedge: bool = field(compare=False)
    safe_mode: bool = field(compare=False)
    event: IntentEvent = field(compare=False)

    def __post_init__(self) -> None:
        self.sort_key = (-self.priority, self.enqueued_at, self.sequence)

    def drop_key(self, *, prefer_new: bool) -> tuple[int, int, int, float, int]:
        """Return the key used to decide which items to drop when saturated."""

        is_new = 1 if prefer_new else 0
        protection_level = 2 if self.safe_mode else 1 if self.hedge else 0
        return (
            protection_level,
            self.priority,
            is_new,
            -self.enqueued_at,
            -self.sequence,
        )


class PrioritizedIntentQueue(asyncio.Queue[QueueItem]):
    """Priority queue built on :class:`asyncio.Queue` preserving hedge intents."""

    def _init(self, maxsize: int) -> None:  # pragma: no cover - behaviour exercised indirectly
        self._queue: list[QueueItem] = []

    def _put(self, item: QueueItem) -> None:  # pragma: no cover - thin wrapper
        bisect.insort(self._queue, item)

    def _get(self) -> QueueItem:  # pragma: no cover - thin wrapper
        return self._queue.pop(0)

    def put_with_drop(self, item: QueueItem) -> tuple[bool, list[QueueItem]]:
        """Insert *item* dropping lower priority intents if the queue is full."""

        dropped: list[QueueItem] = []
        if self.full():
            candidates = list(self._queue)
            candidates.append(item)
            drop_list = self._select_drops(candidates, item)
            if item in drop_list:
                return False, []
            for drop in drop_list:
                self._queue.remove(drop)
                unfinished = cast(int, getattr(self, "_unfinished_tasks"))
                setattr(self, "_unfinished_tasks", unfinished - 1)
                dropped.append(drop)
        super().put_nowait(item)
        return True, dropped

    def _select_drops(
        self, candidates: Iterable[QueueItem], new_item: QueueItem
    ) -> list[QueueItem]:
        items = list(candidates)
        excess = len(items) - self.maxsize
        if excess <= 0:
            return []

        non_critical_items = [
            entry for entry in items if not entry.hedge and not entry.safe_mode
        ]
        if not non_critical_items:
            # Queue is saturated with hedges/safe-mode intents. Reject the newcomer to
            # avoid dropping any critical work.
            return [new_item]

        sorted_items = sorted(
            non_critical_items,
            key=lambda entry: entry.drop_key(prefer_new=entry is new_item),
        )

        if len(sorted_items) < excess:
            # Not enough non-critical intents can be dropped to fit the newcomer. Keep
            # the existing critical work and reject the incoming item instead.
            return [new_item]

        return sorted_items[:excess]


class IntentBackpressure:
    """Priority aware backpressure controller with rate limiting and metrics."""

    def __init__(
        self,
        *,
        max_queue_size: int,
        max_rate_per_account: int,
        publisher: BackpressurePublisher | None = None,
    ) -> None:
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be positive")
        if max_rate_per_account < 0:
            raise ValueError("max_rate_per_account cannot be negative")

        self._queue: PrioritizedIntentQueue = PrioritizedIntentQueue(max_queue_size)
        self._maxsize = max_queue_size
        self._max_rate = max_rate_per_account
        self._publisher: BackpressurePublisher = publisher or _default_publisher
        self._sequence = itertools.count()
        self._queue_lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()
        self._dropped: CollectionCounter[str] = CollectionCounter()
        self._rate_windows: MutableMapping[str, Deque[float]] = defaultdict(deque)

    async def enqueue_intent(
        self, event: IntentEvent, *, priority: int | None = None
    ) -> bool:
        """Attempt to enqueue *event* respecting rate limits and queue capacity."""

        resolved_priority = self._resolve_priority(event, priority)
        now = time.monotonic()
        ts = datetime.now(timezone.utc)

        if self._max_rate:
            window = self._rate_windows[event.account_id]
            cutoff = now - 1.0
            while window and window[0] <= cutoff:
                window.popleft()
            if len(window) >= self._max_rate:
                await self._record_drops({event.account_id: 1}, ts)
                return False

        new_item = self._build_item(resolved_priority, now, event)
        drop_counts: CollectionCounter[str] = CollectionCounter()
        accepted = False

        async with self._queue_lock:
            accepted, dropped_items = self._queue.put_with_drop(new_item)
            for entry in dropped_items:
                drop_counts[entry.event.account_id] += 1
            BACKPRESSURE_QUEUE_DEPTH.set(float(self._queue.qsize()))

        if accepted and self._max_rate:
            self._rate_windows[event.account_id].append(now)

        if not accepted:
            drop_counts[event.account_id] += 1

        if drop_counts:
            await self._record_drops(drop_counts, ts)

        return accepted

    async def get_intent(self) -> IntentEvent:
        """Retrieve the next intent in priority order."""

        item = await self._queue.get()
        BACKPRESSURE_QUEUE_DEPTH.set(float(self._queue.qsize()))
        return item.event

    def task_done(self) -> None:
        """Mark the most recently retrieved intent as processed."""

        self._queue.task_done()

    async def status(self) -> BackpressureStatus:
        """Return the queue depth and dropped intent counters."""

        async with self._stats_lock:
            dropped = dict(self._dropped)
        return BackpressureStatus(queue_depth=self._queue.qsize(), dropped_intents=dropped)

    def _build_item(
        self, priority: int, enqueued_at: float, event: IntentEvent
    ) -> QueueItem:
        return QueueItem(
            priority=priority,
            enqueued_at=enqueued_at,
            sequence=next(self._sequence),
            hedge=self._is_hedge(event),
            safe_mode=self._is_safe_mode(event),
            event=event,
        )

    def _resolve_priority(self, event: IntentEvent, explicit: int | None) -> int:
        if explicit is not None:
            try:
                return max(0, int(explicit))
            except (TypeError, ValueError):
                LOGGER.debug("Invalid explicit priority %r, falling back to payload", explicit)
        payload = event.intent or {}
        raw = payload.get("priority") or payload.get("intent_priority")
        if isinstance(raw, str):
            mapping = {"high": 3, "medium": 2, "normal": 1, "low": 0}
            return mapping.get(raw.lower(), 1)
        if isinstance(raw, bool):
            return 1 if raw else 0
        if isinstance(raw, (int, float)):
            return max(0, int(raw))
        if raw is None:
            return 1
        try:
            return max(0, int(str(raw)))
        except (TypeError, ValueError):
            return 1

    def _is_hedge(self, event: IntentEvent) -> bool:
        payload = event.intent or {}
        hedge_flags = (
            payload.get("hedge"),
            payload.get("is_hedge"),
            payload.get("hedging"),
        )
        if any(isinstance(flag, bool) and flag for flag in hedge_flags):
            return True

        for key in ("intent_type", "type", "category", "strategy", "purpose"):
            value = payload.get(key)
            if isinstance(value, str) and value.lower() in {"hedge", "hedging"}:
                return True
        return False

    def _is_safe_mode(self, event: IntentEvent) -> bool:
        payload = event.intent or {}

        bool_flags = (
            payload.get("safe_mode"),
            payload.get("safe-mode"),
            payload.get("safeMode"),
        )
        if any(isinstance(flag, bool) and flag for flag in bool_flags):
            return True

        textual_fields = (
            payload.get("mode"),
            payload.get("state"),
            payload.get("intent_type"),
            payload.get("type"),
            payload.get("category"),
            payload.get("reason"),
        )
        for value in textual_fields:
            if isinstance(value, str):
                normalized = value.replace("-", "_").lower()
                if normalized in {"safe_mode", "safe", "safemode"}:
                    return True
        return False

    async def _record_drops(self, drop_counts: Mapping[str, int], ts: datetime) -> None:
        if not drop_counts:
            return

        async with self._stats_lock:
            for account_id, count in drop_counts.items():
                self._dropped[account_id] += count
                DROPPED_INTENTS_TOTAL.labels(account_id=account_id).inc(count)

        for account_id, count in drop_counts.items():
            await self._safe_publish(account_id, count, ts)

    async def _safe_publish(self, account_id: str, count: int, ts: datetime) -> None:
        try:
            result = self._publisher(account_id, count, ts)
            if inspect.isawaitable(result):
                await result
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception(
                "Failed to publish backpressure event for account %s", account_id
            )


DEFAULT_QUEUE_SIZE = 500
DEFAULT_RATE_LIMIT = _env_int("BACKPRESSURE_MAX_RATE", 10, minimum=0)

backpressure_controller = IntentBackpressure(
    max_queue_size=DEFAULT_QUEUE_SIZE,
    max_rate_per_account=DEFAULT_RATE_LIMIT,
)

app = FastAPI(title="Backpressure Controller", version="1.0.0")
setup_metrics(app, service_name="backpressure-controller")


@_app_get(app, "/backpressure/status", response_model=BackpressureStatus)
async def get_backpressure_status() -> BackpressureStatus:
    """Expose the backpressure queue depth and drop counters."""

    return await backpressure_controller.status()


__all__ = [
    "BackpressureEvent",
    "BackpressureStatus",
    "IntentBackpressure",
    "app",
    "backpressure_controller",
]
