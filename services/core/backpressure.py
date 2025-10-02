"""Async backpressure controller enforcing prioritised intent throughput."""

from __future__ import annotations

import asyncio
import inspect
import itertools
import logging
import os
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from typing import Awaitable, Callable, Deque, Dict, Mapping, MutableMapping, Tuple

from fastapi import FastAPI
from pydantic import BaseModel, Field

from common.schemas.contracts import IntentEvent
from services.common.adapters import KafkaNATSAdapter

LOGGER = logging.getLogger(__name__)

BackpressurePublisher = Callable[[str, int, datetime], Awaitable[None] | None]
QueueItem = Tuple[int, float, int, IntentEvent]

_BACKPRESSURE_TOPIC = "backpressure.events"


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


def _default_publisher(account_id: str, dropped_count: int, ts: datetime) -> None:
    """Publish a backpressure event using the in-memory Kafka/NATS adapter."""

    adapter = KafkaNATSAdapter(account_id=account_id)
    adapter.publish(
        _BACKPRESSURE_TOPIC,
        {
            "type": "backpressure_event",
            "account_id": account_id,
            "dropped_count": dropped_count,
            "ts": ts.isoformat(),
        },
    )


class BackpressureStatus(BaseModel):
    """Snapshot of the current backpressure metrics for observability endpoints."""

    queue_depth: int = Field(..., description="Number of intents currently buffered")
    dropped_intents: Dict[str, int] = Field(
        default_factory=dict,
        description="Mapping of account id → intents dropped due to backpressure",
    )


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

        self._queue: asyncio.PriorityQueue[QueueItem] = asyncio.PriorityQueue(max_queue_size)
        self._maxsize = max_queue_size
        self._max_rate = max_rate_per_account
        self._publisher = publisher or _default_publisher
        self._sequence = itertools.count()
        self._queue_lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()
        self._dropped: Counter[str] = Counter()
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
        drop_counts: Counter[str] = Counter()
        accepted = False

        async with self._queue_lock:
            if not self._queue.full():
                self._queue.put_nowait(new_item)
                accepted = True
            else:
                removed: list[QueueItem] = []
                while True:
                    try:
                        item = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    removed.append(item)
                    self._queue.task_done()

                removed.append(new_item)
                removed.sort()

                keep = removed[: self._maxsize]
                dropped = removed[self._maxsize :]

                for entry in keep:
                    self._queue.put_nowait(entry)

                for entry in dropped:
                    drop_counts[entry[3].account_id] += 1

                accepted = new_item in keep

        if accepted and self._max_rate:
            self._rate_windows[event.account_id].append(now)

        if drop_counts:
            await self._record_drops(drop_counts, ts)

        return accepted

    async def get_intent(self) -> IntentEvent:
        """Retrieve the next intent in priority order."""

        _, _, _, event = await self._queue.get()
        return event

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
        return (-priority, enqueued_at, next(self._sequence), event)

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
        try:
            return max(0, int(raw))
        except (TypeError, ValueError):
            return 1

    async def _record_drops(self, drop_counts: Mapping[str, int], ts: datetime) -> None:
        if not drop_counts:
            return

        async with self._stats_lock:
            for account_id, count in drop_counts.items():
                self._dropped[account_id] += count

        if not self._publisher:
            return

        for account_id, count in drop_counts.items():
            await self._safe_publish(account_id, count, ts)

    async def _safe_publish(self, account_id: str, count: int, ts: datetime) -> None:
        if not self._publisher:
            return
        try:
            result = self._publisher(account_id, count, ts)
            if inspect.isawaitable(result):
                await result
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception(
                "Failed to publish backpressure event for account %s", account_id
            )


DEFAULT_QUEUE_SIZE = _env_int("BACKPRESSURE_QUEUE_SIZE", 256, minimum=1)
DEFAULT_RATE_LIMIT = _env_int("BACKPRESSURE_MAX_RATE", 10, minimum=0)

backpressure_controller = IntentBackpressure(
    max_queue_size=DEFAULT_QUEUE_SIZE,
    max_rate_per_account=DEFAULT_RATE_LIMIT,
)

app = FastAPI(title="Backpressure Controller", version="1.0.0")


@app.get("/backpressure/status", response_model=BackpressureStatus)
async def get_backpressure_status() -> BackpressureStatus:
    """Expose the backpressure queue depth and drop counters."""

    return await backpressure_controller.status()


__all__ = [
    "BackpressureStatus",
    "IntentBackpressure",
    "app",
    "backpressure_controller",
]
