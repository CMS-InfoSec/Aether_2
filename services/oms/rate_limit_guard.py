"""Cooperative rate limiter for Kraken OMS transports."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, MutableMapping, Optional, Tuple

from metrics import Counter, Gauge, _REGISTRY

LOGGER = logging.getLogger(__name__)

_API_CALLS_TOTAL = Counter(
    "api_calls_total",
    "Total number of Kraken API calls issued by the OMS.",
    ["endpoint"],
    registry=_REGISTRY,
)

_RATE_LIMIT_REMAINING = Gauge(
    "rate_limit_remaining",
    "Remaining Kraken API capacity per account and transport.",
    ["account", "transport"],
    registry=_REGISTRY,
)


@dataclass(slots=True)
class _LimiterConfig:
    """Configuration describing an exchange imposed rate limit."""

    limit: int
    window: float
    soft_ratio: float

    @property
    def min_spacing(self) -> float:
        return self.window / self.limit if self.limit > 0 else 0.0

    @property
    def soft_threshold(self) -> int:
        if self.limit <= 0:
            return 0
        threshold = int(self.limit * self.soft_ratio)
        if threshold >= self.limit:
            threshold = max(self.limit - 1, 0)
        return threshold


class RateLimitGuard:
    """Coordinate Kraken API usage across REST and WebSocket transports.

    The guard cooperatively queues requests when approaching Kraken's
    documented rate limits (20 REST calls / 10 seconds and 60 WebSocket
    actions / 10 seconds per key). Non-urgent traffic is delayed when the
    account approaches saturation while cancel and hedge requests are
    prioritised.
    """

    def __init__(
        self,
        *,
        rest_limit: int = 20,
        rest_window: float = 10.0,
        ws_limit: int = 60,
        ws_window: float = 10.0,
        soft_ratio: float = 0.8,
        sleep_floor: float = 0.05,
    ) -> None:
        if not (0 < soft_ratio <= 1):
            raise ValueError("soft_ratio must be within (0, 1]")

        self._limits: Dict[str, _LimiterConfig] = {
            "rest": _LimiterConfig(rest_limit, rest_window, soft_ratio),
            "websocket": _LimiterConfig(ws_limit, ws_window, soft_ratio),
        }
        self._usage: MutableMapping[Tuple[str, str], Deque[float]] = defaultdict(deque)
        self._locks: Dict[Tuple[str, str], asyncio.Lock] = {}
        self._waiter_counts: Dict[Tuple[str, str], int] = defaultdict(int)
        self._waiter_lock = asyncio.Lock()
        self._sleep_floor = sleep_floor

    async def acquire(
        self,
        account_id: str,
        endpoint: str,
        *,
        transport: str = "rest",
        urgent: bool = False,
    ) -> None:
        """Reserve a slot for the requested API call.

        The coroutine waits until capacity is available. When utilisation
        exceeds the configured soft threshold, non-urgent callers are backed
        off to smooth utilisation. Once the rate limit is exhausted requests
        are queued until the window resets.
        """

        transport_key = self._normalise_transport(transport)
        limiter = self._limits[transport_key]
        key = (account_id, transport_key)
        lock = self._locks.setdefault(key, asyncio.Lock())

        while True:
            wait_reason: Optional[str] = None
            wait_delay = 0.0
            pending_after = 0
            count_snapshot = 0

            async with lock:
                bucket = self._usage.setdefault(key, deque())
                now = time.monotonic()
                self._prune(bucket, now, limiter.window)
                count_snapshot = len(bucket)

                remaining = max(limiter.limit - count_snapshot, 0)
                _RATE_LIMIT_REMAINING.labels(
                    account=account_id, transport=transport_key
                ).set(remaining)

                if count_snapshot < limiter.limit:
                    if not urgent and count_snapshot >= limiter.soft_threshold:
                        wait_reason = "soft"
                        wait_delay = max(limiter.min_spacing, self._sleep_floor)
                    else:
                        bucket.append(now)
                        remaining_after = limiter.limit - len(bucket)
                        _RATE_LIMIT_REMAINING.labels(
                            account=account_id, transport=transport_key
                        ).set(max(remaining_after, 0))
                        _API_CALLS_TOTAL.labels(endpoint=endpoint).inc()
                        return
                else:
                    wait_reason = "hard"
                    oldest = bucket[0]
                    wait_delay = max(oldest + limiter.window - now, limiter.min_spacing)
                    wait_delay = max(wait_delay, self._sleep_floor)
                    _RATE_LIMIT_REMAINING.labels(
                        account=account_id, transport=transport_key
                    ).set(0)

            if wait_reason is None:
                # Defensive fallback when no explicit reason was recorded.
                wait_reason = "soft"
                wait_delay = max(limiter.min_spacing, self._sleep_floor)

            message = (
                "Rate limit queue for account %s transport=%s endpoint=%s reason=%s "
                "delay=%.3fs usage=%s/%s"
            )
            log_args = (account_id, transport_key, endpoint, wait_reason, wait_delay, count_snapshot, limiter.limit)
            if wait_reason == "hard" and not urgent:
                LOGGER.warning(message, *log_args)
            else:
                LOGGER.debug(message, *log_args)

            pending_after = await self._sleep_with_waiters(key, wait_delay)
            if wait_reason == "hard" and urgent and pending_after > 0:
                # Yield momentarily to give urgent callers priority after waking up.
                await asyncio.sleep(self._sleep_floor)

    async def release(
        self,
        account_id: str,
        *,
        transport: str = "rest",
        successful: bool = True,
        remaining: Optional[int] = None,
    ) -> None:
        """Record the completion of a guarded call.

        When ``successful`` is ``False`` the reservation created during
        :meth:`acquire` is released immediately to avoid penalising failed
        transports.  Callers may optionally provide Kraken's reported
        ``remaining`` capacity which, when supplied, is used to update the
        exported gauge metrics.
        """

        transport_key = self._normalise_transport(transport)
        limiter = self._limits[transport_key]
        key = (account_id, transport_key)
        lock = self._locks.get(key)

        if lock is None:
            current = self._coerce_remaining(remaining, limiter.limit)
            _RATE_LIMIT_REMAINING.labels(account=account_id, transport=transport_key).set(current)
            return

        async with lock:
            bucket = self._usage.get(key)
            now = time.monotonic()
            if bucket is not None:
                self._prune(bucket, now, limiter.window)
                if not successful and bucket:
                    bucket.pop()

                current_remaining = limiter.limit - len(bucket)
            else:
                current_remaining = limiter.limit

            if remaining is not None:
                current_remaining = self._coerce_remaining(remaining, limiter.limit)
            else:
                current_remaining = max(current_remaining, 0)

            _RATE_LIMIT_REMAINING.labels(account=account_id, transport=transport_key).set(current_remaining)

    async def status(self, account_id: Optional[str] = None) -> Dict[str, Dict[str, Dict[str, float | int]]]:
        """Return a snapshot of remaining capacity per account."""

        async with self._waiter_lock:
            waiters_snapshot = dict(self._waiter_counts)

        if account_id is None:
            accounts = {key[0] for key in self._usage.keys()}
        else:
            accounts = {account_id}

        if not accounts and account_id is not None:
            accounts = {account_id}

        now = time.monotonic()
        snapshot: Dict[str, Dict[str, Dict[str, float | int]]] = {}

        for account in sorted(accounts):
            entry: Dict[str, Dict[str, float | int]] = {}
            for transport, limiter in self._limits.items():
                key = (account, transport)
                lock = self._locks.get(key)
                timestamps: Deque[float] | Tuple[()] = tuple()
                if lock is not None:
                    async with lock:
                        timestamps = deque(self._usage.get(key, deque()))
                else:
                    timestamps = deque()

                count = 0
                reset_in = 0.0
                if timestamps:
                    self._prune(timestamps, now, limiter.window)
                    count = len(timestamps)
                    if timestamps:
                        oldest = timestamps[0]
                        reset_in = max(limiter.window - (now - oldest), 0.0)

                remaining = max(limiter.limit - count, 0)
                backlog = waiters_snapshot.get(key, 0)
                entry[transport] = {
                    "limit": limiter.limit,
                    "window": limiter.window,
                    "inflight": count,
                    "remaining": remaining,
                    "reset_in": reset_in,
                    "backlog": backlog,
                }
            snapshot[account] = entry
        return snapshot

    async def wait_for_idle(self, timeout: Optional[float] = None) -> bool:
        """Wait until no callers are queued behind the rate limit guard."""

        deadline = None if timeout is None else time.monotonic() + max(timeout, 0.0)
        poll_interval = max(self._sleep_floor, 0.05)

        while True:
            async with self._waiter_lock:
                pending = sum(self._waiter_counts.values())

            if pending == 0:
                return True

            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                await asyncio.sleep(min(poll_interval, remaining))
            else:
                await asyncio.sleep(poll_interval)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _normalise_transport(self, value: str) -> str:
        lowered = value.lower()
        if lowered in {"ws", "websocket"}:
            return "websocket"
        if lowered in {"rest", "http"}:
            return "rest"
        raise ValueError(f"unsupported transport {value!r}")

    @staticmethod
    def _prune(bucket: Deque[float], now: float, window: float) -> None:
        while bucket and now - bucket[0] > window:
            bucket.popleft()

    @staticmethod
    def _coerce_remaining(value: Optional[int], limit: int) -> int:
        if value is None:
            return max(limit, 0)
        try:
            remaining = int(value)
        except (TypeError, ValueError):
            return max(limit, 0)
        return max(min(remaining, limit), 0)

    async def _sleep_with_waiters(self, key: Tuple[str, str], delay: float) -> int:
        async with self._waiter_lock:
            self._waiter_counts[key] += 1
            pending = self._waiter_counts[key]
        try:
            await asyncio.sleep(delay)
        finally:
            async with self._waiter_lock:
                current = self._waiter_counts.get(key, 0) - 1
                if current <= 0:
                    self._waiter_counts.pop(key, None)
                    pending = 0
                else:
                    self._waiter_counts[key] = current
                    pending = current
        return pending


rate_limit_guard = RateLimitGuard()

__all__ = ["RateLimitGuard", "rate_limit_guard"]
