"""Utilities for creating Redis clients with graceful fallbacks.

This module provides a tiny in-memory Redis implementation that mimics the
subset of commands relied upon by the test-suite.  The real production system
uses the ``redis`` package, but many unit tests run in constrained
environments where that dependency is unavailable or stubbed.  Import-time
initialisation of services previously failed with ``AttributeError`` when the
stub lacked the ``Redis`` attribute.  By funnelling client creation through
these helpers we ensure the codebase keeps working even without the optional
dependency while retaining behaviour close to the production system.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, List, Mapping, Tuple


LOGGER = logging.getLogger(__name__)


def _to_float(value: str | float | int) -> float:
    if value == "-inf":
        return float("-inf")
    if value == "+inf":
        return float("inf")
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        return 0.0


@dataclass
class _SortedSetEntry:
    member: str
    score: float


class InMemoryRedis:
    """A minimal Redis clone that supports the commands we rely on in tests."""

    def __init__(self, *, decode_responses: bool = True) -> None:
        self._decode_responses = decode_responses
        self._values: Dict[str, Any] = {}
        self._lists: Dict[str, List[str]] = {}
        self._sorted_sets: Dict[str, List[_SortedSetEntry]] = {}
        self._expirations: Dict[str, float] = {}
        self._lock = Lock()

    # ------------------------------------------------------------------
    # basic key/value operations
    # ------------------------------------------------------------------
    def _is_expired(self, key: str) -> bool:
        expires = self._expirations.get(key)
        if expires is None:
            return False
        if expires <= time.monotonic():
            self.delete(key)
            return True
        return False

    def set(self, key: str, value: Any) -> bool:
        with self._lock:
            self._values[key] = value
            self._expirations.pop(key, None)
        return True

    def setex(self, key: str, ttl_seconds: float | int, value: Any) -> bool:
        ttl = max(float(ttl_seconds), 0.0)
        with self._lock:
            self._values[key] = value
            self._expirations[key] = time.monotonic() + ttl
        return True

    def get(self, key: str) -> Any | None:
        with self._lock:
            if self._is_expired(key):
                return None
            value = self._values.get(key)
        return value

    def delete(self, key: str) -> int:
        removed = 0
        with self._lock:
            if key in self._values:
                removed += 1
                self._values.pop(key, None)
            if key in self._lists:
                removed += 1
                self._lists.pop(key, None)
            if key in self._sorted_sets:
                removed += 1
                self._sorted_sets.pop(key, None)
            self._expirations.pop(key, None)
        return removed

    def expire(self, key: str, ttl_seconds: float | int) -> bool:
        ttl = max(float(ttl_seconds), 0.0)
        with self._lock:
            if key not in self._values and key not in self._lists and key not in self._sorted_sets:
                return False
            self._expirations[key] = time.monotonic() + ttl
        return True

    def flushall(self) -> None:
        with self._lock:
            self._values.clear()
            self._lists.clear()
            self._sorted_sets.clear()
            self._expirations.clear()

    # ------------------------------------------------------------------
    # list operations
    # ------------------------------------------------------------------
    def lpush(self, key: str, value: str) -> int:
        with self._lock:
            queue = self._lists.setdefault(key, [])
            queue.insert(0, value)
            self._values[key] = queue
            return len(queue)

    def ltrim(self, key: str, start: int, end: int) -> None:
        with self._lock:
            queue = self._lists.get(key)
            if queue is None:
                return
            length = len(queue)
            if end < 0:
                end = length + end
            end = min(end, length - 1)
            start = max(start, 0)
            queue[:] = queue[start : end + 1]

    def lrange(self, key: str, start: int, end: int) -> List[str]:
        with self._lock:
            if self._is_expired(key):
                return []
            queue = self._lists.get(key, [])
            length = len(queue)
            if length == 0:
                return []
            if end < 0:
                end = length + end
            end = min(end, length - 1)
            start = max(start, 0)
            return list(queue[start : end + 1])

    # ------------------------------------------------------------------
    # sorted set operations
    # ------------------------------------------------------------------
    def _get_sorted_set(self, key: str) -> List[_SortedSetEntry]:
        zset = self._sorted_sets.setdefault(key, [])
        return zset

    def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        with self._lock:
            zset = self._get_sorted_set(key)
            existing = {entry.member: entry for entry in zset}
            added = 0
            for member, score in mapping.items():
                score = float(score)
                entry = existing.get(member)
                if entry is None:
                    zset.append(_SortedSetEntry(member=member, score=score))
                    existing[member] = zset[-1]
                    added += 1
                else:
                    entry.score = score
            zset.sort(key=lambda item: item.score)
            return added

    def zremrangebyscore(self, key: str, min_score: float | int | str, max_score: float | int | str) -> int:
        minimum = _to_float(min_score)
        maximum = _to_float(max_score)
        with self._lock:
            zset = self._sorted_sets.get(key)
            if not zset:
                return 0
            original_len = len(zset)
            zset[:] = [entry for entry in zset if not (minimum <= entry.score <= maximum)]
            removed = original_len - len(zset)
            if not zset:
                self._sorted_sets.pop(key, None)
            return removed

    def zcount(self, key: str, min_score: float | int | str, max_score: float | int | str) -> int:
        if self._is_expired(key):
            return 0
        minimum = _to_float(min_score)
        maximum = _to_float(max_score)
        with self._lock:
            zset = self._sorted_sets.get(key, [])
            return sum(1 for entry in zset if minimum <= entry.score <= maximum)

    # ------------------------------------------------------------------
    # pipeline support
    # ------------------------------------------------------------------
    class _Pipeline:
        def __init__(self, redis: "InMemoryRedis") -> None:
            self._redis = redis
            self._results: List[Any] = []

        def _record(self, func, *args, **kwargs) -> "InMemoryRedis._Pipeline":
            result = func(*args, **kwargs)
            self._results.append(result)
            return self

        def lpush(self, *args, **kwargs):
            return self._record(self._redis.lpush, *args, **kwargs)

        def ltrim(self, *args, **kwargs):
            return self._record(self._redis.ltrim, *args, **kwargs)

        def zadd(self, *args, **kwargs):
            return self._record(self._redis.zadd, *args, **kwargs)

        def zremrangebyscore(self, *args, **kwargs):
            return self._record(self._redis.zremrangebyscore, *args, **kwargs)

        def expire(self, *args, **kwargs):
            return self._record(self._redis.expire, *args, **kwargs)

        def execute(self) -> List[Any]:
            results = list(self._results)
            self._results.clear()
            return results

    def pipeline(self) -> "InMemoryRedis._Pipeline":
        return InMemoryRedis._Pipeline(self)


def create_redis_from_url(
    url: str,
    *,
    decode_responses: bool = True,
    logger: logging.Logger | None = None,
) -> Tuple[Any, bool]:
    """Return a Redis client and a flag indicating whether a stub was used."""

    try:  # pragma: no cover - optional dependency path
        import redis  # type: ignore[import-not-found]
    except Exception:  # pragma: no cover - fallback when redis unavailable
        redis = None  # type: ignore[assignment]

    client = None
    if redis is not None:
        redis_client = getattr(redis, "Redis", None)
        from_url = getattr(redis_client, "from_url", None) if redis_client is not None else None
        if callable(from_url):
            try:
                client = from_url(url, decode_responses=decode_responses)
                if hasattr(client, "ping"):
                    try:
                        client.ping()
                    except Exception as exc:  # pragma: no cover - redis server unavailable
                        if logger is not None:
                            logger.warning(
                                "Redis ping failed for %s (%s); using in-memory stub instead", url, exc
                            )
                        client = None
                if client is not None:
                    return client, False
            except Exception as exc:  # pragma: no cover - degrade gracefully on runtime failure
                if logger is not None:
                    logger.warning("Failed to create redis client for %s: %s", url, exc)

    if logger is not None:
        logger.info("Using in-memory Redis stub for %s", url)
    return InMemoryRedis(decode_responses=decode_responses), True


__all__ = ["InMemoryRedis", "create_redis_from_url"]

