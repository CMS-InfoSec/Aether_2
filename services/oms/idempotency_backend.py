"""Distributed idempotency backend built on top of Redis primitives."""
from __future__ import annotations

import asyncio
import pickle
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Protocol, Tuple

from services.common.config import get_redis_client

if TYPE_CHECKING:  # pragma: no cover - optional dependency typing aid
    from redis.asyncio import Redis


_STATE_PENDING = "pending"
_STATE_RESULT = "result"
_STATE_ERROR = "error"


class SupportsRedis(Protocol):
    async def get(self, key: str) -> bytes | None:
        ...

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ex: float | int | None = None,
        nx: bool = False,
    ) -> bool:
        ...

    async def delete(self, key: str) -> int:
        ...

    async def ttl(self, key: str) -> int:
        ...


@dataclass
class _LocalEntry:
    future: asyncio.Future[Any]
    expiry: float


class IdempotencyBackend(Protocol):
    async def reserve(
        self, account_id: str, client_id: str, ttl_seconds: float
    ) -> Tuple[asyncio.Future[Any], bool]:
        ...

    async def complete(
        self, account_id: str, client_id: str, result: Any, ttl_seconds: float
    ) -> None:
        ...

    async def fail(self, account_id: str, client_id: str, exc: Exception) -> None:
        ...


class RedisIdempotencyBackend:
    """Coordinate idempotent order execution across OMS replicas."""

    def __init__(
        self,
        redis: SupportsRedis,
        *,
        namespace: str = "oms:idempotency",
        poll_interval: float = 0.05,
        minimum_ttl: float = 1.0,
    ) -> None:
        self._redis = redis
        self._namespace = namespace.rstrip(":")
        self._poll_interval = poll_interval
        self._minimum_ttl = max(minimum_ttl, 0.001)
        self._lock = asyncio.Lock()
        self._entries: dict[str, _LocalEntry] = {}

    def _key(self, account_id: str, client_id: str) -> str:
        return f"{self._namespace}:{account_id}:{client_id}"

    @staticmethod
    def _encode(state: str, payload: Any) -> bytes:
        return pickle.dumps((state, time.time(), payload))

    @staticmethod
    def _decode(value: bytes) -> tuple[str, float, Any]:
        state, timestamp, payload = pickle.loads(value)
        return state, float(timestamp), payload

    async def reserve(
        self, account_id: str, client_id: str, ttl_seconds: float
    ) -> tuple[asyncio.Future[Any], bool]:
        """Return a shared future and whether the caller owns computation."""

        key = self._key(account_id, client_id)
        loop = asyncio.get_running_loop()

        while True:
            async with self._lock:
                existing = self._entries.get(key)
                if existing is not None:
                    if not existing.future.done():
                        return existing.future, False
                    if time.monotonic() < existing.expiry:
                        return existing.future, False
                    self._entries.pop(key, None)

            payload = await self._redis.get(key)
            if payload:
                state, _, data = self._decode(payload)
                future = loop.create_future()
                if state == _STATE_RESULT:
                    future.set_result(data)
                    ttl_remaining = await self._redis.ttl(key)
                    expiry = (
                        time.monotonic() + ttl_remaining
                        if ttl_remaining > 0
                        else time.monotonic()
                    )
                elif state == _STATE_ERROR:
                    future.set_exception(RuntimeError(str(data)))
                    expiry = time.monotonic()
                else:
                    expiry = float("inf")
                    asyncio.create_task(self._wait_for_result(key, future, ttl_seconds))
                async with self._lock:
                    self._entries[key] = _LocalEntry(future=future, expiry=expiry)
                return future, False

            pending_ttl = max(self._minimum_ttl, ttl_seconds or self._minimum_ttl)
            was_set = await self._redis.set(
                key,
                self._encode(_STATE_PENDING, None),
                nx=True,
                ex=pending_ttl,
            )
            if was_set:
                future = loop.create_future()
                async with self._lock:
                    self._entries[key] = _LocalEntry(future=future, expiry=float("inf"))
                return future, True

    async def complete(
        self, account_id: str, client_id: str, result: Any, ttl_seconds: float
    ) -> None:
        key = self._key(account_id, client_id)
        ttl = max(self._minimum_ttl, ttl_seconds)
        await self._redis.set(key, self._encode(_STATE_RESULT, result), ex=ttl)
        async with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return
            if not entry.future.done():
                entry.future.set_result(result)
            entry.expiry = time.monotonic() + ttl_seconds

    async def fail(self, account_id: str, client_id: str, exc: Exception) -> None:
        key = self._key(account_id, client_id)
        message = repr(exc)
        await self._redis.set(key, self._encode(_STATE_ERROR, message), ex=self._minimum_ttl)
        async with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return
            if not entry.future.done():
                entry.future.set_exception(exc)
            entry.expiry = time.monotonic()

    async def _wait_for_result(
        self, key: str, future: asyncio.Future[Any], ttl_seconds: float
    ) -> None:
        try:
            while True:
                payload = await self._redis.get(key)
                if payload is None:
                    raise RuntimeError("Idempotency entry disappeared before completion")
                state, _, data = self._decode(payload)
                if state == _STATE_PENDING:
                    await asyncio.sleep(self._poll_interval)
                    continue
                if state == _STATE_RESULT:
                    if not future.done():
                        future.set_result(data)
                    ttl_remaining = await self._redis.ttl(key)
                    expiry = (
                        time.monotonic() + ttl_remaining
                        if ttl_remaining > 0
                        else time.monotonic()
                    )
                else:
                    if not future.done():
                        future.set_exception(RuntimeError(str(data)))
                    expiry = time.monotonic()
                async with self._lock:
                    entry = self._entries.get(key)
                    if entry and entry.future is future:
                        entry.expiry = expiry
                break
        except Exception as exc:  # pragma: no cover - defensive
            if not future.done():
                future.set_exception(exc)


@lru_cache(maxsize=None)
def get_idempotency_backend(account_id: str) -> "RedisIdempotencyBackend":
    from redis.asyncio import Redis

    client = get_redis_client(account_id)
    redis = Redis.from_url(client.dsn, decode_responses=False)
    return RedisIdempotencyBackend(redis)


__all__ = [
    "IdempotencyBackend",
    "RedisIdempotencyBackend",
    "get_idempotency_backend",
]
