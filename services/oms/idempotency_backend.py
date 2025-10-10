"""Distributed idempotency backend built on top of Redis primitives."""
from __future__ import annotations

import asyncio
import json
import math
import sys
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Protocol, Tuple

from decimal import Decimal

from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

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


class _InMemoryRedis:
    """Asynchronous in-memory Redis replacement used for tests."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[bytes, float | None]] = {}
        self._lock = asyncio.Lock()

    def _now(self) -> float:
        return time.monotonic()

    def _get_entry(self, key: str) -> tuple[bytes, float | None] | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        payload, expiry = entry
        if expiry is not None and expiry <= self._now():
            self._store.pop(key, None)
            return None
        return payload, expiry

    async def get(self, key: str) -> bytes | None:
        async with self._lock:
            entry = self._get_entry(key)
            return entry[0] if entry is not None else None

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ex: float | int | None = None,
        nx: bool = False,
    ) -> bool:
        async with self._lock:
            entry = self._get_entry(key)
            if nx and entry is not None:
                return False
            expiry: float | None = None
            if ex is not None:
                expiry = self._now() + float(ex)
            self._store[key] = (value, expiry)
            return True

    async def delete(self, key: str) -> int:
        async with self._lock:
            existed = self._get_entry(key) is not None
            self._store.pop(key, None)
            return 1 if existed else 0

    async def ttl(self, key: str) -> int:
        async with self._lock:
            entry = self._get_entry(key)
            if entry is None:
                return -2
            _, expiry = entry
            if expiry is None:
                return -1
            remaining = expiry - self._now()
            if remaining <= 0:
                self._store.pop(key, None)
                return -2
            return max(int(math.ceil(remaining)), 1)


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
    def _ensure_json_compatible(value: Any) -> Any:
        if value is None or isinstance(value, (str, bool, int)):
            return value
        if isinstance(value, Decimal):
            return str(value)
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            try:
                dumped = model_dump(mode="json")
            except TypeError:  # pragma: no cover - older BaseModel shims
                dumped = model_dump()
            return RedisIdempotencyBackend._ensure_json_compatible(dumped)
        if hasattr(value, "__dict__") and not isinstance(value, type):
            return RedisIdempotencyBackend._ensure_json_compatible(vars(value))
        if isinstance(value, float):
            if not math.isfinite(value):
                raise TypeError("Non-finite floats are not supported in idempotency payloads")
            return value
        if isinstance(value, (list, tuple)):
            return [RedisIdempotencyBackend._ensure_json_compatible(item) for item in value]
        if isinstance(value, set):
            return sorted(
                RedisIdempotencyBackend._ensure_json_compatible(item) for item in value
            )
        if isinstance(value, dict):
            safe_dict: dict[str, Any] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError("JSON object keys must be strings")
                safe_dict[key] = RedisIdempotencyBackend._ensure_json_compatible(item)
            return safe_dict
        raise TypeError("Idempotency payload contains non JSON-serializable data")

    @staticmethod
    def _validate_json_payload(value: Any) -> None:
        if value is None or isinstance(value, (str, bool, int)):
            return
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("Invalid non-finite float in idempotency payload")
            return
        if isinstance(value, list):
            for item in value:
                RedisIdempotencyBackend._validate_json_payload(item)
            return
        if isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ValueError("Invalid non-string key in idempotency payload")
                RedisIdempotencyBackend._validate_json_payload(item)
            return
        raise ValueError("Invalid idempotency payload type")

    @staticmethod
    def _encode(state: str, payload: Any) -> bytes:
        serializable_payload = RedisIdempotencyBackend._ensure_json_compatible(payload)
        document = {
            "state": state,
            "timestamp": time.time(),
            "payload": serializable_payload,
        }
        return json.dumps(document, separators=(",", ":"), sort_keys=True).encode("utf-8")

    @staticmethod
    def _decode(value: bytes) -> tuple[str, float, Any]:
        try:
            document = json.loads(value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Invalid idempotency payload encoding") from exc

        if not isinstance(document, dict):
            raise ValueError("Invalid idempotency payload structure")

        state = document.get("state")
        timestamp = document.get("timestamp")
        payload = document.get("payload")

        if not isinstance(state, str):
            raise ValueError("Invalid idempotency state")
        if not isinstance(timestamp, (int, float)):
            raise ValueError("Invalid idempotency timestamp")
        RedisIdempotencyBackend._validate_json_payload(payload)
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
                try:
                    state, _, data = self._decode(payload)
                except ValueError:
                    future = loop.create_future()
                    future.set_exception(RuntimeError("Invalid idempotency payload"))
                    await self._redis.delete(key)
                    async with self._lock:
                        self._entries[key] = _LocalEntry(
                            future=future, expiry=time.monotonic()
                        )
                    return future, False
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
                try:
                    state, _, data = self._decode(payload)
                except ValueError:
                    await self._redis.delete(key)
                    if not future.done():
                        future.set_exception(RuntimeError("Invalid idempotency payload"))
                    expiry = time.monotonic()
                    async with self._lock:
                        entry = self._entries.get(key)
                        if entry and entry.future is future:
                            entry.expiry = expiry
                    break
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
    def _memory_backend() -> RedisIdempotencyBackend:
        return RedisIdempotencyBackend(_InMemoryRedis())

    try:
        client = get_redis_client(account_id)
    except RuntimeError:
        if "pytest" in sys.modules:
            return _memory_backend()
        raise

    dsn = client.dsn.strip()
    if dsn.lower().startswith("memory://"):
        return _memory_backend()

    try:
        from redis.asyncio import Redis
    except Exception as exc:
        if "pytest" in sys.modules:
            return _memory_backend()
        raise RuntimeError("redis asyncio client is required for idempotency persistence") from exc

    redis = Redis.from_url(dsn, decode_responses=False)
    return RedisIdempotencyBackend(redis)


__all__ = [
    "IdempotencyBackend",
    "RedisIdempotencyBackend",
    "get_idempotency_backend",
]
