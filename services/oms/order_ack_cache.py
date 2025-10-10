"""Idempotency cache for order acknowledgements backed by Redis."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Protocol

from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

from services.common.config import get_redis_client


logger = logging.getLogger(__name__)


class SupportsRedis(Protocol):
    async def get(self, key: str) -> bytes | str | None:
        ...

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ex: float | int | None = None,
    ) -> Any:
        ...


@dataclass(frozen=True)
class CachedOrderAck:
    """Represents a cached Kraken acknowledgement."""

    payload: dict[str, Any]
    stored_at: datetime


class OrderAckCache(Protocol):
    async def get(self, order_id: str) -> CachedOrderAck | None:
        ...

    async def store(
        self, order_id: str, payload: dict[str, Any], ttl_seconds: float
    ) -> None:
        ...


class RedisOrderAckCache:
    """Persist acknowledgements in Redis for idempotency reuse."""

    def __init__(
        self,
        redis: SupportsRedis,
        account_id: str,
        *,
        namespace: str = "oms:ack-cache",
    ) -> None:
        self._redis = redis
        self._account_id = account_id
        self._namespace = namespace.rstrip(":")

    def _key(self, order_id: str) -> str:
        return f"{self._namespace}:{self._account_id}:{order_id}"

    async def get(self, order_id: str) -> CachedOrderAck | None:
        payload = await self._redis.get(self._key(order_id))
        if payload is None:
            return None

        if isinstance(payload, bytes):
            raw = payload.decode("utf-8", errors="ignore")
        else:
            raw = str(payload)

        try:
            encoded = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Failed to decode cached ack payload", extra={"key": order_id})
            return None

        cached_payload = encoded.get("payload")
        stored_at_raw = encoded.get("stored_at")
        if not isinstance(cached_payload, dict) or not isinstance(stored_at_raw, str):
            logger.debug(
                "Invalid cached ack structure", extra={"key": order_id, "payload": encoded}
            )
            return None

        try:
            stored_at = datetime.fromisoformat(stored_at_raw)
        except ValueError:
            logger.debug("Invalid cached ack timestamp", extra={"stored_at": stored_at_raw})
            return None

        if stored_at.tzinfo is None:
            stored_at = stored_at.replace(tzinfo=timezone.utc)

        return CachedOrderAck(payload=dict(cached_payload), stored_at=stored_at)

    async def store(
        self, order_id: str, payload: dict[str, Any], ttl_seconds: float
    ) -> None:
        if ttl_seconds <= 0:
            return

        entry = {
            "payload": payload,
            "stored_at": datetime.now(timezone.utc).isoformat(),
        }
        data = json.dumps(entry, separators=(",", ":")).encode("utf-8")
        await self._redis.set(self._key(order_id), data, ex=max(ttl_seconds, 0.001))


class InMemoryOrderAckCache:
    """Fallback cache used in test environments when Redis is unavailable."""

    def __init__(self, account_id: str) -> None:
        self._account_id = account_id
        self._entries: dict[str, tuple[dict[str, Any], datetime, float]] = {}
        self._lock = asyncio.Lock()

    def _key(self, order_id: str) -> str:
        return f"{self._account_id}:{order_id}"

    async def get(self, order_id: str) -> CachedOrderAck | None:
        async with self._lock:
            entry = self._entries.get(self._key(order_id))
            if not entry:
                return None
            payload, stored_at, expiry = entry
            if expiry <= time.monotonic():
                self._entries.pop(self._key(order_id), None)
                return None
            return CachedOrderAck(payload=dict(payload), stored_at=stored_at)

    async def store(
        self, order_id: str, payload: dict[str, Any], ttl_seconds: float
    ) -> None:
        if ttl_seconds <= 0:
            return
        async with self._lock:
            expiry = time.monotonic() + ttl_seconds
            self._entries[self._key(order_id)] = (
                dict(payload),
                datetime.now(timezone.utc),
                expiry,
            )


@lru_cache(maxsize=None)
def get_order_ack_cache(account_id: str) -> OrderAckCache:
    """Return the configured cache for the provided account."""

    try:  # pragma: no cover - executed when redis is available
        from redis.asyncio import Redis  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover - fallback for local tests
        logger.debug(
            "redis.asyncio unavailable; using in-memory ack cache", extra={"account": account_id}
        )
        return InMemoryOrderAckCache(account_id)

    client = get_redis_client(account_id)
    redis = Redis.from_url(client.dsn, decode_responses=False)
    return RedisOrderAckCache(redis, account_id)


__all__ = [
    "CachedOrderAck",
    "OrderAckCache",
    "RedisOrderAckCache",
    "InMemoryOrderAckCache",
    "get_order_ack_cache",
]

