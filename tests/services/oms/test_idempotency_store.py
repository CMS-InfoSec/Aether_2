from __future__ import annotations

import asyncio
import types
import time

from services.oms.idempotency_backend import RedisIdempotencyBackend
from services.oms.idempotency_store import _IdempotencyStore


class _MemoryRedis:
    """Minimal Redis-compatible client for exercising the backend in tests."""

    def __init__(self) -> None:
        self._data: dict[str, tuple[bytes, float | None]] = {}
        self._lock = asyncio.Lock()

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ex: float | int | None = None,
        nx: bool = False,
    ) -> bool:
        async with self._lock:
            existing = self._data.get(key)
            if nx and existing is not None:
                _, expiry = existing
                if expiry is None or expiry > time.monotonic():
                    return False
            expiry: float | None = None
            if ex is not None:
                expiry = time.monotonic() + float(ex)
            self._data[key] = (value, expiry)
            return True

    async def get(self, key: str) -> bytes | None:
        async with self._lock:
            item = self._data.get(key)
            if item is None:
                return None
            value, expiry = item
            if expiry is not None and expiry <= time.monotonic():
                self._data.pop(key, None)
                return None
            return value

    async def delete(self, key: str) -> int:
        async with self._lock:
            return int(self._data.pop(key, None) is not None)

    async def ttl(self, key: str) -> int:
        async with self._lock:
            item = self._data.get(key)
            if item is None:
                return -2
            _, expiry = item
            if expiry is None:
                return -1
            remaining = expiry - time.monotonic()
            if remaining <= 0:
                self._data.pop(key, None)
                return -2
            return int(remaining)


async def _create_response(order_id: str) -> types.SimpleNamespace:
    return types.SimpleNamespace(exchange_order_id=order_id)


def _create_store(ttl_seconds: float) -> _IdempotencyStore:
    backend = RedisIdempotencyBackend(_MemoryRedis(), poll_interval=0.01, minimum_ttl=0.01)
    return _IdempotencyStore("acct", ttl_seconds=ttl_seconds, backend=backend)


def test_idempotency_store_evicts_entries_after_ttl() -> None:
    async def scenario() -> None:
        store = _create_store(ttl_seconds=0.05)

        first_response, reused_first = await store.get_or_create("order", _create_response("first"))

        assert reused_first is False
        assert first_response.exchange_order_id == "first"

        await asyncio.sleep(0.06)

        second_response, reused_second = await store.get_or_create(
            "order", _create_response("second")
        )

        assert reused_second is False
        assert second_response.exchange_order_id == "second"

    asyncio.run(scenario())


def test_idempotency_store_retains_recent_entries() -> None:
    async def scenario() -> None:
        store = _create_store(ttl_seconds=10.0)

        first_response, reused_first = await store.get_or_create(
            "order", _create_response("initial")
        )

        assert reused_first is False

        next_factory = _create_response("should-not-run")
        second_response, reused_second = await store.get_or_create("order", next_factory)
        next_factory.close()

        assert reused_second is True
        assert second_response is first_response

    asyncio.run(scenario())


def test_idempotency_store_survives_restart_and_suppresses_duplicates() -> None:
    async def scenario() -> None:
        backend = RedisIdempotencyBackend(_MemoryRedis(), poll_interval=0.01, minimum_ttl=0.01)

        first_store = _IdempotencyStore("acct", ttl_seconds=10.0, backend=backend)
        first_response, reused_first = await first_store.get_or_create(
            "order", _create_response("initial")
        )
        assert reused_first is False

        # Simulate a restart by constructing a new store with the shared backend.
        second_store = _IdempotencyStore("acct", ttl_seconds=10.0, backend=backend)
        duplicate_factory = _create_response("duplicate")
        reused_response, reused_second = await second_store.get_or_create("order", duplicate_factory)
        duplicate_factory.close()

        assert reused_second is True
        assert reused_response.exchange_order_id == first_response.exchange_order_id

    asyncio.run(scenario())
