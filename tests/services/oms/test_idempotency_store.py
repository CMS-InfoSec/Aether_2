from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import pytest

from services.oms.idempotency_backend import RedisIdempotencyBackend
from services.oms.idempotency_store import _IdempotencyStore


class _FakeResponse:
    def __init__(
        self,
        *,
        exchange_order_id: str,
        status: str = "accepted",
        filled_qty: Decimal | str = Decimal("0"),
        avg_price: Decimal | str = Decimal("0"),
        errors: list[str] | None = None,
    ) -> None:
        self.exchange_order_id = exchange_order_id
        self.status = status
        self.filled_qty = Decimal(str(filled_qty))
        self.avg_price = Decimal(str(avg_price))
        self.errors = errors

    def model_dump(self, *, mode: str = "python") -> dict[str, object]:
        if mode not in {"python", "json"}:
            raise ValueError(f"Unsupported dump mode {mode}")
        return {
            "exchange_order_id": self.exchange_order_id,
            "status": self.status,
            "filled_qty": str(self.filled_qty) if mode == "json" else self.filled_qty,
            "avg_price": str(self.avg_price) if mode == "json" else self.avg_price,
            "errors": self.errors,
        }

    @classmethod
    def model_validate(cls, payload: dict[str, object]) -> "_FakeResponse":
        return cls(
            exchange_order_id=str(payload.get("exchange_order_id", "")),
            status=str(payload.get("status", "accepted")),
            filled_qty=payload.get("filled_qty", Decimal("0")),
            avg_price=payload.get("avg_price", Decimal("0")),
            errors=payload.get("errors"),
        )


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


async def _create_response(order_id: str) -> _FakeResponse:
    return _FakeResponse(
        exchange_order_id=order_id,
        status="accepted",
        filled_qty=Decimal("0"),
        avg_price=Decimal("0"),
        errors=None,
    )


def _create_store(ttl_seconds: float) -> _IdempotencyStore:
    backend = RedisIdempotencyBackend(_MemoryRedis(), poll_interval=0.01, minimum_ttl=0.01)
    return _IdempotencyStore(
        "acct",
        ttl_seconds=ttl_seconds,
        backend=backend,
        result_decoder=_FakeResponse.model_validate,
    )


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


def test_idempotency_backend_round_trip_serializes_ack_payload() -> None:
    async def scenario() -> None:
        redis = _MemoryRedis()
        backend = RedisIdempotencyBackend(redis, poll_interval=0.01, minimum_ttl=0.01)

        future, is_owner = await backend.reserve("acct", "order", ttl_seconds=5.0)
        assert is_owner is True

        payload = {
            "status": "ack",
            "exchange_order_id": "abc123",
            "fills": [
                {"price": 101.25, "quantity": 2},
                {"price": 101.5, "quantity": 1},
            ],
        }

        await backend.complete("acct", "order", payload, ttl_seconds=5.0)

        result = await future
        assert result == payload

        reused_future, reused = await backend.reserve("acct", "order", ttl_seconds=5.0)
        assert reused is False
        assert await reused_future == payload

        reader_backend = RedisIdempotencyBackend(redis, poll_interval=0.01, minimum_ttl=0.01)
        cached_future, cached_reused = await reader_backend.reserve(
            "acct", "order", ttl_seconds=5.0
        )
        assert cached_reused is False
        assert await cached_future == payload

    asyncio.run(scenario())


def test_idempotency_backend_rejects_tampered_payload() -> None:
    async def scenario() -> None:
        redis = _MemoryRedis()
        backend = RedisIdempotencyBackend(redis, poll_interval=0.01, minimum_ttl=0.01)

        key = backend._key("acct", "order")
        await redis.set(key, b"not-json")

        future, reused = await backend.reserve("acct", "order", ttl_seconds=5.0)
        assert reused is False

        with pytest.raises(RuntimeError, match="Invalid idempotency payload"):
            await future

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
