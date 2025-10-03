import asyncio
import math
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, Tuple

import pytest
import types

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _stub_module(name: str, **attrs: object) -> types.ModuleType:
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


if "metrics" not in sys.modules:
    _stub_module(
        "metrics",
        increment_oms_error_count=lambda *args, **kwargs: None,
        record_oms_latency=lambda *args, **kwargs: None,
        setup_metrics=lambda *args, **kwargs: None,
        traced_span=lambda *args, **kwargs: types.SimpleNamespace(__enter__=lambda self: None, __exit__=lambda *exc: None),
    )

if "services.common.adapters" not in sys.modules:
    adapters_mod = _stub_module("services.common.adapters")

    class _KafkaNATSAdapter:
        @staticmethod
        def flush_events() -> Dict[str, int]:
            return {}

    adapters_mod.KafkaNATSAdapter = _KafkaNATSAdapter  # type: ignore[attr-defined]

if "services.oms.kraken_rest" not in sys.modules:
    _stub_module(
        "services.oms.kraken_rest",
        KrakenRESTClient=type("KrakenRESTClient", (), {}),
        KrakenRESTError=type("KrakenRESTError", (Exception,), {}),
    )

if "services.oms.kraken_ws" not in sys.modules:
    _stub_module(
        "services.oms.kraken_ws",
        KrakenWSClient=type("KrakenWSClient", (), {}),
        KrakenWSError=type("KrakenWSError", (Exception,), {}),
        KrakenWSTimeout=type("KrakenWSTimeout", (Exception,), {}),
        OrderAck=type("OrderAck", (), {}),
        OrderState=type("OrderState", (), {}),
    )

if "services.oms.rate_limit_guard" not in sys.modules:
    _stub_module(
        "services.oms.rate_limit_guard",
        RateLimitGuard=type("RateLimitGuard", (), {}),
        rate_limit_guard=None,
    )

if "shared.graceful_shutdown" not in sys.modules:
    class _StubShutdownManager:
        def register_flush_callback(self, callback: object) -> None:  # pragma: no cover - simple stub
            self.callback = callback

    _stub_module(
        "shared.graceful_shutdown",
        flush_logging_handlers=lambda *args, **kwargs: None,
        setup_graceful_shutdown=lambda *args, **kwargs: _StubShutdownManager(),
    )

if "services.oms.oms_service" not in sys.modules:
    _stub_module(
        "services.oms.oms_service",
        _PrecisionValidator=type("_PrecisionValidator", (), {}),
        _normalize_symbol=lambda *args, **kwargs: None,
        _resolve_pair_metadata=lambda *args, **kwargs: None,
    )

from oms_service import CancelOrderResponse, IdempotencyCache, PlaceOrderResponse


class _InMemoryRedis:
    def __init__(self) -> None:
        self._data: Dict[str, Tuple[bytes, float | None]] = {}

    def _resolve(self, key: str) -> Tuple[bytes, float | None] | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        payload, expires_at = entry
        if expires_at is not None and expires_at <= time.time():
            self._data.pop(key, None)
            return None
        return payload, expires_at

    async def get(self, key: str) -> bytes | None:
        resolved = self._resolve(key)
        return resolved[0] if resolved else None

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ex: float | int | None = None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self._data:
            return False
        expiry = None if ex is None else time.time() + float(ex)
        self._data[key] = (value, expiry)
        return True

    async def delete(self, key: str) -> int:
        return 1 if self._data.pop(key, None) is not None else 0

    async def ttl(self, key: str) -> int:
        resolved = self._resolve(key)
        if resolved is None:
            return -2
        _, expires_at = resolved
        if expires_at is None:
            return -1
        remaining = expires_at - time.time()
        if remaining <= 0:
            self._data.pop(key, None)
            return -2
        return int(math.ceil(remaining))


def _order_response(order_id: str = "ord-1") -> PlaceOrderResponse:
    return PlaceOrderResponse(
        order_id=order_id,
        status="accepted",
        filled_qty=Decimal("0"),
        avg_price=Decimal("0"),
        errors=None,
        transport="websocket",
        reused=False,
    )


def _cancel_response(order_id: str = "ord-1") -> CancelOrderResponse:
    return CancelOrderResponse(order_id=order_id, status="canceled", transport="websocket", reused=False)


@pytest.mark.asyncio
async def test_idempotency_cache_reuses_recent_results() -> None:
    redis = _InMemoryRedis()
    cache = IdempotencyCache(ttl_seconds=5.0, redis_factory=lambda _: redis)
    account_id = "acct-1"
    client_id = "client-1"

    calls = 0

    async def factory() -> PlaceOrderResponse:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return _order_response()

    result, reused = await cache.get_or_create(account_id, client_id, factory)
    assert reused is False
    assert calls == 1

    await cache.store(account_id, client_id, result)

    again, reused_again = await cache.get_or_create(account_id, client_id, factory)
    assert reused_again is True
    assert calls == 1
    assert again.model_dump() == result.model_dump()


@pytest.mark.asyncio
async def test_idempotency_cache_expires_entries_after_ttl() -> None:
    redis = _InMemoryRedis()
    cache = IdempotencyCache(ttl_seconds=0.1, redis_factory=lambda _: redis)
    account_id = "acct-2"
    client_id = "client-xyz"

    response = _cancel_response()
    await cache.store(account_id, client_id, response)

    await asyncio.sleep(0.15)

    calls = 0

    async def factory() -> CancelOrderResponse:
        nonlocal calls
        calls += 1
        return _cancel_response("ord-2")

    result, reused = await cache.get_or_create(account_id, client_id, factory)
    assert reused is False
    assert calls == 1
    assert result.order_id == "ord-2"


@pytest.mark.asyncio
async def test_idempotency_cache_prunes_expired_entries_from_memory() -> None:
    redis = _InMemoryRedis()
    cache = IdempotencyCache(ttl_seconds=0.05, redis_factory=lambda _: redis)
    account_id = "acct-3"

    for idx in range(5):
        await cache.store(account_id, f"client-{idx}", _order_response(f"ord-{idx}"))

    assert len(cache._entries) == 5  # type: ignore[attr-defined]

    await asyncio.sleep(0.1)

    await cache.store(account_id, "client-final", _order_response("ord-final"))

    assert len(cache._entries) == 1  # type: ignore[attr-defined]
