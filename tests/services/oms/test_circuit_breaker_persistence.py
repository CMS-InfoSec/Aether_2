from __future__ import annotations

from typing import Dict

import pytest

import sys
import types

if "aiohttp" not in sys.modules:
    class _StubSession:
        async def __aenter__(self) -> "_StubSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def close(self) -> None:
            return None

        async def post(self, *args: object, **kwargs: object) -> object:
            raise RuntimeError("aiohttp stub invoked")

        async def get(self, *args: object, **kwargs: object) -> object:
            raise RuntimeError("aiohttp stub invoked")

    class _ClientTimeout:
        def __init__(self, total: float | None = None) -> None:
            self.total = total

    aiohttp_stub = types.SimpleNamespace(
        ClientSession=lambda *args, **kwargs: _StubSession(),
        ClientTimeout=_ClientTimeout,
        ClientError=Exception,
    )
    sys.modules.setdefault("aiohttp", aiohttp_stub)

from services.oms import main
from services.oms.circuit_breaker_store import CircuitBreakerStateStore
from tests.fixtures.backends import MemoryRedis


@pytest.fixture
def memory_store() -> Dict[str, object]:
    backend = MemoryRedis()
    store = CircuitBreakerStateStore(redis_client=backend)
    original_store = main.CircuitBreaker._store  # type: ignore[attr-defined]
    main.CircuitBreaker.use_store(store)
    main.CircuitBreaker.reset()
    yield {"backend": backend, "store": store, "original": original_store}
    main.CircuitBreaker.reset()
    main.CircuitBreaker.use_store(original_store)  # type: ignore[arg-type]
    if original_store is not None:
        main.CircuitBreaker.reload_from_store()
    else:
        main.CircuitBreaker._halts.clear()


def test_circuit_breaker_state_survives_restart(memory_store: Dict[str, object], monkeypatch: pytest.MonkeyPatch) -> None:
    clock = {"now": 1_000_000.0}

    def fake_time() -> float:
        return clock["now"]

    monkeypatch.setattr(main.time, "time", fake_time)

    main.CircuitBreaker.halt("BTC-USD", reason="volatility spike", ttl_seconds=120)
    assert main.CircuitBreaker.is_halted("BTC-USD") is True

    # Simulate process restart by clearing in-memory cache and reloading from the shared store
    main.CircuitBreaker._halts.clear()
    main.CircuitBreaker.reload_from_store()
    assert main.CircuitBreaker.is_halted("BTC-USD") is True

    # Advance to just before expiration; the halt should still be active
    clock["now"] += 110
    assert main.CircuitBreaker.is_halted("BTC-USD") is True

    # Once the expiry time elapses the halt should disappear and be pruned from persistence
    clock["now"] += 20
    assert main.CircuitBreaker.is_halted("BTC-USD") is False

    # Reloading after expiration should not reintroduce stale state
    main.CircuitBreaker._halts.clear()
    main.CircuitBreaker.reload_from_store()
    assert main.CircuitBreaker.is_halted("BTC-USD") is False

    backend = memory_store["backend"]
    assert backend.get(CircuitBreakerStateStore._DEFAULT_KEY) is None
