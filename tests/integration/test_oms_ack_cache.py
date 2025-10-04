from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, List

from pathlib import Path
import importlib
import sys

import pytest

pytest.importorskip("fastapi")

if "redis.asyncio" not in sys.modules:
    redis_stub = ModuleType("redis")
    redis_stub.__path__ = []  # type: ignore[attr-defined]
    redis_asyncio_stub = ModuleType("redis.asyncio")

    # No Redis attribute is exposed so that importing redis.asyncio.Redis raises ImportError
    sys.modules["redis"] = redis_stub
    sys.modules["redis.asyncio"] = redis_asyncio_stub

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

if "aiohttp" not in sys.modules:
    aiohttp_stub = ModuleType("aiohttp")

    class _StubSession:
        async def __aenter__(self) -> "_StubSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def close(self) -> None:
            return None

        async def get(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("aiohttp GET invoked in stub")

        async def post(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("aiohttp POST invoked in stub")

    class _ClientTimeout:
        def __init__(self, total: float | None = None) -> None:
            self.total = total

    aiohttp_stub.ClientSession = lambda *args, **kwargs: _StubSession()
    aiohttp_stub.ClientTimeout = _ClientTimeout
    aiohttp_stub.ClientError = Exception
    sys.modules["aiohttp"] = aiohttp_stub

services_spec = importlib.util.spec_from_file_location(
    "services", ROOT / "services" / "__init__.py"
)
if services_spec is None or services_spec.loader is None:
    raise RuntimeError("Unable to load services package")
services_module = importlib.util.module_from_spec(services_spec)
sys.modules["services"] = services_module
services_spec.loader.exec_module(services_module)

common_spec = importlib.util.spec_from_file_location(
    "services.common", ROOT / "services" / "common" / "__init__.py"
)
if common_spec is None or common_spec.loader is None:
    raise RuntimeError("Unable to load services.common package")
common_pkg = importlib.util.module_from_spec(common_spec)
sys.modules["services.common"] = common_pkg
services_module.common = common_pkg  # type: ignore[attr-defined]
common_spec.loader.exec_module(common_pkg)

security_stub = ModuleType("services.common.security")

def set_default_session_store(store: Any) -> None:
    return None


def require_admin_account() -> Any:
    def _dependency() -> str:
        return "company"

    return _dependency


security_stub.set_default_session_store = set_default_session_store  # type: ignore[attr-defined]
security_stub.require_admin_account = require_admin_account  # type: ignore[attr-defined]
sys.modules["services.common.security"] = security_stub
common_pkg.security = security_stub  # type: ignore[attr-defined]

config_stub = ModuleType("services.common.config")


def get_redis_client(account_id: str) -> SimpleNamespace:
    return SimpleNamespace(dsn="redis://localhost/0")


config_stub.get_redis_client = get_redis_client  # type: ignore[attr-defined]
sys.modules["services.common.config"] = config_stub
common_pkg.config = config_stub  # type: ignore[attr-defined]

adapters_stub = ModuleType("services.common.adapters")


class _StubKafka:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.messages: List[Dict[str, Any]] = []

    async def publish(self, *, topic: str, payload: Dict[str, Any]) -> None:
        self.messages.append({"topic": topic, "payload": dict(payload)})

    @classmethod
    async def flush_events(cls) -> Dict[str, int]:
        return {}

    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        return None


class _StubTimescale:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.acks: List[Dict[str, Any]] = []

    def record_ack(self, payload: Dict[str, Any]) -> None:
        self.acks.append(dict(payload))

    def record_usage(self, notional: float) -> None:
        return None

    def record_fill(self, payload: Dict[str, Any]) -> None:
        return None

    def record_shadow_fill(self, payload: Dict[str, Any]) -> None:
        return None

    @classmethod
    async def flush_event_buffers(cls) -> Dict[str, int]:
        return {}


adapters_stub.KafkaNATSAdapter = _StubKafka  # type: ignore[attr-defined]
adapters_stub.TimescaleAdapter = _StubTimescale  # type: ignore[attr-defined]
class _StubKrakenSecretManager:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        return None

    def get_credentials(self) -> Dict[str, Any]:
        return {
            "api_key": "key",
            "api_secret": "secret",
            "metadata": {"rotated_at": datetime.now(timezone.utc).isoformat()},
        }


adapters_stub.KrakenSecretManager = _StubKrakenSecretManager  # type: ignore[attr-defined]
services_module.common.adapters = adapters_stub  # type: ignore[attr-defined]
sys.modules["services.common.adapters"] = adapters_stub

oms_init_path = ROOT / "services" / "oms" / "__init__.py"
oms_spec = importlib.util.spec_from_file_location("services.oms", oms_init_path)
if oms_spec is None or oms_spec.loader is None:
    raise RuntimeError("Unable to load services.oms package")
oms_module = importlib.util.module_from_spec(oms_spec)
services_module.oms = oms_module  # type: ignore[attr-defined]
sys.modules["services.oms"] = oms_module
oms_spec.loader.exec_module(oms_module)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_duplicate_orders_use_cached_ack(monkeypatch: pytest.MonkeyPatch) -> None:
    oms_main = importlib.reload(importlib.import_module("services.oms.main"))
    order_ack_cache = importlib.import_module("services.oms.order_ack_cache")
    schemas = importlib.import_module("services.common.schemas")

    order_ack_cache.get_order_ack_cache.cache_clear()
    _ack_caches: dict[str, order_ack_cache.InMemoryOrderAckCache] = {}

    def _in_memory_backend(account_id: str) -> order_ack_cache.InMemoryOrderAckCache:
        if account_id not in _ack_caches:
            _ack_caches[account_id] = order_ack_cache.InMemoryOrderAckCache(account_id)
        return _ack_caches[account_id]

    def _clear_backend() -> None:
        _ack_caches.clear()

    _in_memory_backend.cache_clear = _clear_backend  # type: ignore[attr-defined]
    monkeypatch.setattr(order_ack_cache, "get_order_ack_cache", _in_memory_backend)
    monkeypatch.setattr(oms_main, "get_order_ack_cache", _in_memory_backend)

    metadata = {"BTC-USD": {"tick": 0.1, "lot": 0.01, "native_pair": "XBT/USD"}}

    class _MetadataCache:
        async def get(self, instrument: str) -> Dict[str, Any] | None:
            entry = metadata.get(instrument)
            return dict(entry) if entry is not None else None

        async def refresh(self) -> None:
            return None

    submit_payloads: List[Dict[str, Any]] = []

    async def _stub_submit_order(
        ws_client: Any, rest_client: Any, payload: Dict[str, Any]
    ) -> tuple[oms_main.OrderAck, str]:
        submit_payloads.append(dict(payload))
        ack = oms_main.OrderAck(
            exchange_order_id=f"EX-{payload['clientOrderId']}",
            status="ok",
            filled_qty=None,
            avg_price=None,
            errors=None,
        )
        return ack, "websocket"

    async def _stub_fetch_open_orders(*_: Any, **__: Any) -> List[Dict[str, Any]]:
        return []

    async def _stub_fetch_trades(*_: Any, **__: Any) -> List[Dict[str, Any]]:
        return []

    @asynccontextmanager
    async def _stub_clients(_: str):
        async def _credentials() -> Dict[str, Any]:
            return {
                "api_key": "key",
                "api_secret": "secret",
                "metadata": {"rotated_at": datetime.now(timezone.utc).isoformat()},
            }

        bundle = oms_main.KrakenClientBundle(
            credential_getter=_credentials,
            ws_client=object(),
            rest_client=object(),
        )
        yield bundle

    monkeypatch.setattr(oms_main, "KafkaNATSAdapter", _StubKafka)
    monkeypatch.setattr(oms_main, "TimescaleAdapter", _StubTimescale)
    monkeypatch.setattr(oms_main, "_submit_order", _stub_submit_order)
    monkeypatch.setattr(oms_main, "_fetch_open_orders", _stub_fetch_open_orders)
    monkeypatch.setattr(oms_main, "_fetch_own_trades", _stub_fetch_trades)
    monkeypatch.setattr(oms_main, "_acquire_kraken_clients", _stub_clients)
    monkeypatch.setattr(oms_main, "_ensure_credentials_valid", lambda credentials: None)
    monkeypatch.setattr(oms_main, "market_metadata_cache", _MetadataCache())
    oms_main.app.state.market_metadata_cache = _MetadataCache()
    monkeypatch.setattr(oms_main, "ACK_CACHE_TTL_SECONDS", 120.0)

    FeeBreakdown = schemas.FeeBreakdown
    OrderPlacementRequest = schemas.OrderPlacementRequest

    request = OrderPlacementRequest(
        account_id="company",
        order_id="OID-1",
        instrument="BTC-USD",
        side="BUY",
        quantity=0.5,
        price=20000.0,
        fee=FeeBreakdown(currency="USD", maker=0.0, taker=0.0),
    )

    first = await oms_main.place_order(request, account_id="company")
    assert first.accepted is True
    assert len(submit_payloads) == 1

    cache = order_ack_cache.get_order_ack_cache("company")
    cached_entry = await cache.get(request.order_id)
    assert cached_entry is not None
    assert cached_entry.payload["txid"] == "EX-OID-1"

    second = await oms_main.place_order(request, account_id="company")
    assert second.accepted is True
    assert len(submit_payloads) == 1

    follow_up = request.model_copy(update={"order_id": "OID-2"})
    third = await oms_main.place_order(follow_up, account_id="company")
    assert third.accepted is True
    assert len(submit_payloads) == 2
