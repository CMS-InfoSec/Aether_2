from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
from pathlib import Path
import importlib.util
import sys
import types
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

import pytest
import httpx

_REAL_HTTPX_ASYNC_CLIENT = httpx.AsyncClient

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

if "redis.asyncio" not in sys.modules:
    redis_stub = types.ModuleType("redis")
    redis_stub.__path__ = []  # type: ignore[attr-defined]
    redis_asyncio_stub = types.ModuleType("redis.asyncio")
    sys.modules["redis"] = redis_stub
    sys.modules["redis.asyncio"] = redis_asyncio_stub

if "services.common" not in sys.modules:
    common_pkg = importlib.import_module("services.common")
else:
    common_pkg = sys.modules["services.common"]

if "services.common.config" not in sys.modules:
    config_stub = types.ModuleType("services.common.config")

    class _StubTimescaleSession:
        def __init__(self, account_id: str) -> None:
            self.dsn = f"postgresql://stub/{account_id}"
            self.account_schema = f"acct_{account_id}"

    def _stub_get_redis_client(account_id: str) -> types.SimpleNamespace:
        return types.SimpleNamespace(dsn=f"redis://stub/{account_id}")

    def _stub_get_feast_client(*args: Any, **kwargs: Any) -> None:
        return None

    def _stub_get_kafka_producer(account_id: str) -> types.SimpleNamespace:
        return types.SimpleNamespace(account_id=account_id)

    def _stub_get_nats_producer(account_id: str) -> types.SimpleNamespace:
        return types.SimpleNamespace(account_id=account_id)

    def _stub_get_timescale_session(account_id: str) -> _StubTimescaleSession:
        return _StubTimescaleSession(account_id)

    config_stub.get_redis_client = _stub_get_redis_client  # type: ignore[attr-defined]
    config_stub.get_feast_client = _stub_get_feast_client  # type: ignore[attr-defined]
    config_stub.get_kafka_producer = _stub_get_kafka_producer  # type: ignore[attr-defined]
    config_stub.get_nats_producer = _stub_get_nats_producer  # type: ignore[attr-defined]
    config_stub.get_timescale_session = _stub_get_timescale_session  # type: ignore[attr-defined]
    config_stub.TimescaleSession = _StubTimescaleSession  # type: ignore[attr-defined]
    sys.modules["services.common.config"] = config_stub
    setattr(sys.modules["services.common"], "config", config_stub)

if "services.universe" not in sys.modules:
    universe_pkg = types.ModuleType("services.universe")
    universe_pkg.__path__ = []  # type: ignore[attr-defined]
    sys.modules["services.universe"] = universe_pkg

if "services.universe.repository" not in sys.modules:
    repo_stub = types.ModuleType("services.universe.repository")

    class _StubUniverseRepository:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            return None

    repo_stub.UniverseRepository = _StubUniverseRepository  # type: ignore[attr-defined]
    sys.modules["services.universe.repository"] = repo_stub
    setattr(sys.modules["services.universe"], "repository", repo_stub)

if "services.common.schemas" not in sys.modules:
    schemas_spec = importlib.util.spec_from_file_location(
        "services.common.schemas", ROOT / "services" / "common" / "schemas.py"
    )
    if schemas_spec is None or schemas_spec.loader is None:
        raise RuntimeError("Unable to load services.common.schemas module")
    schemas_module = importlib.util.module_from_spec(schemas_spec)
    sys.modules["services.common.schemas"] = schemas_module
    schemas_spec.loader.exec_module(schemas_module)

if "services.secrets" not in sys.modules:
    secrets_pkg = types.ModuleType("services.secrets")
    sys.modules["services.secrets"] = secrets_pkg

if "services.secrets.secure_secrets" not in sys.modules:
    secure_secrets = types.ModuleType("services.secrets.secure_secrets")

    class _StubEnvelope:
        def __init__(self, api_key: str = "", api_secret: str = "") -> None:
            self.api_key = api_key
            self.api_secret = api_secret
            self.kms_key_id = "stub-kms-key"

        @classmethod
        def from_secret_data(cls, data: dict[str, Any]) -> "_StubEnvelope":
            return cls(
                api_key=str(data.get("api_key", "")),
                api_secret=str(data.get("api_secret", "")),
            )

    class _StubEncryptor:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            return None

        @classmethod
        def from_kms(cls, *args: Any, **kwargs: Any) -> "_StubEncryptor":
            return cls()

        def encrypt_credentials(
            self,
            account_id: str,
            *,
            api_key: str,
            api_secret: str,
        ) -> _StubEnvelope:
            return _StubEnvelope(api_key=api_key, api_secret=api_secret)

        def decrypt_credentials(
            self, account_id: str, envelope: _StubEnvelope
        ) -> types.SimpleNamespace:
            return types.SimpleNamespace(
                api_key=envelope.api_key,
                api_secret=envelope.api_secret,
            )

    secure_secrets.EncryptedSecretEnvelope = _StubEnvelope  # type: ignore[attr-defined]
    secure_secrets.EnvelopeEncryptor = _StubEncryptor  # type: ignore[attr-defined]
    sys.modules["services.secrets.secure_secrets"] = secure_secrets
    setattr(sys.modules["services.secrets"], "secure_secrets", secure_secrets)

_security_path = ROOT / "services" / "common" / "security.py"
_security_spec = importlib.util.spec_from_file_location(
    "services.common.security", _security_path
)
if _security_spec is not None and _security_spec.loader is not None:
    _security_module = importlib.util.module_from_spec(_security_spec)
    sys.modules["services.common.security"] = _security_module
    _security_spec.loader.exec_module(_security_module)

_adapters_path = ROOT / "services" / "common" / "adapters.py"
_adapters_spec = importlib.util.spec_from_file_location(
    "services.common.adapters", _adapters_path
)
if _adapters_spec is not None and _adapters_spec.loader is not None:
    _adapters_module = importlib.util.module_from_spec(_adapters_spec)
    sys.modules["services.common.adapters"] = _adapters_module
    _adapters_spec.loader.exec_module(_adapters_module)

_oms_init_path = ROOT / "services" / "oms" / "__init__.py"
_oms_spec = importlib.util.spec_from_file_location("services.oms", _oms_init_path)
if _oms_spec is not None and _oms_spec.loader is not None:
    _oms_module = importlib.util.module_from_spec(_oms_spec)
    sys.modules["services.oms"] = _oms_module
    _oms_spec.loader.exec_module(_oms_module)

pytest.importorskip("fastapi")

import exchange_adapter
from auth.service import InMemorySessionStore
import services.common.security as security

if "aiohttp" not in sys.modules:
    class _StubSession:
        async def __aenter__(self) -> "_StubSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def close(self) -> None:
            return None

        async def post(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise RuntimeError("aiohttp stub invoked")

        async def get(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise RuntimeError("aiohttp stub invoked")

    class _ClientTimeout:
        def __init__(self, total: float | None = None) -> None:
            self.total = total

    aiohttp_stub = types.SimpleNamespace(
        ClientSession=lambda *args, **kwargs: _StubSession(),
        ClientTimeout=_ClientTimeout,
        ClientError=Exception,
    )
    sys.modules["aiohttp"] = aiohttp_stub


@asynccontextmanager
async def _noop_clients(oms_main):
    async def _credentials() -> dict[str, Any]:
        return {
            "api_key": "demo",
            "api_secret": "demo-secret",
            "metadata": {"rotated_at": datetime.now(timezone.utc).isoformat()},
        }

    class _Stub:
        async def close(self) -> None:
            return None

    stub = _Stub()
    yield oms_main.KrakenClientBundle(
        credential_getter=_credentials,
        ws_client=stub,
        rest_client=stub,
    )


class _AdapterResponse:
    def __init__(self, response) -> None:  # type: ignore[no-untyped-def]
        self._response = response
        self.status_code = response.status_code

    def raise_for_status(self) -> None:
        self._response.raise_for_status()

    def json(self) -> Any:
        return self._response.json()


def _extract_path(url: str) -> str:
    parsed = urlsplit(url)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    return path


@pytest.mark.asyncio
async def test_kraken_adapter_round_trip_through_oms(monkeypatch: pytest.MonkeyPatch) -> None:
    oms_main = importlib.reload(importlib.import_module("services.oms.main"))
    order_ack_cache = importlib.import_module("services.oms.order_ack_cache")
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

    class _NoopKafka:
        def __init__(self, *args, **kwargs) -> None:
            return None

        async def publish(self, *args, **kwargs) -> None:
            return None

        @classmethod
        async def flush_events(cls) -> dict[str, int]:
            return {}

        @classmethod
        def reset(cls, account_id: str | None = None) -> None:
            return None

    class _NoopTimescale:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def record_ack(self, *args, **kwargs) -> None:
            return None

        def record_usage(self, *args, **kwargs) -> None:
            return None

        def record_fill(self, *args, **kwargs) -> None:
            return None

        def record_shadow_fill(self, *args, **kwargs) -> None:
            return None

        @classmethod
        def flush_event_buffers(cls) -> dict[str, int]:
            return {}

    oms_main.MARKET_METADATA = {"BTC-USD": {"tick": 0.01, "lot": 0.0001}}

    monkeypatch.setattr(oms_main, "KafkaNATSAdapter", _NoopKafka)
    monkeypatch.setattr(oms_main, "TimescaleAdapter", _NoopTimescale)
    monkeypatch.setattr(oms_main, "_acquire_kraken_clients", lambda account_id: _noop_clients(oms_main))
    monkeypatch.setattr(oms_main, "_ensure_credentials_valid", lambda credentials: None)

    async def _fake_submit_order(*args, **kwargs):  # type: ignore[no-untyped-def]
        ack = oms_main.OrderAck(
            exchange_order_id="OID-1",
            status="ok",
            filled_qty=None,
            avg_price=None,
            errors=None,
        )
        return ack, "websocket"

    monkeypatch.setattr(oms_main, "_submit_order", _fake_submit_order)
    async def _fake_fetch_open_orders(*args, **kwargs):  # type: ignore[no-untyped-def]
        return []

    async def _fake_fetch_own_trades(*args, **kwargs):  # type: ignore[no-untyped-def]
        return []

    monkeypatch.setattr(oms_main, "_fetch_open_orders", _fake_fetch_open_orders)
    monkeypatch.setattr(oms_main, "_fetch_own_trades", _fake_fetch_own_trades)
    monkeypatch.setattr(oms_main.shadow_oms, "record_real_fill", lambda *a, **k: None)
    monkeypatch.setattr(oms_main.shadow_oms, "generate_shadow_fills", lambda *a, **k: [])

    class _MetadataCache:
        async def get(self, instrument: str) -> dict[str, Any] | None:
            if instrument == "BTC-USD":
                return {"tick": 0.01, "lot": 0.0001}
            return None

        async def refresh(self) -> None:
            return None

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    monkeypatch.setattr(oms_main, "market_metadata_cache", _MetadataCache())
    oms_main.app.state.market_metadata_cache = _MetadataCache()

    security.reload_admin_accounts(["company"])
    store = InMemorySessionStore()
    session = store.create("company")
    oms_main.app.state.session_store = store

    import shared.graceful_shutdown as graceful_shutdown

    monkeypatch.setattr(graceful_shutdown, "install_sigterm_handler", lambda manager: None)
    async def _noop_async(*args, **kwargs):  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr(oms_main.market_metadata_cache, "start", _noop_async)
    monkeypatch.setattr(oms_main.market_metadata_cache, "stop", _noop_async)

    transport = httpx.ASGITransport(app=oms_main.app)

    class _AdapterClient:
        def __init__(self, *args, **kwargs) -> None:
            params = dict(kwargs)
            params.setdefault("transport", transport)
            params.setdefault("base_url", "http://testserver")
            self._client = _REAL_HTTPX_ASYNC_CLIENT(*args, **params)

        async def __aenter__(self) -> httpx.AsyncClient:
            return await self._client.__aenter__()

        async def __aexit__(self, exc_type, exc, tb) -> None:
            await self._client.__aexit__(exc_type, exc, tb)

    monkeypatch.setattr(exchange_adapter.httpx, "AsyncClient", _AdapterClient)

    class _StubSessionManager:
        def __init__(self, token: str) -> None:
            self.token = token
            self.calls: list[str] = []

        async def token_for_account(self, account_id: str) -> str:
            self.calls.append(account_id)
            return self.token

    manager = _StubSessionManager(session.token)
    adapter = exchange_adapter.KrakenAdapter(
        primary_url="http://testserver",
        session_manager=manager,
    )

    payload = {
        "order_id": "CID-1",
        "account_id": "company",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 1000.0,
        "fee": {"currency": "USD", "maker": 0.0, "taker": 0.0},
    }

    result = await adapter.place_order("company", payload)

    assert result["accepted"] is True
    assert manager.calls == ["company"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_duplicate_orders_reuse_cached_ack(monkeypatch: pytest.MonkeyPatch) -> None:
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

    class _RecordingKafka:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id
            self.messages: list[dict[str, Any]] = []

        async def publish(self, *, topic: str, payload: dict[str, Any]) -> None:
            self.messages.append({"topic": topic, "payload": dict(payload)})

        @classmethod
        async def flush_events(cls) -> dict[str, int]:
            return {}

        @classmethod
        def reset(cls, account_id: str | None = None) -> None:
            return None

    class _RecordingTimescale:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id
            self.acks: list[dict[str, Any]] = []

        def record_ack(self, payload: dict[str, Any]) -> None:
            self.acks.append(dict(payload))

        def record_usage(self, notional: float) -> None:
            return None

        def record_fill(self, payload: dict[str, Any]) -> None:
            return None

        def record_shadow_fill(self, payload: dict[str, Any]) -> None:
            return None

        @classmethod
        async def flush_event_buffers(cls) -> dict[str, int]:
            return {}

    metadata = {"BTC-USD": {"tick": 0.1, "lot": 0.01, "native_pair": "XBT/USD"}}

    class _MetadataCache:
        async def get(self, instrument: str) -> dict[str, Any] | None:
            entry = metadata.get(instrument)
            return dict(entry) if entry is not None else None

        async def refresh(self) -> None:
            return None

    submit_calls: list[dict[str, Any]] = []

    async def _stub_submit_order(
        ws_client: Any, rest_client: Any, payload: dict[str, Any]
    ) -> tuple[oms_main.OrderAck, str]:
        submit_calls.append(dict(payload))
        ack = oms_main.OrderAck(
            exchange_order_id=f"EX-{payload['clientOrderId']}",
            status="ok",
            filled_qty=None,
            avg_price=None,
            errors=None,
        )
        return ack, "websocket"

    async def _stub_fetch_open_orders(*_: Any, **__: Any) -> list[dict[str, Any]]:
        return []

    async def _stub_fetch_trades(*_: Any, **__: Any) -> list[dict[str, Any]]:
        return []

    @asynccontextmanager
    async def _stub_clients(_: str):
        async def _credentials() -> dict[str, Any]:
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

    monkeypatch.setattr(oms_main, "KafkaNATSAdapter", _RecordingKafka)
    monkeypatch.setattr(oms_main, "TimescaleAdapter", _RecordingTimescale)
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
    assert len(submit_calls) == 1

    cache = order_ack_cache.get_order_ack_cache("company")
    cached_entry = await cache.get(request.order_id)
    assert cached_entry is not None
    assert cached_entry.payload["txid"] == "EX-OID-1"

    second = await oms_main.place_order(request, account_id="company")
    assert second.accepted is True
    assert len(submit_calls) == 1

    follow_up = request.model_copy(update={"order_id": "OID-2"})
    third = await oms_main.place_order(follow_up, account_id="company")
    assert third.accepted is True
    assert len(submit_calls) == 2
