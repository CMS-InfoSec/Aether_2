from __future__ import annotations

import asyncio
import sys
import types
import importlib.util
from pathlib import Path
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pytest


class _FastAPIStub:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        return None

    def on_event(self, event: str) -> Any:
        def decorator(func: Any) -> Any:
            return func

        return decorator

    def post(self, path: str, **_: Any) -> Any:
        def decorator(func: Any) -> Any:
            return func

        return decorator

    def get(self, path: str, **_: Any) -> Any:
        def decorator(func: Any) -> Any:
            return func

        return decorator


def _depends(callable_obj: Any) -> Any:
    return callable_obj


class _HTTPException(Exception):
    def __init__(self, status_code: int, detail: Any = None) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class _Request:
    def __init__(self, headers: Dict[str, Any] | None = None) -> None:
        self.headers = headers or {}


_status = types.SimpleNamespace(
    HTTP_400_BAD_REQUEST=400,
    HTTP_401_UNAUTHORIZED=401,
    HTTP_403_FORBIDDEN=403,
    HTTP_404_NOT_FOUND=404,
    HTTP_408_REQUEST_TIMEOUT=408,
    HTTP_409_CONFLICT=409,
    HTTP_410_GONE=410,
    HTTP_412_PRECONDITION_FAILED=412,
    HTTP_422_UNPROCESSABLE_ENTITY=422,
    HTTP_429_TOO_MANY_REQUESTS=429,
    HTTP_500_INTERNAL_SERVER_ERROR=500,
    HTTP_502_BAD_GATEWAY=502,
    HTTP_503_SERVICE_UNAVAILABLE=503,
)


fastapi_stub = types.ModuleType("fastapi")
fastapi_stub.FastAPI = _FastAPIStub
fastapi_stub.Depends = _depends
fastapi_stub.HTTPException = _HTTPException
fastapi_stub.Request = _Request
fastapi_stub.Response = object
fastapi_stub.Header = lambda *args, **kwargs: None
fastapi_stub.status = _status
sys.modules["fastapi"] = fastapi_stub


class _BaseModel:
    def __init__(self, **data: Any) -> None:
        for key, value in data.items():
            setattr(self, key, value)


def _field(default: Any = None, *args: Any, **kwargs: Any) -> Any:
    return default


def _field_validator(*args: Any, **kwargs: Any) -> Any:
    def decorator(func: Any) -> Any:
        return func

    return decorator


pydantic_stub = types.ModuleType("pydantic")
pydantic_stub.BaseModel = _BaseModel
pydantic_stub.Field = _field
pydantic_stub.field_validator = _field_validator
sys.modules.setdefault("pydantic", pydantic_stub)


class _ClientSession:
    async def __aenter__(self) -> "_ClientSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def close(self) -> None:
        return None

    async def post(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("aiohttp stub not implemented")


class _ClientTimeout:
    def __init__(self, total: float | None = None) -> None:
        self.total = total


class _ClientError(Exception):
    pass


aiohttp_stub = types.ModuleType("aiohttp")
aiohttp_stub.ClientSession = _ClientSession
aiohttp_stub.ClientTimeout = _ClientTimeout
aiohttp_stub.ClientError = _ClientError
sys.modules.setdefault("aiohttp", aiohttp_stub)


class _WebSocketClientProtocol:
    pass


class _WebSocketException(Exception):
    pass


async def _connect(*args: Any, **kwargs: Any) -> Any:
    raise RuntimeError("websockets stub not implemented")


websockets_stub = types.ModuleType("websockets")
websockets_stub.connect = _connect
websockets_stub.WebSocketClientProtocol = _WebSocketClientProtocol
websockets_stub.exceptions = types.SimpleNamespace(WebSocketException=_WebSocketException)
sys.modules.setdefault("websockets", websockets_stub)

websockets_exceptions_stub = types.ModuleType("websockets.exceptions")
websockets_exceptions_stub.WebSocketException = _WebSocketException
sys.modules.setdefault("websockets.exceptions", websockets_exceptions_stub)


def _noop(*args: Any, **kwargs: Any) -> None:
    return None


metrics_stub = types.ModuleType("metrics")
metrics_stub.increment_oms_child_orders_total = _noop
metrics_stub.increment_oms_error_count = _noop
metrics_stub.increment_oms_stale_feed = _noop
metrics_stub.increment_oms_auth_failures = _noop
metrics_stub.record_oms_latency = _noop
metrics_stub.record_oms_submit_ack = _noop
metrics_stub.record_ws_latency = _noop
metrics_stub.setup_metrics = _noop
metrics_stub.get_request_id = lambda: None

class _RegistryStub:
    def register(self, *args: Any, **kwargs: Any) -> None:
        return None

metrics_stub._REGISTRY = _RegistryStub()
sys.modules.setdefault("metrics", metrics_stub)


reconcile_stub = types.ModuleType("services.oms.reconcile")
reconcile_stub.register = lambda app, manager: None
sys.modules.setdefault("services.oms.reconcile", reconcile_stub)


MODULE_PATH = Path(__file__).resolve().parents[3] / "services" / "oms" / "oms_service.py"
MODULE_SPEC = importlib.util.spec_from_file_location("services.oms.oms_service", MODULE_PATH)
assert MODULE_SPEC and MODULE_SPEC.loader
oms_service = importlib.util.module_from_spec(MODULE_SPEC)


async def _require_authorized_account(request: _Request) -> str:
    return request.headers.get("X-Account-ID", "ACC-TEST")


oms_service.require_authorized_account = _require_authorized_account  # type: ignore[attr-defined]
sys.modules.setdefault("services.oms.oms_service", oms_service)
MODULE_SPEC.loader.exec_module(oms_service)

from services.oms.oms_service import AccountContext, OMSPlaceRequest
from services.oms.kraken_ws import OrderAck
from shared.correlation import CorrelationContext
from shared.simulation import sim_broker, sim_mode_state


class _StubCredentialWatcher:
    _instances: Dict[str, "_StubCredentialWatcher"] = {}
    metadata: Dict[str, Any] | None = None

    def __init__(self, account_id: str) -> None:
        self.account_id = account_id

    @classmethod
    def instance(cls, account_id: str) -> "_StubCredentialWatcher":
        existing = cls._instances.get(account_id)
        if existing is None:
            existing = cls(account_id)
            cls._instances[account_id] = existing
        return existing

    async def start(self) -> None:
        return None

    async def get_credentials(self) -> Dict[str, str]:
        return {"api_key": "stub", "api_secret": "stub"}

    async def get_metadata(self) -> Dict[str, Any] | None:
        return self.metadata


class _StubLatencyRouter:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id

    async def start(self, ws_client: Any = None, rest_client: Any = None) -> None:
        return None

    async def stop(self) -> None:
        return None

    def update_probe_template(self, payload: Dict[str, Any]) -> None:
        return None

    def record_latency(self, path: str, latency_ms: float) -> None:
        return None

    @property
    def preferred_path(self) -> str:
        return "websocket"

    def status(self) -> Dict[str, Optional[float] | str]:
        return {"ws_latency": None, "rest_latency": None, "preferred_path": "websocket"}


class _StubImpactStore:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    async def record_fill(self, **payload: Any) -> None:
        self.calls.append(payload)

    async def impact_curve(self, **_: Any) -> List[Dict[str, Any]]:
        return []


class _RecordingWSClient:
    def __init__(
        self,
        *,
        credential_getter: Any,
        stream_update_cb: Any,
        rest_client: Any = None,
        **_: Any,
    ) -> None:
        self._credential_getter = credential_getter
        self._stream_update_cb = stream_update_cb
        self._rest_client = rest_client
        self.add_calls: List[Dict[str, Any]] = []

    async def ensure_connected(self) -> None:
        return None

    async def subscribe_private(self, channels: List[str]) -> None:
        return None

    async def stream_handler(self) -> None:  # pragma: no cover - compatibility shim
        return None

    async def close(self) -> None:  # pragma: no cover - compatibility shim
        return None

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        self.add_calls.append(dict(payload))
        return OrderAck(
            exchange_order_id="WS-1",
            status="accepted",
            filled_qty=None,
            avg_price=None,
            errors=None,
        )

    def heartbeat_age(self) -> Optional[float]:
        return 0.1

    def set_rest_client(self, rest_client: Any) -> None:
        self._rest_client = rest_client


class _StubRESTClient:
    def __init__(self, *, credential_getter: Any) -> None:
        self._credential_getter = credential_getter
        self.add_calls: List[Dict[str, Any]] = []

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        self.add_calls.append(dict(payload))
        return OrderAck(
            exchange_order_id="REST-1",
            status="accepted",
            filled_qty=None,
            avg_price=None,
            errors=None,
        )

    async def close(self) -> None:  # pragma: no cover - compatibility shim
        return None


class _OrderBook:
    async def is_stale(self, threshold: float) -> bool:
        assert threshold >= 0
        return False

    async def depth(self, side: str, levels: int = 10) -> Decimal:
        assert levels > 0
        return Decimal("1")

    async def last_update(self) -> float:
        return 0.0


class _BookStore:
    def __init__(self, book: Any) -> None:
        self.book = book

    async def ensure_book(self, symbol: str) -> Any:
        return self.book


@pytest.fixture
def pair_metadata() -> Dict[str, Any]:
    return {
        "BTC/USD": {
            "price_increment": "0.1",
            "lot_step": "0.0001",
        }
    }


@pytest.fixture
def account_setup(
    monkeypatch: pytest.MonkeyPatch, pair_metadata: Dict[str, Any]
) -> Tuple[
    AccountContext,
    List[_RecordingWSClient],
    List[_StubRESTClient],
    _StubImpactStore,
]:
    _StubCredentialWatcher._instances = {}
    _StubCredentialWatcher.metadata = pair_metadata

    monkeypatch.setattr(oms_service, "CredentialWatcher", _StubCredentialWatcher)
    monkeypatch.setattr(oms_service, "LatencyRouter", _StubLatencyRouter)
    impact_store = _StubImpactStore()
    monkeypatch.setattr(oms_service, "impact_store", impact_store)

    ws_clients: List[_RecordingWSClient] = []
    rest_clients: List[_StubRESTClient] = []

    def _ws_factory(**kwargs: Any) -> _RecordingWSClient:
        client = _RecordingWSClient(**kwargs)
        ws_clients.append(client)
        return client

    def _rest_factory(**kwargs: Any) -> _StubRESTClient:
        client = _StubRESTClient(**kwargs)
        rest_clients.append(client)
        return client

    monkeypatch.setattr(oms_service, "KrakenWSClient", _ws_factory)
    monkeypatch.setattr(oms_service, "KrakenRESTClient", _rest_factory)
    monkeypatch.setattr(oms_service, "order_book_store", _BookStore(_OrderBook()))
    monkeypatch.setattr(oms_service, "increment_oms_child_orders_total", lambda *_, **__: None)
    monkeypatch.setattr(oms_service, "record_oms_latency", lambda *_, **__: None)

    class _StubIdempotencyStore:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id
            self._cache: Dict[str, Any] = {}

        async def get_or_create(
            self, cache_key: str, producer: Any
        ) -> Tuple[Any, bool]:
            if cache_key in self._cache:
                closer = getattr(producer, "close", None)
                if callable(closer):
                    closer()
                return self._cache[cache_key], True
            result = await producer
            self._cache[cache_key] = result
            return result, False

    monkeypatch.setattr(oms_service, "_IdempotencyStore", _StubIdempotencyStore)

    sim_mode_state.deactivate()
    asyncio.run(sim_broker.clear())

    account = AccountContext("ACC-TEST")
    return account, ws_clients, rest_clients, impact_store


@pytest.fixture
def off_tick_request() -> OMSPlaceRequest:
    request = OMSPlaceRequest(
        account_id="ACC-TEST",
        client_id="CID-OFF-TICK",
        symbol="BTC/USD",
        side="buy",
        order_type="limit",
        qty=Decimal("0.3215"),
        limit_px=Decimal("20000.123"),
        take_profit=Decimal("20010.789"),
        stop_loss=Decimal("19990.432"),
        trailing_offset=Decimal("1.237"),
        flags=[],
    )
    request.shadow = False  # type: ignore[attr-defined]
    return request


def test_place_order_snaps_take_profit_stop_loss_and_trailing(
    account_setup: Tuple[
        AccountContext,
        List[_RecordingWSClient],
        List[_StubRESTClient],
        _StubImpactStore,
    ],
    off_tick_request: OMSPlaceRequest,
) -> None:
    account, ws_clients, _, _ = account_setup

    response = asyncio.run(account.place_order(off_tick_request))
    assert response.status == "accepted"

    assert ws_clients, "Expected websocket client to record submission"
    payload = ws_clients[0].add_calls[0]

    assert payload["price"] == "20000.1"
    assert payload["takeProfit"] == "20010.8"
    assert payload["stopLoss"] == "19990.4"
    assert payload["trailingStopOffset"] == "1.2"

    asyncio.run(account.close())


def test_simulated_place_order_marks_records_and_reuses_idempotency(
    account_setup: Tuple[
        AccountContext,
        List[_RecordingWSClient],
        List[_StubRESTClient],
        _StubImpactStore,
    ],
    off_tick_request: OMSPlaceRequest,
) -> None:
    account, ws_clients, rest_clients, impact_store = account_setup

    off_tick_request.pre_trade_mid_px = Decimal("20000")
    sim_mode_state.activate()

    try:
        with CorrelationContext("corr-sim-1"):
            first = asyncio.run(account.place_order(off_tick_request))

        assert first.transport == "simulation"
        assert first.reused is False

        if ws_clients:
            assert ws_clients[0].add_calls == []
        if rest_clients:
            assert rest_clients[0].add_calls == []

        record = asyncio.run(account.lookup(off_tick_request.client_id))
        assert record is not None
        assert record.origin == "SIM"
        assert record.children and record.children[0].origin == "SIM"

        assert impact_store.calls, "expected simulated fill to be recorded"
        fill_payload = impact_store.calls[-1]
        assert fill_payload.get("simulated") is True

        cached = sim_broker.inspect(account.account_id, off_tick_request.client_id)
        assert cached is not None
        assert cached.payload["clientOrderId"] == off_tick_request.client_id
        assert cached.payload["idempotencyKey"] == off_tick_request.client_id
        assert cached.correlation_id == "corr-sim-1"

        with CorrelationContext("corr-sim-2"):
            second = asyncio.run(account.place_order(off_tick_request))

        assert second.reused is True
        assert second.exchange_order_id == first.exchange_order_id
        assert len(impact_store.calls) == 1
    finally:
        sim_mode_state.deactivate()
        asyncio.run(sim_broker.clear())
        asyncio.run(account.close())
