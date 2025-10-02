from __future__ import annotations

import sys

import asyncio
import builtins
import types
from decimal import Decimal

import pytest


fastapi_stub = types.ModuleType("fastapi")


class _FastAPI:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self.user_middleware: list[object] = []
        self.routes: list[types.SimpleNamespace] = []

    def add_middleware(self, middleware: type, **_: object) -> None:
        self.user_middleware.append(types.SimpleNamespace(cls=middleware))

    def get(self, path: str, **_: object):
        def decorator(func):
            self.routes.append(types.SimpleNamespace(path=path))
            return func

        return decorator

    def post(self, path: str, **_: object):
        def decorator(func):
            self.routes.append(types.SimpleNamespace(path=path))
            return func

        return decorator

    def on_event(self, event: str):
        def decorator(func):
            return func

        return decorator

    def add_event_handler(self, event: str, handler: object) -> None:
        return None

    def include_router(self, router: object) -> None:
        return None


class _Request:
    pass


class _Response:
    def __init__(self, content: object = None, media_type: str | None = None) -> None:
        self.content = content
        self.media_type = media_type


def _depends(callable_obj):
    return callable_obj


class _HTTPException(Exception):
    def __init__(self, status_code: int, detail: object | None = None) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class _APIRouter:
    def __init__(self, *args: object, **kwargs: object) -> None:
        return None

    def get(self, path: str, **_: object):
        def decorator(func):
            return func

        return decorator

    def post(self, path: str, **_: object):
        def decorator(func):
            return func

        return decorator


fastapi_stub.FastAPI = _FastAPI
fastapi_stub.Request = _Request
fastapi_stub.Response = _Response
fastapi_stub.Depends = _depends
fastapi_stub.HTTPException = _HTTPException
fastapi_stub.Header = lambda *args, **kwargs: None
fastapi_stub.status = types.SimpleNamespace(
    HTTP_400_BAD_REQUEST=400,
    HTTP_401_UNAUTHORIZED=401,
    HTTP_403_FORBIDDEN=403,
    HTTP_404_NOT_FOUND=404,
    HTTP_502_BAD_GATEWAY=502,
)
fastapi_stub.APIRouter = _APIRouter
sys.modules.setdefault("fastapi", fastapi_stub)


starlette_stub = types.ModuleType("starlette")
starlette_middleware = types.ModuleType("starlette.middleware")
starlette_base = types.ModuleType("starlette.middleware.base")


class _BaseHTTPMiddleware:
    pass


starlette_base.BaseHTTPMiddleware = _BaseHTTPMiddleware
starlette_middleware.base = starlette_base
starlette_stub.middleware = starlette_middleware
sys.modules.setdefault("starlette", starlette_stub)
sys.modules.setdefault("starlette.middleware", starlette_middleware)
sys.modules.setdefault("starlette.middleware.base", starlette_base)


pydantic_stub = types.ModuleType("pydantic")


class _BaseModel:
    def __init__(self, **data: object) -> None:
        for key, value in data.items():
            setattr(self, key, value)


def _Field(
    *args: object,
    default: object = None,
    default_factory: object | None = None,
    **kwargs: object,
) -> object:
    if callable(default_factory):
        return default_factory()
    return default


def _field_validator(*args: object, **kwargs: object):
    def decorator(func):
        return func

    return decorator


pydantic_stub.BaseModel = _BaseModel
pydantic_stub.Field = _Field
pydantic_stub.field_validator = _field_validator
sys.modules.setdefault("pydantic", pydantic_stub)


aiohttp_stub = types.ModuleType("aiohttp")


class _ClientSession:
    async def __aenter__(self) -> "_ClientSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def close(self) -> None:
        return None

    async def post(self, *args: object, **kwargs: object) -> None:  # pragma: no cover
        raise RuntimeError("aiohttp stub not implemented")


class _ClientTimeout:
    def __init__(self, total: float | None = None) -> None:
        self.total = total


class _ClientError(Exception):
    pass


aiohttp_stub.ClientSession = _ClientSession
aiohttp_stub.ClientTimeout = _ClientTimeout
aiohttp_stub.ClientError = _ClientError
sys.modules.setdefault("aiohttp", aiohttp_stub)


websockets_stub = types.ModuleType("websockets")


async def _connect(*args: object, **kwargs: object) -> None:  # pragma: no cover
    raise RuntimeError("websockets stub not implemented")


websockets_stub.connect = _connect


class _WebSocketClientProtocol:
    pass


class _WebSocketException(Exception):
    pass


websockets_stub.WebSocketClientProtocol = _WebSocketClientProtocol
websockets_stub.exceptions = types.SimpleNamespace(WebSocketException=_WebSocketException)
sys.modules.setdefault("websockets", websockets_stub)
exceptions_module = types.ModuleType("websockets.exceptions")
exceptions_module.WebSocketException = _WebSocketException
sys.modules.setdefault("websockets.exceptions", exceptions_module)


httpx_stub = types.ModuleType("httpx")


class _HTTPXAsyncClient:
    async def __aenter__(self) -> "_HTTPXAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def post(self, *args: object, **kwargs: object) -> "_HTTPXResponse":  # pragma: no cover
        return _HTTPXResponse()


class _HTTPXResponse:
    def raise_for_status(self) -> None:
        return None


class _HTTPXError(Exception):
    pass


httpx_stub.AsyncClient = _HTTPXAsyncClient
httpx_stub.HTTPError = _HTTPXError
sys.modules.setdefault("httpx", httpx_stub)


prometheus_stub = types.ModuleType("prometheus_client")


class _ValueWrapper:
    def __init__(self) -> None:
        self._value = 0.0

    def get(self) -> float:
        return self._value

    def set(self, value: float) -> None:
        self._value = value


class _MetricSample:
    def __init__(self) -> None:
        self._value = _ValueWrapper()

    def inc(self, amount: float = 1.0) -> None:
        self._value.set(self._value.get() + amount)

    def set(self, value: float) -> None:
        self._value.set(value)

    def observe(self, value: float) -> None:
        self._value.set(value)


class _Metric:
    def __init__(self, name: str, documentation: str, labelnames: tuple[str, ...] | list[str] = (), registry: object | None = None, **_: object) -> None:
        self._labelnames = tuple(labelnames)
        self._samples: dict[tuple[object, ...], _MetricSample] = {}

    def labels(self, **kwargs: object) -> _MetricSample:
        key = tuple(kwargs.get(label) for label in self._labelnames)
        sample = self._samples.get(key)
        if sample is None:
            sample = _MetricSample()
            self._samples[key] = sample
        return sample


class _CollectorRegistry:
    pass


def _generate_latest(registry: object) -> bytes:  # pragma: no cover - debugging helper
    return b""


prometheus_stub.CollectorRegistry = _CollectorRegistry
prometheus_stub.Counter = _Metric
prometheus_stub.Gauge = _Metric
prometheus_stub.Histogram = _Metric
prometheus_stub.generate_latest = _generate_latest
prometheus_stub.CONTENT_TYPE_LATEST = "text/plain; version=0.0.4"
sys.modules["prometheus_client"] = prometheus_stub


async def _stub_require_authorized_account(request: object) -> str:
    return "ACC-METRIC"


builtins.require_authorized_account = _stub_require_authorized_account


import metrics
from services.oms import oms_service
from services.oms.kraken_ws import OrderAck


class StubCredentials:
    async def get_metadata(self) -> dict[str, object]:
        return {
            "BTC/USD": {
                "lot_decimals": 8,
                "price_increment": "1",
                "ordermin": "0.001",
            }
        }

    async def get_credentials(self) -> dict[str, str]:
        return {
            "api_key": "stub-key",
            "api_secret": "stub-secret",
            "ws_token": "stub-token",
        }


def test_multi_child_slice_increments_metric() -> None:
    asyncio.run(_run_multi_child_slice_increments_metric())


async def _run_multi_child_slice_increments_metric() -> None:
    metrics.init_metrics(metrics._SERVICE_NAME)

    class DummyBackend:
        def __init__(self) -> None:
            self._futures: dict[tuple[str, str], asyncio.Future] = {}

        async def reserve(
            self, account_id: str, key: str, ttl_seconds: float
        ) -> tuple[asyncio.Future, bool]:
            future: asyncio.Future = asyncio.Future()
            self._futures[(account_id, key)] = future
            return future, True

        async def fail(self, account_id: str, key: str, exc: Exception) -> None:
            future = self._futures.get((account_id, key))
            if future is not None and not future.done():
                future.set_exception(exc)

        async def complete(
            self,
            account_id: str,
            key: str,
            result: oms_service.OMSOrderStatusResponse,
            ttl_seconds: float,
        ) -> None:
            future = self._futures.get((account_id, key))
            if future is not None and not future.done():
                future.set_result(result)

    dummy_backend_factory = lambda account_id: DummyBackend()
    oms_service.get_idempotency_backend = dummy_backend_factory  # type: ignore[attr-defined]
    from services.oms import idempotency_backend as backend_module

    backend_module.get_idempotency_backend = dummy_backend_factory  # type: ignore[attr-defined]
    from services.oms import idempotency_store as store_module

    store_module.get_idempotency_backend = dummy_backend_factory  # type: ignore[attr-defined]

    account = oms_service.AccountContext("ACC-METRIC")
    account.credentials = StubCredentials()
    account.ws_client = object()  # type: ignore[assignment]
    account.rest_client = object()  # type: ignore[assignment]
    account._record_trade_impact = types.MethodType(  # type: ignore[attr-defined]
        lambda self, record: asyncio.sleep(0), account
    )

    async def _fake_start(self: oms_service.AccountContext) -> None:
        return None

    account.start = types.MethodType(_fake_start, account)

    child_quantities = [Decimal("0.15"), Decimal("0.1"), Decimal("0.05")]

    def _fixed_plan(
        self: oms_service.AccountContext,
        request: oms_service.OMSPlaceRequest,
        qty: Decimal,
        metadata: dict[str, object] | None,
        depth: Decimal | None,
    ) -> list[Decimal]:
        return list(child_quantities)

    account._plan_child_quantities = types.MethodType(_fixed_plan, account)

    async def _fake_submit(
        self: oms_service.AccountContext,
        payload: dict[str, object],
        symbol: str,
        base_client_id: str,
    ) -> tuple[OrderAck, str, str, float]:
        ack = OrderAck(
            exchange_order_id=f"EX-{base_client_id}",
            status="accepted",
            filled_qty=0.0,
            avg_price=None,
            errors=None,
        )
        return ack, "websocket", base_client_id, 4.2

    account._submit_order_with_preference = types.MethodType(
        _fake_submit, account
    )

    class StubBook:
        async def depth(self, side: str, levels: int = 10) -> Decimal:
            return Decimal("1")

    class StubOrderBookStore:
        async def ensure_book(self, symbol: str) -> StubBook:
            return StubBook()

    oms_service.order_book_store = StubOrderBookStore()  # type: ignore[assignment]

    metric = metrics._METRICS["oms_child_orders_total"]
    service_name = metrics._SERVICE_NAME
    prior_value = metric.labels(
        service=service_name,
        account="ACC-METRIC",
        symbol="BTC/USD",
        transport="websocket",
    )._value.get()

    request = oms_service.OMSPlaceRequest(
        account_id="ACC-METRIC",
        client_id="CID-CHILD",
        symbol="BTC/USD",
        side="buy",
        order_type="limit",
        qty=Decimal("0.30"),
        limit_px=Decimal("30000"),
        flags=[],
        shadow=False,
    )

    response = await account.place_order(request)
    assert response.transport == "websocket"

    updated_value = metric.labels(
        service=service_name,
        account="ACC-METRIC",
        symbol="BTC/USD",
        transport="websocket",
    )._value.get()

    assert updated_value == pytest.approx(prior_value + len(child_quantities))
    assert response.status in {"accepted", "placed", "open"}
