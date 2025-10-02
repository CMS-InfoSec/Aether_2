import asyncio
import base64
import sys
import types
from decimal import Decimal
from typing import Any, Dict, List

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
    HTTP_502_BAD_GATEWAY=502,
)

fastapi_stub = types.ModuleType('fastapi')
fastapi_stub.FastAPI = _FastAPIStub
fastapi_stub.Depends = _depends
fastapi_stub.HTTPException = _HTTPException
fastapi_stub.Request = _Request
fastapi_stub.Response = object
fastapi_stub.Header = lambda *args, **kwargs: None
fastapi_stub.status = _status
sys.modules['fastapi'] = fastapi_stub

pydantic_stub = types.ModuleType('pydantic')

aiohttp_stub = types.ModuleType('aiohttp')


class _ClientSession:
    async def __aenter__(self) -> '_ClientSession':
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def close(self) -> None:
        return None

    async def post(self, *args, **kwargs):
        raise RuntimeError('aiohttp stub not implemented')


class _ClientTimeout:
    def __init__(self, total: float | None = None) -> None:
        self.total = total


class _ClientError(Exception):
    pass


aiohttp_stub.ClientSession = _ClientSession
aiohttp_stub.ClientTimeout = _ClientTimeout
aiohttp_stub.ClientError = _ClientError
sys.modules['aiohttp'] = aiohttp_stub


websockets_stub = types.ModuleType('websockets')


async def _connect(*args, **kwargs):
    raise RuntimeError('websockets stub not implemented')


websockets_stub.connect = _connect


class _WebSocketClientProtocol:
    pass


class _WebSocketException(Exception):
    pass


websockets_stub.WebSocketClientProtocol = _WebSocketClientProtocol
websockets_stub.exceptions = types.SimpleNamespace(WebSocketException=_WebSocketException)
sys.modules['websockets'] = websockets_stub
sys.modules['websockets.exceptions'] = types.ModuleType('websockets.exceptions')
sys.modules['websockets.exceptions'].WebSocketException = _WebSocketException

metrics_stub = types.ModuleType('metrics')


def _noop(*args, **kwargs) -> None:
    return None


metrics_stub.increment_oms_child_orders_total = _noop
metrics_stub.increment_oms_error_count = _noop
metrics_stub.record_oms_latency = _noop
metrics_stub.setup_metrics = _noop
metrics_stub.record_scaling_state = _noop
metrics_stub.observe_scaling_evaluation = _noop
sys.modules['metrics'] = metrics_stub


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


pydantic_stub.BaseModel = _BaseModel
pydantic_stub.Field = _field
pydantic_stub.field_validator = _field_validator
sys.modules['pydantic'] = pydantic_stub

from services.oms import oms_service
from services.oms.kraken_ws import KrakenWSError, OrderAck, OrderState

def _place_request(**overrides: Any) -> Any:
    defaults = {
        'account_id': '',
        'client_id': '',
        'symbol': 'BTC/USD',
        'side': 'buy',
        'order_type': 'limit',
        'qty': Decimal('0'),
        'limit_px': Decimal('0'),
        'tif': None,
        'flags': [],
        'post_only': False,
        'reduce_only': False,
        'take_profit': None,
        'stop_loss': None,
        'trailing_offset': None,
        'shadow': False,
    }
    defaults.update(overrides)
    return types.SimpleNamespace(**defaults)


def _cancel_request(**overrides: Any) -> Any:
    defaults = {
        'account_id': '',
        'client_id': '',
        'exchange_order_id': None,
    }
    defaults.update(overrides)
    return types.SimpleNamespace(**defaults)


class StubCredentialWatcher:
    _instances: Dict[str, "StubCredentialWatcher"] = {}

    def __init__(self, account_id: str) -> None:
        self.account_id = account_id

    @classmethod
    def instance(cls, account_id: str) -> "StubCredentialWatcher":
        cls._instances.setdefault(account_id, cls(account_id))
        return cls._instances[account_id]

    async def start(self) -> None:  # pragma: no cover - stubbed dependency
        return None

    async def get_credentials(self) -> Dict[str, str]:
        secret = base64.b64encode(b"stub-secret").decode()
        return {
            "api_key": "stub-key",
            "api_secret": secret,
            "ws_token": "stub-token",
            "account_id": self.account_id,
        }

    async def get_metadata(self) -> Dict[str, Any]:
        return {
            "BTC/USD": {
                "lot_decimals": 8,
                "price_increment": "0.5",
                "ordermin": "0.01",
            }
        }


class StubLatencyRouter:
    def __init__(self, account_id: str, *args: Any, **kwargs: Any) -> None:
        self.account_id = account_id
        self._preferred = "websocket"

    async def start(self, ws_client: Any = None, rest_client: Any = None) -> None:
        return None

    async def stop(self) -> None:
        return None

    @property
    def preferred_path(self) -> str:
        return self._preferred

    def update_probe_template(self, payload: Dict[str, Any]) -> None:
        return None

    def record_latency(self, path: str, latency_ms: float) -> None:  # pragma: no cover - noop
        return None

    def status(self) -> Dict[str, Any]:
        return {"ws_latency": None, "rest_latency": None, "preferred_path": self._preferred}


class DummyImpactStore:
    def __init__(self) -> None:
        self.records: List[Dict[str, Any]] = []

    async def record_fill(self, **kwargs: Any) -> None:
        self.records.append(kwargs)

    async def impact_curve(self, **kwargs: Any) -> List[Dict[str, Any]]:
        return []


class DummyBook:
    async def depth(self, side: str, levels: int = 10) -> None:  # pragma: no cover - deterministic
        return None


class DummyOrderBookStore:
    async def ensure_book(self, symbol: str) -> DummyBook:
        return DummyBook()

    async def stop(self) -> None:  # pragma: no cover - noop
        return None


class _BaseKrakenStub:
    def __init__(
        self,
        *,
        credential_getter: Any,
        stream_update_cb: Any,
        server: Any,
        userrefs: Dict[str, str],
        transport: str,
    ) -> None:
        self._credential_getter = credential_getter
        self._stream_update_cb = stream_update_cb
        self._server = server
        self._userrefs = userrefs
        self._transport = transport
        self._account_id: str | None = None

    async def _resolve_account(self) -> str:
        if self._account_id is None:
            credentials = await self._credential_getter()
            self._account_id = credentials.get("account_id", "default")
        return self._account_id

    async def _submit_order(self, payload: Dict[str, Any], *, emit_updates: bool) -> OrderAck:
        account = await self._resolve_account()
        price = float(payload["price"]) if "price" in payload else None
        volume = float(payload.get("volume", 0.0))
        ordertype = payload.get("ordertype", "limit")
        pair = payload.get("pair")
        side = payload.get("type")
        client_id = payload.get("clientOrderId") or payload.get("userref")

        response = await self._server.add_order(
            pair,
            side,
            volume=volume,
            price=price,
            ordertype=ordertype,
            account=account,
            userref=client_id,
        )
        order = response["order"]
        fills = response["fills"]
        self._userrefs[order["order_id"]] = str(client_id or order["order_id"])

        filled_qty = float(order["volume"] - order["remaining"])
        avg_price = None
        if fills:
            notional = sum(float(fill["price"]) * float(fill["volume"]) for fill in fills)
            quantity = sum(float(fill["volume"]) for fill in fills)
            avg_price = notional / quantity if quantity else None

        if emit_updates and self._stream_update_cb is not None:
            await self._emit_updates(order, fills)

        return OrderAck(
            exchange_order_id=order["order_id"],
            status=order["status"],
            filled_qty=filled_qty,
            avg_price=avg_price,
            errors=None,
        )

    async def _emit_updates(self, order: Dict[str, Any], fills: List[Dict[str, Any]]) -> None:
        client_id = str(order.get("userref") or order["order_id"])
        total_filled = float(order["volume"] - order["remaining"])
        if not fills:
            state = OrderState(
                client_order_id=client_id,
                exchange_order_id=order["order_id"],
                status=str(order.get("status", "open")),
                filled_qty=total_filled,
                avg_price=None,
                errors=None,
                transport=self._transport,
            )
            await self._stream_update_cb(state)
            return

        cumulative = 0.0
        notional = 0.0
        for fill in fills:
            volume = float(fill["volume"])
            price = float(fill["price"])
            cumulative += volume
            notional += price * volume
            avg_price = notional / cumulative if cumulative else None
            is_final = total_filled and abs(cumulative - total_filled) < 1e-9
            status = str(order.get("status", "open")) if is_final else "partially_filled"
            state = OrderState(
                client_order_id=client_id,
                exchange_order_id=order["order_id"],
                status=status,
                filled_qty=cumulative,
                avg_price=avg_price,
                errors=None,
                transport=self._transport,
            )
            await self._stream_update_cb(state)

    async def fetch_open_orders_snapshot(self) -> List[Dict[str, Any]]:
        account = await self._resolve_account()
        return [
            order.to_dict()
            for order in self._server._open_orders.values()
            if order.account == account
        ]

    async def fetch_own_trades_snapshot(self) -> List[Dict[str, Any]]:
        account = await self._resolve_account()
        aggregates: Dict[str, Dict[str, Any]] = {}
        for trade in self._server._trades:
            if trade.account != account:
                continue
            entry = aggregates.setdefault(
                trade.order_id,
                {
                    "order_id": trade.order_id,
                    "ordertxid": trade.order_id,
                    "userref": self._userrefs.get(trade.order_id),
                    "pair": trade.pair,
                    "status": "filled",
                    "filled": 0.0,
                    "notional": 0.0,
                },
            )
            entry["filled"] += float(trade.volume)
            entry["notional"] += float(trade.price * trade.volume)

        results: List[Dict[str, Any]] = []
        for order_id, data in aggregates.items():
            filled = data.pop("filled")
            notional = data.pop("notional")
            avg_price = notional / filled if filled else None
            if order_id in self._server._open_orders:
                data["status"] = "partially_filled"
            data["filled"] = filled
            if avg_price is not None:
                data["avg_price"] = avg_price
            results.append(data)
        return results


class StubWSClient(_BaseKrakenStub):
    def __init__(self, *, credential_getter: Any, stream_update_cb: Any, server: Any, userrefs: Dict[str, str], **_: Any) -> None:
        super().__init__(
            credential_getter=credential_getter,
            stream_update_cb=stream_update_cb,
            server=server,
            userrefs=userrefs,
            transport="websocket",
        )
        self.fail_next_add = False
        self.emit_stream_updates = True

    async def ensure_connected(self) -> None:
        return None

    async def subscribe_private(self, channels: List[str]) -> None:  # pragma: no cover - noop
        return None

    async def stream_handler(self) -> None:
        while True:  # pragma: no cover - cancelled in tests
            await asyncio.sleep(3600)

    async def close(self) -> None:
        return None

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        if self.fail_next_add:
            self.fail_next_add = False
            raise KrakenWSError("simulated websocket failure")
        return await self._submit_order(payload, emit_updates=self.emit_stream_updates)

    async def cancel_order(self, payload: Dict[str, Any]) -> OrderAck:
        account = await self._resolve_account()
        order_id = payload.get("txid") or payload.get("order_id")
        response = await self._server.cancel_order(order_id, account=account)
        status = response.get("status", "cancelled")
        ack = OrderAck(
            exchange_order_id=response.get("order_id"),
            status=status,
            filled_qty=None,
            avg_price=None,
            errors=None,
        )
        if self.emit_stream_updates and self._stream_update_cb is not None:
            client_id = self._userrefs.get(order_id, order_id)
            state = OrderState(
                client_order_id=str(client_id),
                exchange_order_id=response.get("order_id"),
                status=status,
                filled_qty=None,
                avg_price=None,
                errors=None,
                transport="websocket",
            )
            await self._stream_update_cb(state)
        return ack

    def heartbeat_age(self) -> float:
        return 0.0


class StubRESTClient(_BaseKrakenStub):
    def __init__(self, *, credential_getter: Any, stream_update_cb: Any | None = None, server: Any, userrefs: Dict[str, str], **_: Any) -> None:
        super().__init__(
            credential_getter=credential_getter,
            stream_update_cb=stream_update_cb,
            server=server,
            userrefs=userrefs,
            transport="rest",
        )
        self.submitted_payloads: List[Dict[str, Any]] = []
        self.cancel_payloads: List[Dict[str, Any]] = []

    async def close(self) -> None:
        return None

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        self.submitted_payloads.append(dict(payload))
        return await self._submit_order(payload, emit_updates=False)

    async def cancel_order(self, payload: Dict[str, Any]) -> OrderAck:
        self.cancel_payloads.append(dict(payload))
        account = await self._resolve_account()
        order_id = payload.get("txid") or payload.get("order_id")
        response = await self._server.cancel_order(order_id, account=account)
        return OrderAck(
            exchange_order_id=response.get("order_id"),
            status=response.get("status", "cancelled"),
            filled_qty=None,
            avg_price=None,
            errors=None,
        )

    async def open_orders(self) -> Dict[str, Any]:
        orders = await self.fetch_open_orders_snapshot()
        payload = {order["order_id"]: order for order in orders}
        return {"result": {"open": payload}}

    async def own_trades(self, **_: Any) -> Dict[str, Any]:
        trades = await self.fetch_own_trades_snapshot()
        payload = {index: trade for index, trade in enumerate(trades)}
        return {"result": {"trades": payload}}


@pytest.fixture(autouse=True)
def stub_environment(monkeypatch: pytest.MonkeyPatch, kraken_mock_server: Any) -> None:
    userrefs: Dict[str, str] = {}
    StubCredentialWatcher._instances = {}

    monkeypatch.setattr(oms_service, "CredentialWatcher", StubCredentialWatcher)
    monkeypatch.setattr(oms_service, "LatencyRouter", StubLatencyRouter)
    monkeypatch.setattr(oms_service, "order_book_store", DummyOrderBookStore())
    dummy_impact = DummyImpactStore()
    monkeypatch.setattr(oms_service, "impact_store", dummy_impact)

    def _ws_factory(**kwargs: Any) -> StubWSClient:
        return StubWSClient(server=kraken_mock_server, userrefs=userrefs, **kwargs)

    def _rest_factory(**kwargs: Any) -> StubRESTClient:
        return StubRESTClient(server=kraken_mock_server, userrefs=userrefs, **kwargs)

    monkeypatch.setattr(oms_service, "KrakenWSClient", _ws_factory)
    monkeypatch.setattr(oms_service, "KrakenRESTClient", _rest_factory)


def test_partial_fill_and_cancel(kraken_mock_server: Any) -> None:
    asyncio.run(_run_partial_fill_and_cancel(kraken_mock_server))


async def _run_partial_fill_and_cancel(kraken_mock_server: Any) -> None:
    account = oms_service.AccountContext("ACC-PARTIAL")
    account._reconcile_interval = 0
    await account.start()

    await kraken_mock_server.add_order(
        "BTC/USD",
        "sell",
        volume=0.2,
        price=30000.0,
        account="liquidity",
    )
    await kraken_mock_server.add_order(
        "BTC/USD",
        "sell",
        volume=0.2,
        price=30005.0,
        account="liquidity",
    )

    request = _place_request(
        account_id="ACC-PARTIAL",
        client_id="CID-123",
        symbol="BTC/USD",
        side="buy",
        order_type="limit",
        qty=Decimal("0.6"),
        limit_px=Decimal("30005"),
    )

    response = await account.place_order(request)
    assert response.status == "partially_filled"
    assert response.filled_qty == Decimal("0.4")

    record = await account.lookup("CID-123")
    assert record is not None
    assert record.result.status == "partially_filled"
    assert record.result.filled_qty == Decimal("0.4")
    assert record.requested_qty == Decimal("0.6")

    cancel = _cancel_request(
        account_id="ACC-PARTIAL",
        client_id="CID-123-cancel",
        exchange_order_id=str(response.exchange_order_id),
    )

    cancel_result = await account.cancel_order(cancel)
    assert cancel_result.status.startswith("cancel")

    record_after_cancel = await account.lookup("CID-123")
    assert record_after_cancel is not None
    assert record_after_cancel.result.status in {"canceled", "cancelled"}

    await account.close()


def test_rest_fallback_preserves_idempotency(kraken_mock_server: Any) -> None:
    asyncio.run(_run_rest_fallback_test(kraken_mock_server))


async def _run_rest_fallback_test(kraken_mock_server: Any) -> None:
    account = oms_service.AccountContext("ACC-REST")
    account._reconcile_interval = 0
    await account.start()

    assert account.ws_client is not None
    assert account.rest_client is not None
    account.ws_client.fail_next_add = True  # type: ignore[attr-defined]

    request = _place_request(
        account_id="ACC-REST",
        client_id="REST-001",
        symbol="BTC/USD",
        side="buy",
        order_type="limit",
        qty=Decimal("0.2"),
        limit_px=Decimal("30050"),
    )

    response = await account.place_order(request)
    assert response.transport == "rest"

    rest_payloads = account.rest_client.submitted_payloads  # type: ignore[attr-defined]
    assert rest_payloads, "expected REST client to record payloads"
    assert rest_payloads[0]["idempotencyKey"] == "REST-001"

    await account.close()


def test_periodic_reconciliation_updates_state(kraken_mock_server: Any) -> None:
    asyncio.run(_run_periodic_reconciliation_test(kraken_mock_server))


async def _run_periodic_reconciliation_test(kraken_mock_server: Any) -> None:
    account = oms_service.AccountContext("ACC-RECON")
    account._reconcile_interval = 0.05
    await account.start()

    assert account.ws_client is not None
    account.ws_client.emit_stream_updates = False  # type: ignore[attr-defined]

    request = _place_request(
        account_id="ACC-RECON",
        client_id="RECON-1",
        symbol="BTC/USD",
        side="sell",
        order_type="limit",
        qty=Decimal("0.3"),
        limit_px=Decimal("30010"),
    )

    place_response = await account.place_order(request)
    assert place_response.status in {"accepted", "open"}

    resting = await account.lookup("RECON-1")
    assert resting is not None
    assert resting.result.status in {"accepted", "open"}

    await kraken_mock_server.add_order(
        "BTC/USD",
        "buy",
        volume=0.3,
        price=30020.0,
        account="aggressor",
    )

    for _ in range(20):
        await asyncio.sleep(0.05)
        current = await account.lookup("RECON-1")
        if current and current.result.status == "filled":
            break
    current = await account.lookup("RECON-1")
    assert current is not None
    assert current.result.status == "filled"
    assert current.result.filled_qty == Decimal("0.3")

    await account.close()
