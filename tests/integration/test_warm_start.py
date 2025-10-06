from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import types
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Optional

import pytest

if "cryptography" not in sys.modules:
    cryptography_stub = types.ModuleType("cryptography")
    hazmat_stub = types.ModuleType("cryptography.hazmat")
    primitives_stub = types.ModuleType("cryptography.hazmat.primitives")
    ciphers_stub = types.ModuleType("cryptography.hazmat.primitives.ciphers")
    aead_stub = types.ModuleType("cryptography.hazmat.primitives.ciphers.aead")

    class _AESGCM:  # pragma: no cover - lightweight stub
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            return None

        @staticmethod
        def generate_key(bit_length: int = 256) -> bytes:  # pragma: no cover - deterministic stub
            return b"0" * (bit_length // 8)

        def encrypt(self, nonce: bytes, data: bytes, associated_data: bytes | None = None) -> bytes:  # noqa: D401 - noop
            return data

        def decrypt(self, nonce: bytes, data: bytes, associated_data: bytes | None = None) -> bytes:  # noqa: D401 - noop
            return data

    aead_stub.AESGCM = _AESGCM
    ciphers_stub.aead = aead_stub
    primitives_stub.ciphers = ciphers_stub
    hazmat_stub.primitives = primitives_stub
    cryptography_stub.hazmat = hazmat_stub

    sys.modules["cryptography"] = cryptography_stub
    sys.modules["cryptography.hazmat"] = hazmat_stub
    sys.modules["cryptography.hazmat.primitives"] = primitives_stub
    sys.modules["cryptography.hazmat.primitives.ciphers"] = ciphers_stub
    sys.modules["cryptography.hazmat.primitives.ciphers.aead"] = aead_stub

if "fastapi" not in sys.modules:
    fastapi_stub = types.ModuleType("fastapi")

    class _FastAPIStub:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            return None

        def on_event(self, event: str):  # noqa: D401 - decorator shim
            def decorator(func: Any) -> Any:
                return func

            return decorator

        def post(self, path: str, **_: Any):  # noqa: D401 - decorator shim
            def decorator(func: Any) -> Any:
                return func

            return decorator

        def get(self, path: str, **_: Any):  # noqa: D401 - decorator shim
            def decorator(func: Any) -> Any:
                return func

            return decorator

    def _depends(callable_obj: Any) -> Any:  # pragma: no cover - shim for FastAPI Depends
        return callable_obj

    class _HTTPException(Exception):
        def __init__(self, status_code: int, detail: Any = None) -> None:
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class _Request:
        def __init__(self, headers: Optional[Dict[str, Any]] = None) -> None:
            self.headers = headers or {}

    _status = types.SimpleNamespace(
        HTTP_400_BAD_REQUEST=400,
        HTTP_401_UNAUTHORIZED=401,
        HTTP_403_FORBIDDEN=403,
        HTTP_404_NOT_FOUND=404,
        HTTP_502_BAD_GATEWAY=502,
        HTTP_423_LOCKED=423,
    )

    fastapi_stub.FastAPI = _FastAPIStub
    fastapi_stub.Depends = _depends
    fastapi_stub.HTTPException = _HTTPException
    fastapi_stub.Request = _Request
    fastapi_stub.status = _status
    fastapi_stub.Header = lambda default=None, **__: default  # pragma: no cover - simple shim
    sys.modules["fastapi"] = fastapi_stub

if "pydantic" not in sys.modules:
    pydantic_stub = types.ModuleType("pydantic")

    class _BaseModel:
        def __init__(self, **data: Any) -> None:
            for key, value in data.items():
                setattr(self, key, value)

        def model_dump(self, *_, **__) -> Dict[str, Any]:  # noqa: D401 - basic serialiser
            return {key: value for key, value in self.__dict__.items() if not key.startswith("_")}

        def model_copy(self, *_, **__) -> "_BaseModel":  # pragma: no cover - simple identity
            return self

    def _Field(default: Any = None, *_, default_factory: Optional[Callable[[], Any]] = None, **__) -> Any:
        if default_factory is not None:
            return default_factory()
        return default

    def _field_validator(*_args: Any, **_kwargs: Any):  # noqa: D401 - passthrough decorator
        def decorator(func: Any) -> Any:
            return func

        return decorator

    pydantic_stub.BaseModel = _BaseModel
    pydantic_stub.Field = _Field
    pydantic_stub.field_validator = _field_validator
    sys.modules["pydantic"] = pydantic_stub

if "aiohttp" not in sys.modules:
    aiohttp_stub = types.ModuleType("aiohttp")

    class _ClientSession:
        async def __aenter__(self) -> "_ClientSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        async def close(self) -> None:  # pragma: no cover - noop
            return None

        async def post(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - placeholder
            raise RuntimeError("aiohttp stub not implemented")

    class _ClientTimeout:
        def __init__(self, total: Optional[float] = None) -> None:
            self.total = total

    class _ClientError(Exception):
        pass

    aiohttp_stub.ClientSession = _ClientSession
    aiohttp_stub.ClientTimeout = _ClientTimeout
    aiohttp_stub.ClientError = _ClientError
    sys.modules["aiohttp"] = aiohttp_stub

if "websockets" not in sys.modules:
    websockets_stub = types.ModuleType("websockets")

    async def _connect(*args: Any, **kwargs: Any) -> Any:  # pragma: no cover - placeholder
        raise RuntimeError("websocket stub not implemented")

    class _WebSocketClientProtocol:
        closed = False

        async def send(self, data: Any) -> None:  # pragma: no cover - noop
            return None

        async def recv(self) -> str:  # pragma: no cover - noop
            return ""

        async def close(self) -> None:  # pragma: no cover - noop
            self.closed = True
            return None

    websockets_stub.connect = _connect
    websockets_stub.WebSocketClientProtocol = _WebSocketClientProtocol
    websockets_stub.exceptions = types.SimpleNamespace(WebSocketException=Exception)
    sys.modules["websockets"] = websockets_stub
    sys.modules["websockets.exceptions"] = websockets_stub.exceptions

if "metrics" not in sys.modules:
    metrics_stub = types.ModuleType("metrics")

    def _noop(*args: Any, **kwargs: Any) -> None:
        return None

    class _TransportMember:
        def __init__(self, value: str) -> None:
            self.value = value

        def __str__(self) -> str:
            return self.value

    class _TransportType:
        UNKNOWN = _TransportMember("unknown")
        INTERNAL = _TransportMember("internal")
        REST = _TransportMember("rest")
        WEBSOCKET = _TransportMember("websocket")
        FIX = _TransportMember("fix")
        BATCH = _TransportMember("batch")

    metrics_stub.increment_oms_child_orders_total = _noop
    metrics_stub.increment_oms_error_count = _noop
    metrics_stub.record_oms_latency = _noop
    metrics_stub.setup_metrics = _noop
    metrics_stub.increment_trade_rejection = _noop
    metrics_stub.record_ws_latency = _noop
    metrics_stub.record_oms_submit_ack = _noop
    metrics_stub.record_drift_score = _noop
    metrics_stub.record_scaling_state = _noop
    metrics_stub.observe_scaling_evaluation = _noop
    metrics_stub.get_request_id = lambda: None
    metrics_stub.TransportType = _TransportType
    metrics_stub.bind_metric_context = lambda *args, **kwargs: contextlib.nullcontext()
    metrics_stub.metric_context = lambda *args, **kwargs: contextlib.nullcontext()
    sys.modules["metrics"] = metrics_stub

from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.oms import oms_service
from services.oms.kraken_ws import OrderAck, OrderState
from services.oms.oms_service import OMSManager, OMSPlaceRequest
from services.oms.warm_start import WarmStartCoordinator
from tests.mocks.mock_kraken import MockKrakenExchange


_ORDER_CLIENT_MAP: Dict[str, str] = {}


class StubCredentialWatcher:
    _instances: Dict[str, "StubCredentialWatcher"] = {}

    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self._metadata = {
            "BTC/USD": {
                "lot_decimals": 4,
                "pair_decimals": 1,
                "ordermin": "0.0001",
                "tick_size": "0.1",
                "step_size": "0.0001",
                "wsname": "BTC/USD",
            }
        }

    @classmethod
    def instance(cls, account_id: str) -> "StubCredentialWatcher":
        watcher = cls._instances.get(account_id)
        if watcher is None:
            watcher = cls(account_id)
            cls._instances[account_id] = watcher
        return watcher

    @classmethod
    def reset(cls) -> None:
        cls._instances.clear()

    async def start(self) -> None:  # pragma: no cover - behaviourless stub
        return None

    async def stop(self) -> None:  # pragma: no cover - behaviourless stub
        return None

    async def get_credentials(self) -> Dict[str, str]:
        return {
            "api_key": "stub-key",
            "api_secret": "stub-secret",
            "account_id": self.account_id,
        }

    async def get_metadata(self) -> Dict[str, Any]:
        return dict(self._metadata)


class StubLatencyRouter:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self._preferred = "websocket"

    async def start(self, ws_client=None, rest_client=None) -> None:  # noqa: ANN001 - interface shim
        return None

    async def stop(self) -> None:  # pragma: no cover - behaviourless stub
        return None

    def record_latency(self, path: str, latency_ms: float) -> None:
        self._preferred = path

    def update_probe_template(self, payload: Dict[str, Any]) -> None:  # noqa: D401 - noop
        return None

    @property
    def preferred_path(self) -> str:
        return self._preferred

    def status(self) -> Dict[str, Optional[float] | str]:
        return {"ws_latency": None, "rest_latency": None, "preferred_path": self._preferred}


class DummyBook:
    async def depth(self, side: str, levels: int = 10) -> None:  # noqa: D401 - deterministic stub
        return None


class DummyOrderBookStore:
    async def ensure_book(self, symbol: str) -> DummyBook:  # noqa: D401 - deterministic stub
        return DummyBook()

    async def stop(self) -> None:  # pragma: no cover - noop
        return None


class DummyImpactStore:
    def __init__(self) -> None:
        self.records: List[Dict[str, Any]] = []

    async def record_fill(self, **kwargs: Any) -> None:
        self.records.append(dict(kwargs))

    async def impact_curve(self, **kwargs: Any) -> List[Dict[str, float]]:  # noqa: D401 - deterministic stub
        return []


class _ExchangeClientBase:
    def __init__(
        self,
        *,
        credential_getter,
        stream_update_cb,
        exchange: MockKrakenExchange,
        transport: str,
    ) -> None:
        self._credential_getter = credential_getter
        self._stream_update_cb = stream_update_cb
        self._exchange = exchange
        self._transport = transport
        self._account_id: Optional[str] = None

    async def _resolve_account(self) -> str:
        if self._account_id is None:
            credentials = await self._credential_getter()
            self._account_id = str(credentials.get("account_id") or "company")
        return self._account_id

    def _lookup_client(self, order_id: str) -> str:
        return _ORDER_CLIENT_MAP.get(order_id, order_id)

    async def _submit_order(self, payload: Dict[str, Any], *, emit_updates: bool, use_rest: bool) -> OrderAck:
        account = await self._resolve_account()
        client_id = str(
            payload.get("clientOrderId")
            or payload.get("userref")
            or payload.get("idempotencyKey")
        )
        request_payload: Dict[str, Any] = {
            "account": account,
            "pair": str(payload.get("pair")),
            "type": str(payload.get("type")),
            "ordertype": str(payload.get("ordertype", "limit")),
            "volume": float(payload.get("volume", 0.0)),
            "userref": client_id,
        }
        if payload.get("price") is not None:
            request_payload["price"] = float(payload["price"])

        if use_rest:
            response = await self._exchange.place_order_rest(request_payload)
        else:
            response = self._exchange.place_order_ws(request_payload)

        order_id = str(response.get("txid"))
        _ORDER_CLIENT_MAP[order_id] = client_id

        ack = self._ack_from_response(response)
        if emit_updates and self._stream_update_cb is not None:
            await self._emit_updates(response, client_id, transport="rest" if use_rest else "websocket")
        return ack

    def _ack_from_response(self, response: Dict[str, Any]) -> OrderAck:
        exchange_order_id = response.get("txid") or response.get("order_id")
        status = response.get("status")
        filled_value = response.get("filled")
        if filled_value is None and {"volume", "remaining"} <= response.keys():
            try:
                filled_value = Decimal(str(response["volume"])) - Decimal(str(response["remaining"]))
            except Exception:  # pragma: no cover - defensive
                filled_value = None
        elif filled_value is not None:
            try:
                filled_value = Decimal(str(filled_value))
            except Exception:  # pragma: no cover - defensive
                filled_value = None

        avg_price = None
        fills = response.get("fills") or []
        if fills:
            total_qty = sum(Decimal(str(fill.get("volume", 0.0))) for fill in fills)
            total_notional = sum(
                Decimal(str(fill.get("price", 0.0))) * Decimal(str(fill.get("volume", 0.0)))
                for fill in fills
            )
            if total_qty:
                avg_price = total_notional / total_qty

        return OrderAck(
            exchange_order_id=str(exchange_order_id) if exchange_order_id is not None else None,
            status=str(status) if status is not None else None,
            filled_qty=filled_value,
            avg_price=avg_price,
            errors=None,
        )

    async def _emit_updates(self, response: Dict[str, Any], client_id: str, *, transport: str) -> None:
        if self._stream_update_cb is None:
            return
        order_id = str(response.get("txid"))
        fills = response.get("fills") or []
        total_filled = Decimal(str(response.get("filled", 0.0)))
        status = str(response.get("status", "open"))
        if not fills:
            state = OrderState(
                client_order_id=client_id,
                exchange_order_id=order_id,
                status=status,
                filled_qty=total_filled if total_filled != Decimal("0") else None,
                avg_price=None,
                errors=None,
                transport=transport,
            )
            await self._stream_update_cb(state)
            return

        cumulative = Decimal("0")
        notional = Decimal("0")
        tolerance = Decimal("1e-9")
        for fill in fills:
            volume = Decimal(str(fill.get("volume", 0.0)))
            price = Decimal(str(fill.get("price", 0.0)))
            cumulative += volume
            notional += price * volume
            avg_price = (notional / cumulative) if cumulative else None
            final_fill = total_filled != Decimal("0") and abs(cumulative - total_filled) <= tolerance
            state = OrderState(
                client_order_id=client_id,
                exchange_order_id=order_id,
                status=status if final_fill else "partially_filled",
                filled_qty=cumulative,
                avg_price=avg_price,
                errors=None,
                transport=transport,
            )
            await self._stream_update_cb(state)

    async def fetch_open_orders_snapshot(self) -> List[Dict[str, Any]]:
        account = await self._resolve_account()
        payload = self._exchange.open_orders(account=account)
        orders: List[Dict[str, Any]] = []
        for raw in payload.get("open", []):
            enriched = dict(raw)
            client_id = self._lookup_client(str(enriched.get("order_id")))
            enriched["userref"] = client_id
            enriched["clientOrderId"] = client_id
            enriched["txid"] = enriched.get("order_id")
            orders.append(enriched)
        return orders

    async def fetch_own_trades_snapshot(self) -> List[Dict[str, Any]]:
        account = await self._resolve_account()
        trades = await self._exchange.get_trades(account=account)
        aggregates: Dict[str, Dict[str, Any]] = {}
        for trade in trades:
            order_id = str(trade.get("order_id"))
            entry = aggregates.setdefault(
                order_id,
                {
                    "ordertxid": order_id,
                    "order_id": order_id,
                    "userref": self._lookup_client(order_id),
                    "pair": trade.get("pair"),
                    "status": "filled",
                    "filled": Decimal("0"),
                    "notional": Decimal("0"),
                },
            )
            volume = Decimal(str(trade.get("volume", 0.0)))
            price = Decimal(str(trade.get("price", 0.0)))
            entry["filled"] += volume
            entry["notional"] += price * volume

        results: List[Dict[str, Any]] = []
        for order_id, data in aggregates.items():
            filled = data.pop("filled")
            notional = data.pop("notional")
            avg_price = (notional / filled) if filled else None
            order = self._exchange._orders.get(order_id)  # noqa: SLF001 - test helper access
            if order is not None and getattr(order, "remaining", 0.0) > 0:
                data["status"] = "partially_filled"
            data["filled"] = filled
            if avg_price is not None:
                data["avg_price"] = avg_price
            if not data.get("userref"):
                data["userref"] = self._lookup_client(order_id)
            results.append(data)
        return results


class StubWSClient(_ExchangeClientBase):
    def __init__(self, *, credential_getter, stream_update_cb, exchange: MockKrakenExchange, **_: Any) -> None:
        super().__init__(
            credential_getter=credential_getter,
            stream_update_cb=stream_update_cb,
            exchange=exchange,
            transport="websocket",
        )
        self.emit_stream_updates = True

    async def ensure_connected(self) -> None:  # pragma: no cover - noop
        return None

    async def subscribe_private(self, channels: Iterable[str]) -> None:  # noqa: D401 - noop
        return None

    async def stream_handler(self) -> None:  # pragma: no cover - cancelled during shutdown
        while True:
            await asyncio.sleep(3600)

    async def close(self) -> None:  # pragma: no cover - noop
        return None

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        return await self._submit_order(payload, emit_updates=self.emit_stream_updates, use_rest=False)

    async def cancel_order(self, payload: Dict[str, Any]) -> OrderAck:
        account = await self._resolve_account()
        order_id = str(payload.get("txid") or payload.get("order_id"))
        response = self._exchange.cancel_order_ws({"txid": order_id, "account": account})
        ack = self._ack_from_response(response)
        if self.emit_stream_updates and self._stream_update_cb is not None:
            client_id = self._lookup_client(order_id)
            state = OrderState(
                client_order_id=client_id,
                exchange_order_id=str(response.get("txid")),
                status=str(response.get("status", "cancelled")),
                filled_qty=None,
                avg_price=None,
                errors=None,
                transport="websocket",
            )
            await self._stream_update_cb(state)
        return ack

    def heartbeat_age(self) -> float:
        return 0.0


class StubRESTClient(_ExchangeClientBase):
    def __init__(
        self,
        *,
        credential_getter,
        stream_update_cb: Optional[Any] = None,
        exchange: MockKrakenExchange,
        **_: Any,
    ) -> None:
        super().__init__(
            credential_getter=credential_getter,
            stream_update_cb=stream_update_cb,
            exchange=exchange,
            transport="rest",
        )
        self.submitted_payloads: List[Dict[str, Any]] = []
        self.cancel_payloads: List[Dict[str, Any]] = []

    async def close(self) -> None:  # pragma: no cover - noop
        return None

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        self.submitted_payloads.append(dict(payload))
        return await self._submit_order(payload, emit_updates=False, use_rest=True)

    async def cancel_order(self, payload: Dict[str, Any]) -> OrderAck:
        self.cancel_payloads.append(dict(payload))
        account = await self._resolve_account()
        order_id = str(payload.get("txid") or payload.get("order_id"))
        response = await self._exchange.cancel_order_rest({"txid": order_id, "account": account})
        return self._ack_from_response(response)

    async def open_orders(self) -> Dict[str, Any]:
        snapshot = await self.fetch_open_orders_snapshot()
        return {"result": {"open": {entry["order_id"]: entry for entry in snapshot}}}

    async def own_trades(self, **_: Any) -> Dict[str, Any]:
        snapshot = await self.fetch_own_trades_snapshot()
        return {"result": {"trades": {idx: entry for idx, entry in enumerate(snapshot)}}}


async def _execute_warm_start_scenario(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _ORDER_CLIENT_MAP.clear()
    StubCredentialWatcher.reset()
    KafkaNATSAdapter.reset()
    TimescaleAdapter.reset()

    exchange = MockKrakenExchange()
    exchange.schedule_fill_sequence([0.5])

    dummy_impact = DummyImpactStore()

    monkeypatch.setattr(oms_service, "CredentialWatcher", StubCredentialWatcher)
    monkeypatch.setattr(oms_service, "LatencyRouter", StubLatencyRouter)
    monkeypatch.setattr(oms_service, "order_book_store", DummyOrderBookStore())
    monkeypatch.setattr(oms_service, "impact_store", dummy_impact)
    monkeypatch.setattr(oms_service, "KrakenWSClient", lambda **kwargs: StubWSClient(exchange=exchange, **kwargs))
    monkeypatch.setattr(oms_service, "KrakenRESTClient", lambda **kwargs: StubRESTClient(exchange=exchange, **kwargs))
    monkeypatch.setattr(oms_service, "increment_oms_child_orders_total", lambda *_, **__: None)
    monkeypatch.setattr(oms_service, "increment_oms_error_count", lambda *_, **__: None)
    monkeypatch.setattr(oms_service, "record_oms_latency", lambda *_, **__: None)

    manager = OMSManager()
    warm_start = WarmStartCoordinator(lambda: manager)
    monkeypatch.setattr(oms_service, "manager", manager)
    monkeypatch.setattr(oms_service, "warm_start", warm_start)

    account_id = "company"
    client_id = "ORD-WARM-1"
    request = OMSPlaceRequest(
        account_id=account_id,
        client_id=client_id,
        symbol="BTC/USD",
        side="buy",
        order_type="limit",
        qty=Decimal("1"),
        limit_px=Decimal("30000"),
        flags=[],
        post_only=False,
        reduce_only=False,
        shadow=False,
    )

    account = await manager.get_account(account_id)
    response = await account.place_order(request)
    assert response.status == "partially_filled"

    record = await account.lookup(client_id)
    assert record is not None
    assert float(record.result.filled_qty) == pytest.approx(0.5)

    exchange_state = exchange.open_orders(account=account_id)["open"]
    assert exchange_state, "exchange should report an open order"
    exchange_order = exchange_state[0]
    assert exchange_order["order_id"] == record.result.exchange_order_id

    await manager.shutdown()
    StubCredentialWatcher.reset()

    new_manager = OMSManager()
    new_warm_start = WarmStartCoordinator(lambda: new_manager)
    monkeypatch.setattr(oms_service, "manager", new_manager)
    monkeypatch.setattr(oms_service, "warm_start", new_warm_start)

    with caplog.at_level(logging.WARNING, logger="services.oms.warm_start"):
        await new_warm_start.run(accounts=[account_id])

    status = await new_warm_start.status()
    assert status["orders_resynced"] >= 1

    recovered = await new_manager.get_account(account_id)
    recovered_record = await recovered.lookup(client_id)
    assert recovered_record is not None
    assert recovered_record.result.status == "partially_filled"

    expected_filled = Decimal(str(exchange_order["volume"])) - Decimal(str(exchange_order["remaining"]))
    assert recovered_record.result.filled_qty == expected_filled
    assert recovered_record.result.exchange_order_id == exchange_order["order_id"]
    assert float(recovered_record.result.avg_price) == pytest.approx(exchange_order["price"])

    trades = await exchange.get_trades(account=account_id)
    assert trades, "exchange should have at least one trade recorded"
    assert any(trade["order_id"] == exchange_order["order_id"] for trade in trades)

    warm_start_messages = [message for message in caplog.messages if "Warm start" in message]
    assert warm_start_messages, "expected warm start log entries to be written"

    await new_manager.shutdown()
    StubCredentialWatcher.reset()
    _ORDER_CLIENT_MAP.clear()


@pytest.mark.integration
def test_warm_start_recovers_orders_and_fills(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    asyncio.run(_execute_warm_start_scenario(monkeypatch, caplog))
