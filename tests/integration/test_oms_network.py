import asyncio
import importlib
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List
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

        async def post(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("aiohttp stub invoked")

        async def get(self, *args: Any, **kwargs: Any) -> Any:
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

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.oms.kraken_rest import KrakenRESTError
from services.oms.kraken_ws import OrderAck


class RecordingTransport:
    def __init__(self, responder: Callable[[Dict[str, Any]], Dict[str, Any] | None]) -> None:
        self._responder = responder
        self.sent: List[Dict[str, Any]] = []
        self._queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def send_json(self, payload: Dict[str, Any]) -> None:
        self.sent.append(payload)
        response = self._responder(payload)
        if response is not None:
            await self._queue.put(response)

    async def recv_json(self) -> Dict[str, Any]:
        return await self._queue.get()

    async def close(self) -> None:
        self._closed = True


def _credential_payload(account_id: str) -> Dict[str, Any]:
    return {
        "api_key": f"net-{account_id}",
        "api_secret": "secret",
        "metadata": {"rotated_at": datetime.now(timezone.utc).isoformat()},
    }


def _rest_stub(*, raise_error: bool = False):
    class _StubRest:
        async def websocket_token(self) -> tuple[str, float]:
            return "TOKEN", 60.0

        async def add_order(self, payload: Dict[str, Any]):
            if raise_error:
                raise KrakenRESTError("rest add order failed")
            return OrderAck(
                exchange_order_id="REST-ACK",
                status="ok",
                filled_qty=None,
                avg_price=None,
                errors=None,
            )

        async def open_orders(self) -> Dict[str, Any]:
            return {"result": {"open": []}}

        async def own_trades(self) -> Dict[str, Any]:
            return {"result": {"trades": {}}}

        async def close(self) -> None:
            return None

    return _StubRest()


def _make_factory(
    oms_main,
    transport: RecordingTransport,
    *,
    request_timeout: float = 0.2,
    rest_raises: bool = False,
):
    @asynccontextmanager
    async def _factory(account_id: str):
        async def _credentials() -> Dict[str, Any]:
            return _credential_payload(account_id)

        async def _transport_factory(url: str, *, headers: Dict[str, str] | None = None):
            del url, headers
            return transport

        rest_client = _rest_stub(raise_error=rest_raises)
        ws_client = oms_main.KrakenWSClient(
            credential_getter=_credentials,
            rest_client=rest_client,
            transport_factory=_transport_factory,
            account_id=account_id,
            request_timeout=request_timeout,
        )
        try:
            yield oms_main.KrakenClientBundle(
                credential_getter=_credentials,
                ws_client=ws_client,
                rest_client=rest_client,
            )
        finally:
            await ws_client.close()
            await rest_client.close()

    return _factory


@pytest.mark.integration
def test_place_order_uses_websocket_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    oms_main = importlib.reload(importlib.import_module("services.oms.main"))

    def _responder(payload: Dict[str, Any]) -> Dict[str, Any] | None:
        method = payload.get("method")
        req_id = payload.get("req_id")
        if method == "add_order":
            return {"req_id": req_id, "status": "ok", "result": {"txid": "WS-123"}}
        if method in {"open_orders", "openOrders", "openOrdersStatus", "open_orders_status"}:
            return {"req_id": req_id, "result": {"open": []}}
        if method in {"own_trades", "ownTrades", "ownTradesStatus", "own_trades_status"}:
            return {
                "req_id": req_id,
                "result": {
                    "trades": [
                        {
                            "ordertxid": "WS-123",
                            "price": "30000",
                            "quantity": "1",
                            "time": time.time(),
                            "side": "buy",
                        }
                    ]
                },
            }
        return {"req_id": req_id or 1}

    import shared.graceful_shutdown as graceful_shutdown

    transport = RecordingTransport(_responder)
    factory = _make_factory(oms_main, transport)
    monkeypatch.setattr(oms_main.app.state, "kraken_client_factory", factory)
    monkeypatch.setattr(graceful_shutdown, "install_sigterm_handler", lambda manager: None)

    KafkaNATSAdapter.reset()
    asyncio.run(TimescaleAdapter.flush_event_buffers())

    with TestClient(oms_main.app) as client:
        payload = {
            "order_id": "OID-1",
            "account_id": "company",
            "instrument": "BTC-USD",
            "side": "BUY",
            "quantity": 1.0,
            "price": 30000.0,
            "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        }
        response = client.post(
            "/oms/place",
            json=payload,
            headers={"X-Account-ID": "company"},
        )

    assert response.status_code == 200
    assert any(msg.get("method") == "add_order" for msg in transport.sent)


@pytest.mark.integration
def test_place_order_rejects_on_kraken_error(monkeypatch: pytest.MonkeyPatch) -> None:
    oms_main = importlib.reload(importlib.import_module("services.oms.main"))

    def _responder(payload: Dict[str, Any]) -> Dict[str, Any] | None:
        req_id = payload.get("req_id")
        if payload.get("method") == "add_order":
            return {"req_id": req_id, "status": "error", "error": ["EOrder:Rejected"]}
        return {"req_id": req_id or 1}

    import shared.graceful_shutdown as graceful_shutdown

    transport = RecordingTransport(_responder)
    factory = _make_factory(oms_main, transport)
    monkeypatch.setattr(oms_main.app.state, "kraken_client_factory", factory)
    monkeypatch.setattr(graceful_shutdown, "install_sigterm_handler", lambda manager: None)

    KafkaNATSAdapter.reset()
    asyncio.run(TimescaleAdapter.flush_event_buffers())

    with TestClient(oms_main.app) as client:
        payload = {
            "order_id": "OID-ERR",
            "account_id": "company",
            "instrument": "BTC-USD",
            "side": "BUY",
            "quantity": 1.0,
            "price": 30000.0,
            "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        }
        response = client.post(
            "/oms/place",
            json=payload,
            headers={"X-Account-ID": "company"},
        )

    assert response.status_code == 502
    assert any(msg.get("method") == "add_order" for msg in transport.sent)


@pytest.mark.integration
def test_place_order_times_out_when_kraken_stalls(monkeypatch: pytest.MonkeyPatch) -> None:
    oms_main = importlib.reload(importlib.import_module("services.oms.main"))

    def _responder(payload: Dict[str, Any]) -> Dict[str, Any] | None:
        if payload.get("method") == "add_order":
            return None
        return {"req_id": payload.get("req_id") or 1}

    import shared.graceful_shutdown as graceful_shutdown

    transport = RecordingTransport(_responder)
    factory = _make_factory(oms_main, transport, request_timeout=0.05, rest_raises=True)
    monkeypatch.setattr(oms_main.app.state, "kraken_client_factory", factory)
    monkeypatch.setattr(graceful_shutdown, "install_sigterm_handler", lambda manager: None)

    KafkaNATSAdapter.reset()
    asyncio.run(TimescaleAdapter.flush_event_buffers())

    with TestClient(oms_main.app) as client:
        payload = {
            "order_id": "OID-TIMEOUT",
            "account_id": "company",
            "instrument": "BTC-USD",
            "side": "BUY",
            "quantity": 1.0,
            "price": 30000.0,
            "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        }
        response = client.post(
            "/oms/place",
            json=payload,
            headers={"X-Account-ID": "company"},
        )

    assert response.status_code == 504
    assert any(msg.get("method") == "add_order" for msg in transport.sent)
