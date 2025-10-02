import asyncio
import base64
import json
from decimal import Decimal
from typing import Any, Dict, List

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from services.oms import oms_service
from services.oms.kraken_rest import KrakenRESTClient
from services.oms.kraken_ws import KrakenWSClient
import services.oms.kraken_rest as kraken_rest_module
import services.oms.kraken_ws as kraken_ws_module


@pytest.mark.integration
def test_rest_request_id_propagates_to_exchange(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_headers: List[Dict[str, str]] = []

    class _FakeResponse:
        status = 200

        async def text(self) -> str:
            return json.dumps({"result": {}})

    class _FakeRequestContext:
        def __init__(self, headers: Dict[str, str] | None) -> None:
            captured_headers.append(dict(headers or {}))

        async def __aenter__(self) -> _FakeResponse:
            return _FakeResponse()

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class _FakeSession:
        def post(
            self,
            url: str,
            *,
            data: str | None = None,
            headers: Dict[str, str] | None = None,
            timeout: Any = None,
        ) -> _FakeRequestContext:
            del url, data, timeout
            return _FakeRequestContext(headers)

        async def close(self) -> None:
            return None

    monkeypatch.setattr(
        kraken_rest_module.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: _FakeSession(),
    )

    account_id = "acc-rest"
    request_id = "rest-correlation-id"

    class _RestAccount:
        def __init__(self) -> None:
            self.rest_client = KrakenRESTClient(credential_getter=self._credentials)

        async def _credentials(self) -> Dict[str, str]:
            return {
                "api_key": "dummy",
                "api_secret": base64.b64encode(b"secret").decode(),
            }

        async def place_order(self, request: oms_service.OMSPlaceRequest) -> oms_service.OMSPlaceResponse:
            await self.rest_client.balance()
            await self.rest_client.close()
            return oms_service.OMSPlaceResponse(
                exchange_order_id="REST-1",
                status="ok",
                filled_qty=Decimal("0"),
                avg_price=Decimal("0"),
                errors=None,
                transport="rest",
                reused=False,
                shadow=request.shadow,
            )

    class _StubManager:
        def __init__(self, account: _RestAccount) -> None:
            self._account = account

        async def get_account(self, requested_account: str) -> _RestAccount:
            assert requested_account == account_id
            return self._account

        async def shutdown(self) -> None:
            return None

    account = _RestAccount()
    monkeypatch.setattr(oms_service, "manager", _StubManager(account))

    async def _require_authorized_account(request: oms_service.Request) -> str:
        return request.headers.get("X-Account-ID", account_id)

    oms_service.app.dependency_overrides[oms_service.require_authorized_account] = (
        _require_authorized_account
    )
    try:
        with TestClient(oms_service.app) as client:
            payload = {
                "account_id": account_id,
                "client_id": "CID-REST-001",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "qty": "1",
                "limit_px": "30000",
            }
            response = client.post(
                "/oms/place",
                json=payload,
                headers={"X-Account-ID": account_id, "X-Request-ID": request_id},
            )
        assert response.status_code == 200
    finally:
        oms_service.app.dependency_overrides.pop(oms_service.require_authorized_account, None)

    assert captured_headers, "Expected the REST client to issue an HTTP request"
    assert captured_headers[0].get("X-Request-ID") == request_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_websocket_request_id_propagates_to_exchange(monkeypatch: pytest.MonkeyPatch) -> None:
    handshake_headers: List[Dict[str, str]] = []

    class _FakeProtocol:
        def __init__(self) -> None:
            self.closed = False

        async def send(self, message: str) -> None:
            del message
            return None

        async def recv(self) -> str:
            await asyncio.sleep(0)
            return json.dumps({"event": "heartbeat"})

        async def close(self) -> None:
            self.closed = True

    async def _fake_connect(
        url: str,
        *,
        ping_interval: float | None = None,
        extra_headers: Dict[str, str] | None = None,
    ) -> _FakeProtocol:
        del url, ping_interval
        handshake_headers.append(dict(extra_headers or {}))
        return _FakeProtocol()

    monkeypatch.setattr(kraken_ws_module.websockets, "connect", _fake_connect)

    async def _receiver_loop_stub(self: KrakenWSClient) -> None:
        await asyncio.sleep(0)

    monkeypatch.setattr(
        KrakenWSClient,
        "_receiver_loop",
        _receiver_loop_stub,
    )

    account_id = "acc-ws"
    request_id = "ws-correlation-id"

    class _WSAccount:
        def __init__(self) -> None:
            self.ws_client = KrakenWSClient(credential_getter=self._credentials)

        async def _credentials(self) -> Dict[str, str]:
            return {"api_key": "dummy", "api_secret": "secret"}

        async def place_order(self, request: oms_service.OMSPlaceRequest) -> oms_service.OMSPlaceResponse:
            await self.ws_client.ensure_connected()
            await self.ws_client.close()
            return oms_service.OMSPlaceResponse(
                exchange_order_id="WS-1",
                status="ok",
                filled_qty=Decimal("0"),
                avg_price=Decimal("0"),
                errors=None,
                transport="websocket",
                reused=False,
                shadow=request.shadow,
            )

    class _StubManager:
        def __init__(self, account: _WSAccount) -> None:
            self._account = account

        async def get_account(self, requested_account: str) -> _WSAccount:
            assert requested_account == account_id
            return self._account

        async def shutdown(self) -> None:
            return None

    account = _WSAccount()
    monkeypatch.setattr(oms_service, "manager", _StubManager(account))

    async def _require_authorized_account(request: oms_service.Request) -> str:
        return request.headers.get("X-Account-ID", account_id)

    oms_service.app.dependency_overrides[oms_service.require_authorized_account] = (
        _require_authorized_account
    )
    try:
        with TestClient(oms_service.app) as client:
            payload = {
                "account_id": account_id,
                "client_id": "CID-WS-001",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "qty": "1",
                "limit_px": "30000",
            }
            response = client.post(
                "/oms/place",
                json=payload,
                headers={"X-Account-ID": account_id, "X-Request-ID": request_id},
            )
        assert response.status_code == 200
    finally:
        oms_service.app.dependency_overrides.pop(oms_service.require_authorized_account, None)

    assert handshake_headers, "Expected the WebSocket client to initiate a connection"
    assert handshake_headers[0].get("X-Request-ID") == request_id
