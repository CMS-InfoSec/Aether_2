from __future__ import annotations

import asyncio
import sys
import types
from decimal import Decimal

import pytest

_websockets_stub = types.ModuleType("websockets")


class _DummyWebSocketProtocol:
    pass


class _DummyWebSocketException(Exception):
    pass


async def _connect_stub(*args: object, **kwargs: object) -> _DummyWebSocketProtocol:
    return _DummyWebSocketProtocol()


_websockets_stub.connect = _connect_stub  # type: ignore[attr-defined]
_websockets_stub.WebSocketClientProtocol = _DummyWebSocketProtocol  # type: ignore[attr-defined]

_websocket_exceptions = types.ModuleType("websockets.exceptions")
_websocket_exceptions.WebSocketException = _DummyWebSocketException

sys.modules.setdefault("websockets", _websockets_stub)
sys.modules.setdefault("websockets.exceptions", _websocket_exceptions)

_aiohttp_stub = types.ModuleType("aiohttp")


class _DummyClientSession:
    def __init__(self, *args: object, **kwargs: object) -> None:
        return None

    async def close(self) -> None:  # pragma: no cover - not used in tests
        return None


class _DummyClientTimeout:
    def __init__(self, *args: object, **kwargs: object) -> None:
        return None


class _DummyClientError(Exception):
    pass


_aiohttp_stub.ClientSession = _DummyClientSession  # type: ignore[attr-defined]
_aiohttp_stub.ClientTimeout = _DummyClientTimeout  # type: ignore[attr-defined]
_aiohttp_stub.ClientError = _DummyClientError  # type: ignore[attr-defined]

sys.modules.setdefault("aiohttp", _aiohttp_stub)

from services.oms.kraken_rest import KrakenRESTClient
from services.oms.kraken_ws import KrakenWSClient, KrakenWSError


class _StubRestClient:
    def __init__(self, token: str = "rest-token") -> None:
        self.token = token
        self.calls = 0

    async def websocket_token(self) -> str:
        self.calls += 1
        return self.token


class _FailingRestClient:
    async def websocket_token(self) -> str:
        raise RuntimeError("boom")


def test_sign_auth_prefers_existing_token() -> None:
    async def _creds() -> dict[str, str]:
        return {"ws_token": "from-credentials"}

    rest_client = _StubRestClient()
    client = KrakenWSClient(credential_getter=_creds, rest_client=rest_client)

    async def _run() -> None:
        token = await client._sign_auth()
        assert token == "from-credentials"
        assert rest_client.calls == 0

    asyncio.run(_run())


def test_sign_auth_fetches_rest_token_without_leaking_credentials() -> None:
    api_key = "APIKEY123"
    api_secret = "SECRET456"

    async def _creds() -> dict[str, str]:
        return {"api_key": api_key, "api_secret": api_secret}

    rest_client = _StubRestClient(token="rest-generated-token")
    client = KrakenWSClient(credential_getter=_creds, rest_client=rest_client)

    async def _run() -> None:
        token = await client._sign_auth()
        assert token == "rest-generated-token"
        assert api_key not in token
        assert api_secret not in token
        assert rest_client.calls == 1

    asyncio.run(_run())


def test_sign_auth_rest_failure_raises() -> None:
    async def _creds() -> dict[str, str]:
        return {}

    client = KrakenWSClient(credential_getter=_creds, rest_client=_FailingRestClient())

    async def _run() -> None:
        with pytest.raises(KrakenWSError):
            await client._sign_auth()

    asyncio.run(_run())


def test_ack_from_payload_preserves_decimal_precision() -> None:
    async def _creds() -> dict[str, str]:
        return {}

    client = KrakenWSClient(credential_getter=_creds)

    payload = {
        "result": {
            "txid": "ABC123",
            "status": "filled",
            "filled": "0.123456789",  # 9 decimal places
            "avg_price": "12345.67890123",  # high precision price
        }
    }

    ack = client._ack_from_payload(payload)

    assert isinstance(ack.filled_qty, Decimal)
    assert isinstance(ack.avg_price, Decimal)
    assert ack.filled_qty == Decimal("0.123456789")
    assert ack.avg_price == Decimal("12345.67890123")


def test_rest_parse_response_preserves_decimal_precision() -> None:
    async def _creds() -> dict[str, str]:
        return {}

    client = KrakenRESTClient(credential_getter=_creds)

    payload = {
        "result": {
            "txid": ["XYZ987"],
            "status": "ok",
            "filled": "0.000000123456789",  # 15 decimal places
            "avg_price": "98765.432109876",  # high precision price
        }
    }

    ack = client._parse_response(payload)

    assert isinstance(ack.filled_qty, Decimal)
    assert isinstance(ack.avg_price, Decimal)
    assert ack.filled_qty == Decimal("0.000000123456789")
    assert ack.avg_price == Decimal("98765.432109876")
