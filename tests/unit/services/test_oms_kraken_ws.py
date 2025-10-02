from __future__ import annotations

import asyncio
import sys
import types

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
