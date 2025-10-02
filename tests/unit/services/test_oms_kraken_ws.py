from __future__ import annotations

import asyncio
import importlib.util
import sys
import time
import types

import pytest

_websockets_stub = types.ModuleType("websockets")

_fastapi_stub = types.ModuleType("fastapi")


class _DummyFastAPI:
    def __init__(self, *args: object, **kwargs: object) -> None:
        pass


class _DummyRequest:
    pass


class _DummyResponse:
    pass


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

def _module_available(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except ModuleNotFoundError:
        return False


if not _module_available("fastapi"):
    _fastapi_stub.FastAPI = _DummyFastAPI  # type: ignore[attr-defined]
    _fastapi_stub.Request = _DummyRequest  # type: ignore[attr-defined]
    _fastapi_stub.Response = _DummyResponse  # type: ignore[attr-defined]
    sys.modules.setdefault("fastapi", _fastapi_stub)

_starlette_stub = types.ModuleType("starlette")
_starlette_middleware_stub = types.ModuleType("starlette.middleware")
_starlette_middleware_base_stub = types.ModuleType("starlette.middleware.base")


class _DummyBaseHTTPMiddleware:
    def __init__(self, *args: object, **kwargs: object) -> None:
        pass


if not _module_available("starlette.middleware.base"):
    _starlette_middleware_base_stub.BaseHTTPMiddleware = _DummyBaseHTTPMiddleware  # type: ignore[attr-defined]
    sys.modules.setdefault("starlette", _starlette_stub)
    sys.modules.setdefault("starlette.middleware", _starlette_middleware_stub)
    sys.modules.setdefault("starlette.middleware.base", _starlette_middleware_base_stub)

from services.oms.kraken_ws import KrakenWSClient, KrakenWSError


class _StubRestClient:
    def __init__(self, token: str = "rest-token", expires: float = 900.0) -> None:
        self.token = token
        self.expires = expires
        self.calls = 0

    async def websocket_token(self) -> tuple[str, float]:
        self.calls += 1
        return self.token, self.expires


class _FailingRestClient:
    async def websocket_token(self) -> tuple[str, float]:
        raise RuntimeError("boom")


class _StubGuard:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    async def acquire(
        self,
        account_id: str,
        endpoint: str,
        *,
        transport: str = "rest",
        urgent: bool = False,
    ) -> None:
        self.calls.append((account_id, endpoint, transport))


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


def test_sign_auth_caches_rest_token_until_expiry() -> None:
    async def _creds() -> dict[str, str]:
        return {"api_key": "api", "api_secret": "secret"}

    rest_client = _StubRestClient(token="rest-generated-token", expires=120.0)
    guard = _StubGuard()
    client = KrakenWSClient(
        credential_getter=_creds,
        rest_client=rest_client,
        rate_limit_guard=guard,
        account_id="acct-1",
    )

    async def _run() -> None:
        first = await client._sign_auth()
        assert first == "rest-generated-token"
        assert rest_client.calls == 1
        assert guard.calls == [("acct-1", "websocket_token", "rest")]

        second = await client._sign_auth()
        assert second == first
        assert rest_client.calls == 1

        client._ws_token_expiry = time.monotonic() - 1

        third = await client._sign_auth()
        assert third == first
        assert rest_client.calls == 2
        assert guard.calls[-1] == ("acct-1", "websocket_token", "rest")

    asyncio.run(_run())
