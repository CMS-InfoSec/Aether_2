import asyncio
import base64
import json
import sys
import types
from decimal import Decimal
from urllib.parse import parse_qs

class _DummyAiohttp(types.ModuleType):
    class ClientSession:  # pragma: no cover - test stub
        async def close(self) -> None:  # pragma: no cover - compatibility
            return None

    class ClientTimeout:  # pragma: no cover - test stub
        def __init__(self, total: float) -> None:
            self.total = total

    class ClientError(Exception):
        pass


sys.modules.setdefault("aiohttp", _DummyAiohttp("aiohttp"))


class _DummyWebsockets(types.ModuleType):
    class WebSocketClientProtocol:  # pragma: no cover - test stub
        closed = False

        async def send(self, data: str) -> None:  # pragma: no cover - compatibility
            return None

        async def recv(self) -> str:  # pragma: no cover - compatibility
            return ""

        async def close(self) -> None:  # pragma: no cover - compatibility
            return None


async def _dummy_connect(url: str, ping_interval=None):  # pragma: no cover - compatibility
    raise NotImplementedError


_websockets_module = _DummyWebsockets("websockets")
_websockets_module.connect = _dummy_connect
_websockets_module.WebSocketClientProtocol = _DummyWebsockets.WebSocketClientProtocol


class _DummyWebsocketsExceptions(types.ModuleType):
    class WebSocketException(Exception):
        pass


sys.modules.setdefault("websockets", _websockets_module)
sys.modules.setdefault("websockets.exceptions", _DummyWebsocketsExceptions("websockets.exceptions"))

from services.oms.kraken_rest import KrakenRESTClient


class DummyResponse:
    def __init__(self) -> None:
        self.status = 200

    async def __aenter__(self) -> "DummyResponse":
        await asyncio.sleep(0)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def text(self) -> str:
        await asyncio.sleep(0)
        return json.dumps({"error": [], "result": {"status": "ok"}})


class RecordingSession:
    def __init__(self) -> None:
        self.nonces: list[int] = []

    def post(self, url, data, headers, timeout):  # pragma: no cover - signature compatibility
        parsed = parse_qs(data)
        nonce = int(parsed["nonce"][0])
        self.nonces.append(nonce)
        return DummyResponse()


def test_request_nonce_is_strictly_increasing_concurrently():
    asyncio.run(_run_nonce_test())


async def _run_nonce_test() -> None:
    session = RecordingSession()
    credentials = {
        "api_key": "key",
        "api_secret": base64.b64encode(b"secret").decode(),
    }

    async def credential_getter():
        await asyncio.sleep(0)
        return credentials

    client = KrakenRESTClient(
        credential_getter=credential_getter,
        session=session,
        base_url="http://test",
    )

    async def issue_request() -> None:
        payload = {"foo": "bar"}
        response = await client._request("/private/Test", payload)
        assert response["result"]["status"] == "ok"

    tasks = [asyncio.create_task(issue_request()) for _ in range(20)]
    await asyncio.gather(*tasks)

    nonces = session.nonces
    assert len(nonces) == 20
    assert nonces == sorted(nonces)
    assert len(set(nonces)) == len(nonces)


def test_parse_response_preserves_decimal_precision() -> None:
    async def credential_getter() -> dict[str, str]:
        return {}

    client = KrakenRESTClient(credential_getter=credential_getter)
    payload = {
        "result": {
            "status": "ok",
            "txid": ["ABC123"],
            "filled": "0.987654321098",
            "avg_price": "123.4500006789",
        }
    }

    ack = client._parse_response(payload)

    assert ack.filled_qty == Decimal("0.987654321098")
    assert ack.avg_price == Decimal("123.4500006789")
    assert str(ack.filled_qty) == "0.987654321098"
    assert str(ack.avg_price) == "123.4500006789"
