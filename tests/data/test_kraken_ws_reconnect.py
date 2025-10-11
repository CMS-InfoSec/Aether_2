from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Iterable, List

import pytest

from data.ingest import kraken_ws


class _FakeMessage:
    def __init__(self, message_type: object, data: str) -> None:
        self.type = message_type
        self.data = data


class _FakeWebSocket:
    def __init__(self, messages: Iterable[_FakeMessage], *, error: Exception | None = None) -> None:
        self._messages = iter(messages)
        self._error = error
        self.sent_messages: List[str] = []

    async def send_str(self, payload: str) -> None:
        self.sent_messages.append(payload)

    def __aiter__(self) -> "_FakeWebSocket":
        return self

    async def __anext__(self) -> _FakeMessage:
        try:
            return next(self._messages)
        except StopIteration:
            if self._error is not None:
                raise self._error
            raise StopAsyncIteration

    async def close(self) -> None:  # pragma: no cover - API parity
        return None


@pytest.mark.asyncio
async def test_consume_reconnects_after_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    payload_one = {
        "as": [["50000.0", "0.5", "1620000000.0"]],
        "sequence": 1,
    }
    payload_two = {
        "a": [["50001.0", "0.4", "1620000001.0"]],
        "sequence": 2,
    }

    message_one = _FakeMessage(
        message_type="TEXT",
        data=json.dumps([0, payload_one, {"channel": "book"}, "BTC/USD"]),
    )
    message_two = _FakeMessage(
        message_type="TEXT",
        data=json.dumps([0, payload_two, {"channel": "book"}, "BTC/USD"]),
    )

    disconnect_error = Exception("connection dropped")
    sockets = iter(
        [
            _FakeWebSocket([message_one], error=disconnect_error),
            _FakeWebSocket([message_two]),
        ]
    )

    class _FakeClientSession:
        def __init__(self) -> None:
            self.ws_connect_calls = 0

        async def __aenter__(self) -> "_FakeClientSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - API parity
            return None

        async def ws_connect(self, url: str):  # pragma: no cover - unused url
            self.ws_connect_calls += 1
            return next(sockets)

    fake_ws_types = SimpleNamespace(
        TEXT="TEXT",
        ERROR="ERROR",
        CLOSE="CLOSE",
        CLOSED="CLOSED",
        CLOSING="CLOSING",
    )

    fake_aiohttp = SimpleNamespace(
        ClientSession=_FakeClientSession,
        ClientError=Exception,
        ClientConnectionError=Exception,
        WSMsgType=fake_ws_types,
    )

    monkeypatch.setattr(kraken_ws, "aiohttp", fake_aiohttp)
    monkeypatch.setattr(kraken_ws, "DATABASE_URL", None)
    monkeypatch.setenv("KRAKEN_WS_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setattr(kraken_ws, "_require_sqlalchemy", lambda: None)

    persisted = []

    def _capture_persist(engine, updates):
        persisted.append([update.sequence for update in updates])

    async def _capture_publish_to_nats(updates):
        return None

    class _DummyProducer:
        def produce(self, topic, payload):  # pragma: no cover - API parity
            return None

        def flush(self):  # pragma: no cover - API parity
            return None

    monkeypatch.setattr(kraken_ws, "persist_updates", _capture_persist)
    monkeypatch.setattr(kraken_ws, "publish_updates", lambda producer, updates: None)
    monkeypatch.setattr(kraken_ws, "publish_to_nats", _capture_publish_to_nats)
    monkeypatch.setattr(kraken_ws, "kafka_producer", lambda: _DummyProducer())

    sleep_calls: List[float] = []

    async def _fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(kraken_ws.asyncio, "sleep", _fake_sleep)

    await kraken_ws.consume(["BTC/USD"], max_cycles=2)

    assert persisted == [[1], [2]], "expected updates from both websocket cycles"
    assert sleep_calls, "expected exponential backoff to trigger after disconnect"
