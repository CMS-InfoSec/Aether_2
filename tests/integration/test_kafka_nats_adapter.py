from __future__ import annotations

import asyncio
import json
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from typing import Dict, List, Tuple

import pytest

from services.common.adapters import KafkaNATSAdapter, PublishError


class _BrokerState:
    def __init__(self) -> None:
        self.topics: List[Tuple[str, Dict[str, object]]] = []
        self.subjects: List[Tuple[str, Dict[str, object]]] = []
        self.topic_failures = 0
        self.subject_failures = 0
        self._lock = threading.Lock()

    def record_topic(self, topic: str, payload: Dict[str, object]) -> None:
        with self._lock:
            self.topics.append((topic, payload))

    def record_subject(self, subject: str, payload: Dict[str, object]) -> None:
        with self._lock:
            self.subjects.append((subject, payload))


class _ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class _BrokerHandler(BaseHTTPRequestHandler):
    server: _ThreadedHTTPServer  # type: ignore[assignment]

    def do_GET(self) -> None:  # noqa: N802 - required signature
        if self.path == "/health":
            self.send_response(HTTPStatus.OK)
            self.end_headers()
            return
        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802 - required signature
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            payload = {}

        if self.path.startswith("/topics/"):
            if self.server.state.topic_failures > 0:
                self.server.state.topic_failures -= 1
                self.send_response(HTTPStatus.SERVICE_UNAVAILABLE)
                self.end_headers()
                return
            topic = self.path.split("/topics/", 1)[1]
            self.server.state.record_topic(topic, payload)
        elif self.path.startswith("/subjects/"):
            if self.server.state.subject_failures > 0:
                self.server.state.subject_failures -= 1
                self.send_response(HTTPStatus.SERVICE_UNAVAILABLE)
                self.end_headers()
                return
            subject = self.path.split("/subjects/", 1)[1]
            self.server.state.record_subject(subject, payload)
        else:
            self.send_response(HTTPStatus.NOT_FOUND)
            self.end_headers()
            return

        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003 - mirror BaseHTTPRequestHandler
        return


@pytest.fixture
def mock_broker_server() -> Tuple[_BrokerState, str]:
    state = _BrokerState()
    server = _ThreadedHTTPServer(("127.0.0.1", 0), _BrokerHandler)
    server.state = state  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    time.sleep(0.05)
    base_url = f"http://127.0.0.1:{server.server_port}"

    try:
        yield state, base_url
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _configure_account(monkeypatch: pytest.MonkeyPatch, account: str, base_url: str) -> None:
    prefix = account.upper()
    monkeypatch.setenv(f"AETHER_{prefix}_KAFKA_BOOTSTRAP", base_url)
    monkeypatch.setenv(f"AETHER_{prefix}_KAFKA_TOPIC_PREFIX", account)
    monkeypatch.setenv(f"AETHER_{prefix}_NATS_SERVERS", base_url)
    monkeypatch.setenv(f"AETHER_{prefix}_NATS_SUBJECT_PREFIX", account)


def test_publish_delivers_to_all_transports(mock_broker_server: Tuple[_BrokerState, str], monkeypatch: pytest.MonkeyPatch) -> None:
    KafkaNATSAdapter.reset()
    state, base_url = mock_broker_server
    account = "integration"
    _configure_account(monkeypatch, account, base_url)

    adapter = KafkaNATSAdapter(account_id=account)
    asyncio.run(adapter.publish("events.trade", {"id": "abc123", "qty": 1}))

    assert state.topics, "Kafka endpoint did not receive any messages"
    assert state.subjects, "NATS endpoint did not receive any messages"

    history = adapter.history()
    assert history[-1]["delivered"] is True
    assert history[-1]["partial_delivery"] is False

    KafkaNATSAdapter.shutdown()


def test_flush_retries_failed_publishes(mock_broker_server: Tuple[_BrokerState, str], monkeypatch: pytest.MonkeyPatch) -> None:
    KafkaNATSAdapter.reset()
    state, base_url = mock_broker_server
    account = "integration"
    _configure_account(monkeypatch, account, base_url)

    adapter = KafkaNATSAdapter(account_id=account, max_retries=1, backoff_seconds=0.01)

    state.topic_failures = 1
    state.subject_failures = 1
    with pytest.raises(PublishError):
        asyncio.run(adapter.publish("events.retry", {"attempt": 1}))

    assert not state.topics

    state.topic_failures = 0
    counts = asyncio.run(KafkaNATSAdapter.flush_events())
    assert counts.get(account) == 1
    assert state.topics, "Expected buffered event to reach Kafka after flush"

    history = adapter.history()
    assert history[-1]["delivered"] is True

    KafkaNATSAdapter.shutdown()
