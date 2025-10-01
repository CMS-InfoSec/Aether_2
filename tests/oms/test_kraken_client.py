from __future__ import annotations

import json
import queue
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from services.common.adapters import TimescaleAdapter
from services.oms.kraken_client import (
    KrakenCredentialExpired,
    KrakenWSClient,
    _LoopbackSession,
)
from services.oms.oms_kraken import ANNOTATION_ROTATED_AT, KrakenCredentialWatcher
from shared.k8s import KrakenSecretStore, KubernetesSecretClient


def setup_function() -> None:
    KrakenSecretStore.reset()
    TimescaleAdapter.reset()
    KrakenCredentialWatcher.reset_instances()


def test_ws_client_rejects_expired_credentials() -> None:
    rotated_at = datetime.now(timezone.utc) - timedelta(days=91)
    client = KrakenWSClient(
        "company",
        credentials={
            "api_key": "key",
            "api_secret": "secret",
            "metadata": {"rotated_at": rotated_at},
        },
    )

    with pytest.raises(KrakenCredentialExpired):
        client.add_order({"clientOrderId": "EXPIRED"})


def test_ws_client_rejects_existing_session_after_expiry() -> None:
    fresh_rotated_at = datetime.now(timezone.utc) - timedelta(days=10)
    credentials: Dict[str, Any] = {
        "api_key": "key",
        "api_secret": "secret",
        "metadata": {"rotated_at": fresh_rotated_at},
    }
    client = KrakenWSClient("company", credentials=credentials)

    client.add_order({"clientOrderId": "INITIAL"})
    assert client._session is not None

    credentials["metadata"]["rotated_at"] = datetime.now(timezone.utc) - timedelta(days=91)

    with pytest.raises(KrakenCredentialExpired):
        client.add_order({"clientOrderId": "STALE"})

    assert client._session is None


def test_ws_client_loads_credentials() -> None:
    store = KubernetesSecretClient()
    store.write_credentials("company", api_key="key-123", api_secret="secret-456")

    client = KrakenWSClient("company")

    assert client._credentials["api_key"] == "key-123"
    assert client._credentials["api_secret"] == "secret-456"
    assert client._credentials["metadata"]["api_key"] == "***"
    assert client._credentials["metadata"]["api_secret"] == "***"
    assert client._credentials["metadata"]["secret_name"] == "kraken-keys-company"

    events = TimescaleAdapter(account_id="company").credential_events()
    assert events
    access_event = events[-1]
    assert access_event["event_type"] == "kraken.credentials.access"
    assert access_event["metadata"]["secret_name"] == "kraken-keys-company"
    assert access_event["metadata"]["material_present"] is True


def test_client_reloads_credentials_on_secret_change(tmp_path: Path) -> None:
    secret_file = tmp_path / "kraken.json"
    secret_file.write_text(json.dumps({"key": "key-1", "secret": "sec-1"}))

    watcher = KrakenCredentialWatcher(
        "company",
        secret_path=secret_file,
        refresh_interval=0.2,
        debounce_seconds=0.1,
    )

    session_credentials: List[Dict[str, str]] = []

    def factory(creds: Dict[str, str]) -> _LoopbackSession:
        session_credentials.append({"api_key": creds["api_key"], "api_secret": creds["api_secret"]})
        return _LoopbackSession(creds)

    client = KrakenWSClient("company", session_factory=factory, credential_source=watcher)

    try:
        client.add_order({"clientOrderId": "A"})
        assert session_credentials[-1]["api_key"] == "key-1"

        secret_file.write_text(json.dumps({"key": "key-2", "secret": "sec-2"}))
        watcher.trigger_refresh()
        assert watcher.wait_for_version(1, timeout=2.0)

        client.add_order({"clientOrderId": "B"})
        assert session_credentials[-1]["api_key"] == "key-2"
        assert len(session_credentials) == 2
    finally:
        client.close()
        watcher.close()


def test_kubernetes_watch_triggers_refresh() -> None:
    class StubCoreV1:
        def list_namespaced_secret(self, namespace: str, field_selector: str | None = None, timeout_seconds: int | None = None) -> List[Dict[str, str]]:
            return []

    class StubManager:
        def __init__(self) -> None:
            self.secret_store = SimpleNamespace(
                namespace="aether-secrets",
                core_v1=StubCoreV1(),
            )
            self.secret_name = "kraken-keys-company"
            self._counter = 0

        def get_credentials(self) -> Dict[str, Any]:  # type: ignore[override]
            self._counter += 1
            rotated = (datetime.now(timezone.utc) + timedelta(seconds=self._counter)).isoformat()
            return {
                "api_key": f"key-{self._counter}",
                "api_secret": f"secret-{self._counter}",
                "metadata": {
                    "annotations": {ANNOTATION_ROTATED_AT: rotated},
                },
            }

    class StubWatch:
        def __init__(self) -> None:
            self._queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
            self._stopped = False

        def stream(self, func: Any, *args: Any, **kwargs: Any):  # noqa: ANN001 - signature matches kubernetes watch
            while not self._stopped:
                try:
                    event = self._queue.get(timeout=0.1)
                except queue.Empty:
                    if self._stopped:
                        break
                    continue
                yield event

        def stop(self) -> None:
            self._stopped = True

        def emit(self, event: Dict[str, Any]) -> None:
            self._queue.put(event)

    stub_watch = StubWatch()
    manager = StubManager()
    watcher = KrakenCredentialWatcher(
        "company",
        manager=manager,
        watch_factory=lambda: stub_watch,
        refresh_interval=30.0,
        debounce_seconds=0.0,
    )

    try:
        time.sleep(0.1)
        initial_version = watcher.snapshot()[1]
        stub_watch.emit({"type": "MODIFIED", "object": {"metadata": {"name": manager.secret_name}}})
        assert watcher.wait_for_version(initial_version + 1, timeout=2.0)
    finally:
        watcher.close()


def test_client_continues_order_flow_during_rotation(tmp_path: Path) -> None:
    secret_file = tmp_path / "kraken.json"
    secret_file.write_text(json.dumps({"key": "init", "secret": "first"}))

    watcher = KrakenCredentialWatcher(
        "company",
        secret_path=secret_file,
        refresh_interval=0.2,
        debounce_seconds=0.05,
    )

    class RecordingSession(_LoopbackSession):
        def __init__(self, creds: Dict[str, str]) -> None:
            super().__init__(creds)
            self.credentials = creds

    sessions: List[RecordingSession] = []

    def factory(creds: Dict[str, str]) -> RecordingSession:
        session = RecordingSession(creds)
        sessions.append(session)
        return session

    client = KrakenWSClient("company", session_factory=factory, credential_source=watcher)

    try:
        response = client.add_order({"clientOrderId": "1"})
        assert response["status"] == "ok"
        assert sessions[-1].credentials["api_key"] == "init"

        secret_file.write_text(json.dumps({"key": "rotated", "secret": "second"}))
        watcher.trigger_refresh()
        assert watcher.wait_for_version(1, timeout=2.0)

        # Ensure outstanding session can still service follow-up calls before reconnect
        open_snapshot = client.open_orders()
        assert open_snapshot["status"] == "ok"

        response = client.add_order({"clientOrderId": "2"})
        assert response["status"] == "ok"
        assert sessions[-1].credentials["api_key"] == "rotated"
        assert len(sessions) == 2
    finally:
        client.close()
        watcher.close()
