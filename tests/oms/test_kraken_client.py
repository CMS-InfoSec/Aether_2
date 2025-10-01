from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from services.common.adapters import TimescaleAdapter
from services.oms.kraken_client import KrakenWSClient, _LoopbackSession
from services.oms.oms_kraken import KrakenCredentialWatcher
from shared.k8s import KrakenSecretStore, KubernetesSecretClient


def setup_function() -> None:
    KrakenSecretStore.reset()
    TimescaleAdapter.reset()
    KrakenCredentialWatcher.reset_instances()


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
