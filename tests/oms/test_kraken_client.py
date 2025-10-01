from __future__ import annotations

from services.common.adapters import TimescaleAdapter
from services.oms.kraken_client import KrakenWSClient
from shared.k8s import KrakenSecretStore, KubernetesSecretClient


def setup_function() -> None:
    KrakenSecretStore.reset()
    TimescaleAdapter.reset()


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
