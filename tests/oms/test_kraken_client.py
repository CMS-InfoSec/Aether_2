from __future__ import annotations

from services.oms.kraken_client import KrakenWSClient
from shared.k8s import KrakenSecretStore, KubernetesSecretClient


def setup_function() -> None:
    KrakenSecretStore.reset()


def test_ws_client_loads_credentials() -> None:
    store = KubernetesSecretClient()
    store.write_credentials("admin-eu", api_key="key-123", api_secret="secret-456")

    client = KrakenWSClient("admin-eu")

    assert client._credentials["api_key"] == "key-123"
    assert client._credentials["api_secret"] == "secret-456"
    assert client._credentials["metadata"]["api_key"] == "***"
    assert client._credentials["metadata"]["api_secret"] == "***"
    assert client._credentials["metadata"]["secret_name"] == "kraken-keys-admin-eu"
