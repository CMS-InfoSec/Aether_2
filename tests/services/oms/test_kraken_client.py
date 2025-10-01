from __future__ import annotations

from services.common.adapters import KrakenSecretManager, TimescaleAdapter
from services.oms.kraken_client import KrakenWSClient
from shared.k8s import KubernetesSecretClient


def test_kraken_ws_client_loads_credentials_from_kubernetes_secret() -> None:
    account_id = "test-account"
    namespace = "aether-secrets"
    secret_name = f"kraken-{account_id}"

    KubernetesSecretClient.reset()
    TimescaleAdapter.reset()

    client = KubernetesSecretClient(namespace=namespace)
    client.patch_secret(secret_name, {"api_key": "test-key", "api_secret": "test-secret"})

    ws_client = KrakenWSClient(account_id)
    assert ws_client._credentials == {"api_key": "test-key", "api_secret": "test-secret"}

    manager = KrakenSecretManager(account_id)
    events = manager.timescale.credential_events() if manager.timescale else []
    assert events, "Expected a credential access audit entry to be recorded"

    access_event = events[-1]
    assert access_event["event"] == "access"
    assert access_event["secret_name"] == secret_name

    metadata = access_event["metadata"]
    assert metadata["api_key"] == "***"
    assert metadata["api_secret"] == "***"
    assert metadata["material_present"] is True
