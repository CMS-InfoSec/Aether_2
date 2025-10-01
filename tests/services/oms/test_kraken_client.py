from __future__ import annotations

from services.common.adapters import KrakenSecretManager, TimescaleAdapter
from services.oms.kraken_client import KrakenWSClient
from shared.k8s import KubernetesSecretClient


def test_kraken_ws_client_loads_credentials_from_kubernetes_secret() -> None:
    account_id = "test-account"
    namespace = "aether-secrets"
    secret_name = f"kraken-keys-{account_id}"

    KubernetesSecretClient.reset()
    TimescaleAdapter.reset()

    client = KubernetesSecretClient(namespace=namespace)
    client.write_credentials(account_id, api_key="test-key", api_secret="test-secret")

    ws_client = KrakenWSClient(account_id)
    assert ws_client._credentials["api_key"] == "test-key"
    assert ws_client._credentials["api_secret"] == "test-secret"
    assert ws_client._credentials["metadata"]["material_present"] is True

    manager = KrakenSecretManager(account_id)
    events = manager.timescale.credential_events() if manager.timescale else []
    assert events, "Expected a credential access audit entry to be recorded"

    access_event = events[-1]
    assert access_event["event_type"] == "kraken.credentials.access"
    assert access_event["secret_name"] == secret_name

    metadata = access_event["metadata"]
    assert metadata["api_key"] == "***"
    assert metadata["api_secret"] == "***"
    assert metadata["material_present"] is True


def test_kraken_secret_manager_rotation_status_includes_created_at() -> None:
    account_id = "rotation-account"

    KubernetesSecretClient.reset()
    TimescaleAdapter.reset()

    manager = KrakenSecretManager(account_id)

    manager.rotate_credentials(api_key="first", api_secret="secret")
    first_status = manager.status()
    assert first_status is not None
    assert first_status["created_at"] == first_status["rotated_at"]

    manager.rotate_credentials(api_key="second", api_secret="secret")
    second_status = manager.status()
    assert second_status is not None
    assert second_status["created_at"] == first_status["created_at"]
    assert second_status["rotated_at"] != first_status["rotated_at"]
