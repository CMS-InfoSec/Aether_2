from services.common.adapters import KrakenSecretManager, TimescaleAdapter
from shared.k8s import KrakenSecretStore


def test_secret_manager_get_credentials_loads_values() -> None:
    KrakenSecretStore.reset()
    TimescaleAdapter.reset()

    store = KrakenSecretStore()
    store.write_credentials("company", api_key="api-key", api_secret="api-secret")

    timescale = TimescaleAdapter(account_id="company")
    manager = KrakenSecretManager(
        account_id="company", secret_store=store, timescale=timescale
    )

    credentials = manager.get_credentials()

    assert credentials["api_key"] == "api-key"
    assert credentials["api_secret"] == "api-secret"
    metadata = credentials["metadata"]
    assert metadata["api_key"] == "***"
    assert metadata["api_secret"] == "***"
    assert metadata["secret_name"] == manager.secret_name

    events = timescale._events["company"]["events"]  # type: ignore[attr-defined]
    assert events
    last_event = events[-1]
    assert last_event["event_type"] == "kraken.credentials.access"
    assert last_event["payload"]["secret_name"] == manager.secret_name

    audit_events = timescale.credential_events()
    assert audit_events
    audit_event = audit_events[-1]
    assert audit_event["event_type"] == "kraken.credentials.access"
    assert audit_event["secret_name"] == manager.secret_name
    assert audit_event["metadata"]["material_present"] is True
    assert audit_event["metadata"]["api_key"] == "***"
    assert audit_event["metadata"]["api_secret"] == "***"
