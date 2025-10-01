from services.common.adapters import KrakenSecretManager, TimescaleAdapter
from shared.k8s import KrakenSecretStore


def test_secret_manager_get_credentials_loads_values() -> None:
    KrakenSecretStore.reset()
    TimescaleAdapter.reset()

    store = KrakenSecretStore()
    store.write_credentials("admin-eu", api_key="api-key", api_secret="api-secret")

    timescale = TimescaleAdapter(account_id="admin-eu")
    manager = KrakenSecretManager(
        account_id="admin-eu", secret_store=store, timescale=timescale
    )

    credentials = manager.get_credentials()

    assert credentials == {"api_key": "api-key", "api_secret": "api-secret"}

    events = timescale._events["admin-eu"]["events"]  # type: ignore[attr-defined]
    assert events
    last_event = events[-1]
    assert last_event["event_type"] == "kraken.credentials.access"
    assert last_event["payload"]["secret_name"] == manager.secret_name
