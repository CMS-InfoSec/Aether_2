from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.common.adapters import KrakenSecretManager, TimescaleAdapter
from services.secrets.main import app
from shared import k8s
from shared.k8s import KrakenSecretStore

client = TestClient(app)

MFA_HEADER = {"X-MFA-Context": "verified"}


@pytest.fixture(autouse=True)
def _reset_state() -> None:
    KrakenSecretStore.reset()
    TimescaleAdapter.reset()
    yield
    KrakenSecretStore.reset()
    TimescaleAdapter.reset()


def _mock_kubernetes(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    core = MagicMock()
    monkeypatch.setattr(k8s, "client", SimpleNamespace(CoreV1Api=lambda: core))
    monkeypatch.setattr(k8s, "config", SimpleNamespace(load_incluster_config=lambda: None))
    return core


def test_rotate_and_fetch_status_includes_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_kubernetes(monkeypatch)

    response = client.post(
        "/secrets/kraken",
        json={"account_id": "admin-eu", "api_key": "new-key", "api_secret": "new-secret"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADER},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["secret_name"] == "kraken-keys-admin-eu"
    assert "created_at" in payload
    assert "rotated_at" in payload

    status_response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "admin-eu"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADER},
    )

    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["secret_name"] == "kraken-keys-admin-eu"
    assert "created_at" in status_payload
    assert "rotated_at" in status_payload


def test_manager_rotate_credentials_includes_metadata() -> None:
    secret_store = MagicMock()
    secret_store.secret_name.return_value = "kraken-keys-admin-eu"

    timescale = TimescaleAdapter(account_id="admin-eu")
    manager = KrakenSecretManager(
        account_id="admin-eu",
        secret_store=secret_store,
        timescale=timescale,
    )

    result = manager.rotate_credentials(api_key="new", api_secret="secret")

    secret_store.write_credentials.assert_called_once_with(
        "admin-eu", api_key="new", api_secret="secret"
    )

    metadata = result["metadata"]
    assert metadata["secret_name"] == "kraken-keys-admin-eu"
    assert metadata["created_at"]
    assert metadata["rotated_at"]

    rotation_events = timescale.credential_events()
    assert any(
        event["event"] == "rotation"
        and event["metadata"]["secret_name"] == "kraken-keys-admin-eu"
        for event in rotation_events
    )
