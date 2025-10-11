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

MFA_HEADER = {"X-MFA-Token": "verified"}


def setup_function() -> None:
    KrakenSecretStore.reset()
    TimescaleAdapter.reset()


def _mock_kubernetes(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    core = MagicMock()
    monkeypatch.setattr(k8s, "client", SimpleNamespace(CoreV1Api=lambda: core))
    monkeypatch.setattr(k8s, "config", SimpleNamespace(load_incluster_config=lambda: None))
    return core


def test_status_returns_rotation_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_kubernetes(monkeypatch)

    response = client.post(
        "/secrets/kraken",
        json={
            "account_id": "company",
            "api_key": "rotated-key",
            "api_secret": "rotated-secret",
        },
        headers={"X-Account-ID": "company", **MFA_HEADER},
    )

    assert response.status_code == 200

    status_response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "company"},
        headers={"X-Account-ID": "company", **MFA_HEADER},
    )

    assert status_response.status_code == 200

    rotation_metadata = response.json()
    status_body = status_response.json()

    assert status_body["account_id"] == "company"
    assert status_body["secret_name"] == "kraken-keys-company"
    assert status_body["created_at"] == rotation_metadata["created_at"]
    assert status_body["rotated_at"] == rotation_metadata["rotated_at"]

    store = KrakenSecretStore(core_v1=SimpleNamespace())
    store.write_credentials(
        "company", api_key="rotated-key", api_secret="rotated-secret"
    )
    manager = KrakenSecretManager(account_id="company", secret_store=store)
    credentials = manager.get_credentials()
    assert credentials["metadata"]["secret_name"] == "kraken-keys-company"
    assert credentials["metadata"]["api_key"] == "***"
    assert credentials["metadata"]["api_secret"] == "***"

    events = TimescaleAdapter(account_id="company").credential_events()
    assert events
    access_event = events[-1]
    assert access_event["event_type"] == "kraken.credentials.access"
    assert access_event["metadata"]["secret_name"] == "kraken-keys-company"
    assert access_event["metadata"]["material_present"] is True
