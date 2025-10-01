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


def setup_function() -> None:
    KrakenSecretStore.reset()
    TimescaleAdapter.reset_rotation_state()
    TimescaleAdapter.reset()


def _mock_kubernetes(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    core = MagicMock()
    monkeypatch.setattr(k8s, "client", SimpleNamespace(CoreV1Api=lambda: core))
    monkeypatch.setattr(k8s, "config", SimpleNamespace(load_incluster_config=lambda: None))
    return core


def test_upsert_secret_writes_to_kubernetes_and_timescale(monkeypatch: pytest.MonkeyPatch) -> None:
    core = _mock_kubernetes(monkeypatch)

    response = client.post(
        "/secrets/kraken",
        json={"account_id": "admin-eu", "api_key": "new-key", "api_secret": "new-secret"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADER},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == "admin-eu"
    assert body["secret_name"] == "kraken-keys-admin-eu"
    assert "created_at" in body
    assert "rotated_at" in body
    assert "api_key" not in body
    assert "api_secret" not in body

    core.patch_namespaced_secret.assert_called_once()
    _, kwargs = core.patch_namespaced_secret.call_args
    assert kwargs["name"] == "kraken-keys-admin-eu"
    assert kwargs["namespace"] == "aether-secrets"
    assert kwargs["body"]["stringData"] == {"api_key": "new-key", "api_secret": "new-secret"}

    status_response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "admin-eu"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADER},
    )

    assert status_response.status_code == 200
    status_body = status_response.json()
    assert status_body["secret_name"] == "kraken-keys-admin-eu"
    assert status_body["account_id"] == "admin-eu"
    assert "rotated_at" in status_body
    assert "created_at" in status_body


def test_upsert_secret_requires_mfa() -> None:
    response = client.post(
        "/secrets/kraken",
        json={"account_id": "admin-eu", "api_key": "new-key", "api_secret": "new-secret"},
        headers={"X-Account-ID": "admin-eu"},
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"][-1] == "X-MFA-Context"


def test_status_requires_authorized_account(monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_kubernetes(monkeypatch)

    client.post(
        "/secrets/kraken",
        json={"account_id": "admin-eu", "api_key": "new-key", "api_secret": "new-secret"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADER},
    )

    response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "admin-eu"},
        headers={"X-Account-ID": "guest", **MFA_HEADER},
    )

    assert response.status_code == 403


def test_manager_rotation_updates_status_metadata() -> None:
    manager = KrakenSecretManager(account_id="admin-eu")

    first_rotation = manager.rotate_credentials(api_key="first", api_secret="secret")
    second_rotation = manager.rotate_credentials(api_key="second", api_secret="secret-2")

    status = manager.status()

    assert status is not None
    assert status["secret_name"] == manager.secret_name
    assert status["created_at"] == first_rotation["metadata"]["created_at"]
    assert status["rotated_at"] == second_rotation["metadata"]["rotated_at"]
