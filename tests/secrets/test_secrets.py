from __future__ import annotations

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.common.adapters import TimescaleAdapter
from services.secrets.main import app
from shared.k8s import KubernetesSecretClient

client = TestClient(app)

MFA_HEADER = {"X-MFA-Context": "verified"}


def setup_function() -> None:
    KubernetesSecretClient.reset()
    TimescaleAdapter.reset_rotation_state()


def test_rotate_secret_writes_to_kubernetes_and_timescale() -> None:
    response = client.post(
        "/secrets/kraken/rotate",
        json={"account_id": "admin-eu", "api_key": "new-key", "api_secret": "new-secret"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADER},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == "admin-eu"
    assert body["secret_name"] == "kraken-admin-eu"
    assert "rotated_at" in body
    assert "api_key" not in body
    assert "api_secret" not in body

    k8s_client = KubernetesSecretClient(namespace="aether-secrets")
    stored = k8s_client.get_secret("kraken-admin-eu")
    assert stored == {"api_key": "new-key", "api_secret": "new-secret"}

    status_response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "admin-eu"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADER},
    )

    assert status_response.status_code == 200
    status_body = status_response.json()
    assert status_body["secret_name"] == "kraken-admin-eu"
    assert status_body["account_id"] == "admin-eu"
    assert "rotated_at" in status_body
    assert "created_at" in status_body


def test_rotate_secret_requires_mfa() -> None:
    response = client.post(
        "/secrets/kraken/rotate",
        json={"account_id": "admin-eu", "api_key": "new-key", "api_secret": "new-secret"},
        headers={"X-Account-ID": "admin-eu"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "MFA context is invalid or incomplete."


def test_status_requires_authorized_account() -> None:
    client.post(
        "/secrets/kraken/rotate",
        json={"account_id": "admin-eu", "api_key": "new-key", "api_secret": "new-secret"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADER},
    )

    response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "admin-eu"},
        headers={"X-Account-ID": "guest", **MFA_HEADER},
    )

    assert response.status_code == 403
