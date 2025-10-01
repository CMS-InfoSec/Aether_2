from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.common.adapters import TimescaleAdapter
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


def test_status_returns_rotation_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_kubernetes(monkeypatch)

    response = client.post(
        "/secrets/kraken",
        json={
            "account_id": "admin-eu",
            "api_key": "rotated-key",
            "api_secret": "rotated-secret",
        },
        headers={"X-Account-ID": "admin-eu", **MFA_HEADER},
    )

    assert response.status_code == 200

    status_response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "admin-eu"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADER},
    )

    assert status_response.status_code == 200

    rotation_metadata = response.json()
    status_body = status_response.json()

    assert status_body["account_id"] == "admin-eu"
    assert status_body["secret_name"] == "kraken-keys-admin-eu"
    assert status_body["created_at"] == rotation_metadata["created_at"]
    assert status_body["rotated_at"] == rotation_metadata["rotated_at"]
