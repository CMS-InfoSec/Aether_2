from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.secrets import secrets_service


MFA_HEADERS = {"X-MFA-Context": "verified"}


@pytest.fixture()
def kubernetes_core() -> MagicMock:
    core = MagicMock()
    secrets_service.app.dependency_overrides[secrets_service.get_core_v1_api] = lambda: core
    try:
        yield core
    finally:
        secrets_service.app.dependency_overrides.pop(secrets_service.get_core_v1_api, None)


@pytest.fixture()
def secure_api_client(kubernetes_core: MagicMock) -> tuple[TestClient, MagicMock]:
    client = TestClient(secrets_service.app, base_url="https://testserver")
    try:
        yield client, kubernetes_core
    finally:
        client.close()


def _mock_secret_metadata(secret_name: str, *, rotated_at: datetime) -> SimpleNamespace:
    metadata = SimpleNamespace(
        name=secret_name,
        annotations={
            secrets_service.ANNOTATION_CREATED_AT: rotated_at.isoformat(),
            secrets_service.ANNOTATION_ROTATED_AT: rotated_at.isoformat(),
        },
    )
    return SimpleNamespace(metadata=metadata)


def test_rotate_secret_creates_when_missing(
    secure_api_client: tuple[TestClient, MagicMock]
) -> None:
    client, core = secure_api_client

    api_exception = secrets_service.ApiException(status=404)
    core.patch_namespaced_secret.side_effect = api_exception
    core.read_namespaced_secret.side_effect = api_exception

    response = client.post(
        "/secrets/kraken",
        json={
            "account_id": "admin-eu",
            "api_key": "new-key",
            "api_secret": "new-secret",
        },
        headers={"X-Account-ID": "admin-eu", **MFA_HEADERS},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["secret_name"] == "kraken-keys-admin-eu"
    assert "last_rotated_at" in payload

    core.patch_namespaced_secret.assert_called_once()
    core.create_namespaced_secret.assert_called_once()


def test_status_returns_rotation_timestamp(
    secure_api_client: tuple[TestClient, MagicMock]
) -> None:
    client, core = secure_api_client

    rotated_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    secret_name = "kraken-keys-admin-eu"
    core.read_namespaced_secret.return_value = _mock_secret_metadata(secret_name, rotated_at=rotated_at)

    response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "admin-eu"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADERS},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["account_id"] == "admin-eu"
    assert payload["secret_name"] == secret_name
    assert datetime.fromisoformat(payload["last_rotated_at"].replace("Z", "+00:00")) == rotated_at


def test_rejects_insecure_transport(kubernetes_core: MagicMock) -> None:
    client = TestClient(secrets_service.app, base_url="http://testserver")
    try:
        response = client.post(
            "/secrets/kraken",
            json={
                "account_id": "admin-eu",
                "api_key": "new-key",
                "api_secret": "new-secret",
            },
            headers={"X-Account-ID": "admin-eu", **MFA_HEADERS},
        )
    finally:
        client.close()

    assert response.status_code == 400
    assert response.json()["detail"] == "TLS termination required (https only)."

