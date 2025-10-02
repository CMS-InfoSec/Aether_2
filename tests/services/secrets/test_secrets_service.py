from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.common import security
from services.secrets import secrets_service


MFA_HEADERS = {"X-MFA-Context": "verified"}


@pytest.fixture()
def kubernetes_core() -> MagicMock:
    core = MagicMock()
    secrets_service.app.dependency_overrides[secrets_service.get_core_v1_api] = lambda: core
    secrets_service.app.dependency_overrides[secrets_service.require_admin_account] = (
        lambda: "admin-eu"
    )
    secrets_service.app.dependency_overrides[secrets_service.require_mfa_context] = (
        lambda: "verified"
    )
    secrets_service.app.dependency_overrides[
        secrets_service.require_dual_director_confirmation
    ] = lambda: ("director-a", "director-b")
    security.ADMIN_ACCOUNTS.add("admin-eu")
    try:
        yield core
    finally:
        security.ADMIN_ACCOUNTS.discard("admin-eu")
        secrets_service.app.dependency_overrides.pop(secrets_service.get_core_v1_api, None)
        secrets_service.app.dependency_overrides.pop(
            secrets_service.require_admin_account, None
        )
        secrets_service.app.dependency_overrides.pop(
            secrets_service.require_mfa_context, None
        )
        secrets_service.app.dependency_overrides.pop(
            secrets_service.require_dual_director_confirmation, None
        )


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


def test_force_rotate_updates_annotations(
    secure_api_client: tuple[TestClient, MagicMock]
) -> None:
    client, core = secure_api_client

    secret_name = "kraken-keys-admin-eu"
    original_rotated = "2024-01-01T00:00:00+00:00"
    annotations = {
        secrets_service.ANNOTATION_CREATED_AT: "2024-01-01T00:00:00+00:00",
        secrets_service.ANNOTATION_ROTATED_AT: original_rotated,
    }
    metadata = SimpleNamespace(name=secret_name, annotations=annotations)
    core.read_namespaced_secret.return_value = SimpleNamespace(metadata=metadata)

    response = client.post(
        "/secrets/kraken/force_rotate",
        json={"account_id": "admin-eu"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADERS},
    )

    assert response.status_code == 204
    patch_kwargs = core.patch_namespaced_secret.call_args.kwargs
    assert patch_kwargs["name"] == secret_name
    annotations = patch_kwargs["body"]["metadata"]["annotations"]
    assert annotations[secrets_service.ANNOTATION_CREATED_AT] == "2024-01-01T00:00:00+00:00"
    new_rotated = annotations[secrets_service.ANNOTATION_ROTATED_AT]
    assert new_rotated != original_rotated
    parsed_rotated = datetime.fromisoformat(new_rotated.replace("Z", "+00:00"))
    assert parsed_rotated > datetime.fromisoformat(original_rotated.replace("Z", "+00:00"))


def test_force_rotate_missing_secret_returns_not_found(
    secure_api_client: tuple[TestClient, MagicMock]
) -> None:
    client, core = secure_api_client

    core.read_namespaced_secret.return_value = None

    response = client.post(
        "/secrets/kraken/force_rotate",
        json={"account_id": "admin-eu"},
        headers={"X-Account-ID": "admin-eu", **MFA_HEADERS},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Kraken credentials not found for account"


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

