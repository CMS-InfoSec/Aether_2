from __future__ import annotations

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi import status  # noqa: E402  # pragma: no cover
from fastapi.testclient import TestClient  # noqa: E402  # pragma: no cover

from services.secrets import middleware, secrets_service  # noqa: E402  # pragma: no cover
from services.secrets.secrets_service import app  # noqa: E402


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    secrets_service.app.dependency_overrides[secrets_service.require_admin_account] = (
        lambda: "company"
    )
    secrets_service.app.dependency_overrides[secrets_service.require_mfa_context] = (
        lambda: "verified"
    )
    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()
        secrets_service.app.dependency_overrides.pop(
            secrets_service.require_admin_account, None
        )
        secrets_service.app.dependency_overrides.pop(
            secrets_service.require_mfa_context, None
        )


def test_forwarded_proto_allows_secure_requests(client: TestClient) -> None:
    response = client.post(
        "/secrets/kraken/test",
        json={
            "account_id": "company",
            "api_key": "apikey-123",
            "api_secret": "secret-value-123",
        },
        headers={
            "X-Account-ID": "company",
            "X-MFA-Token": "verified",
            "X-Forwarded-Proto": "https",
        },
    )

    assert response.status_code == 204


def test_forwarded_proto_rejected_for_untrusted_clients(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from services.secrets import secrets_service

    monkeypatch.setattr(secrets_service, "TRUSTED_PROXY_CLIENTS", ("127.0.0.1",))
    monkeypatch.setattr(middleware, "TRUSTED_PROXY_CLIENTS", ("127.0.0.1",))

    response = client.post(
        "/secrets/kraken/test",
        json={
            "account_id": "company",
            "api_key": "apikey-123",
            "api_secret": "secret-value-123",
        },
        headers={
            "X-Account-ID": "company",
            "X-MFA-Token": "verified",
            "X-Forwarded-Proto": "https",
        },
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()["detail"] == "TLS termination required (https only)."
