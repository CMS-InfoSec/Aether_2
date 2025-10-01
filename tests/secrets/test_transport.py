from __future__ import annotations

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402  # pragma: no cover

from services.secrets.secrets_service import app  # noqa: E402


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


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
            "X-MFA-Context": "verified",
            "X-Forwarded-Proto": "https",
        },
    )

    assert response.status_code == 204
