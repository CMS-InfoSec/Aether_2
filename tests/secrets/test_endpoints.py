from __future__ import annotations

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.secrets.main import app


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


def test_rotate_rejects_mismatched_account(client: TestClient) -> None:
    response = client.post(
        "/secrets/kraken/rotate",
        json={"account_id": "admin-eu", "api_key": "a", "api_secret": "b"},
        headers={"X-Account-ID": "admin-us", "X-MFA-Context": "verified"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Account mismatch between header and payload."


def test_status_returns_not_found_without_rotation(client: TestClient) -> None:
    response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "admin-eu"},
        headers={"X-Account-ID": "admin-eu", "X-MFA-Context": "verified"},
    )

    assert response.status_code == 404


def test_mfa_context_must_be_verified(client: TestClient) -> None:
    response = client.post(
        "/secrets/kraken/rotate",
        json={"account_id": "admin-eu", "api_key": "a", "api_secret": "b"},
        headers={"X-Account-ID": "admin-eu", "X-MFA-Context": "unverified"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "MFA context is invalid or incomplete."
