import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.secrets.main import app
from shared.k8s import KrakenSecretStore


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    KrakenSecretStore.reset()
    return TestClient(app)


def test_request_missing_mfa_token_is_rejected(client: TestClient) -> None:
    response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "company"},
        headers={"X-Account-ID": "company"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing MFA verification token."


def test_request_with_valid_mfa_token_succeeds(client: TestClient) -> None:
    create_response = client.post(
        "/secrets/kraken",
        json={"account_id": "company", "api_key": "key", "api_secret": "secret"},
        headers={"X-Account-ID": "company", "X-MFA-Token": "verified"},
    )
    assert create_response.status_code == 200

    response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "company"},
        headers={"X-Account-ID": "company", "X-MFA-Token": "verified"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["account_id"] == "company"
    assert payload["secret_name"].startswith("kraken-keys-")
