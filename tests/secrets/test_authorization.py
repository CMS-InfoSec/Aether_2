"""Authorization tests for the standalone Kraken secrets service."""

from __future__ import annotations

import base64
import importlib
import sys
from dataclasses import dataclass
from typing import Any, Dict

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


@dataclass
class _FakeSecretManager:
    """Minimal secret manager that records how it was used."""

    last_upsert: Dict[str, Any] | None = None
    last_status_account: str | None = None
    last_test_account: str | None = None

    def upsert_secret(self, account_id: str, payload: Dict[str, str], actor: str) -> Dict[str, str]:
        self.last_upsert = {"account_id": account_id, "payload": payload, "actor": actor}
        return {"secret_name": f"kraken-keys-{account_id}", "last_rotated": "2024-01-01T00:00:00Z"}

    def get_status(self, account_id: str) -> Dict[str, str]:
        self.last_status_account = account_id
        return {"secret_name": f"kraken-keys-{account_id}", "last_rotated": "2024-01-01T00:00:00Z"}

    def get_decrypted_credentials(self, account_id: str) -> Dict[str, str]:
        self.last_test_account = account_id
        return {"api_key": "demo", "api_secret": "demo-secret"}


@pytest.fixture
def secrets_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Create a FastAPI test client with authentication overrides applied."""

    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", base64.b64encode(b"a" * 32).decode())
    monkeypatch.setenv("KRAKEN_SECRETS_ALLOWED_ADMINS", "admin@example.com")
    monkeypatch.setenv("KRAKEN_SECRETS_SERVICE_TOKENS", "service-token-123")

    import kubernetes.config as k8s_config

    monkeypatch.setattr(k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(k8s_config, "load_kube_config", lambda: None)

    if "secrets_service" in sys.modules:
        module = importlib.reload(sys.modules["secrets_service"])
    else:
        module = importlib.import_module("secrets_service")

    fake_manager = _FakeSecretManager()
    module.app.dependency_overrides[module.get_secret_manager] = lambda: fake_manager

    async def _fake_balance(*_: Any, **__: Any) -> Dict[str, Any]:
        return {"error": [], "result": {"balance": {}}}

    monkeypatch.setattr(module, "kraken_get_balance", _fake_balance)

    client = TestClient(module.app)
    client._secrets_module = module  # type: ignore[attr-defined]
    client._fake_manager = fake_manager  # type: ignore[attr-defined]
    yield client

    module.app.dependency_overrides.clear()


def test_store_secret_requires_credentials(secrets_client: TestClient) -> None:
    response = secrets_client.post(
        "/secrets/kraken",
        json={"account_id": "acct", "api_key": "key", "api_secret": "secret"},
    )

    assert response.status_code == 401


def test_store_secret_allows_authorized_admin(secrets_client: TestClient) -> None:
    response = secrets_client.post(
        "/secrets/kraken",
        json={"account_id": "acct", "api_key": "key", "api_secret": "secret"},
        headers={"X-Admin-ID": "admin@example.com"},
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["secret_name"] == "kraken-keys-acct"


def test_status_requires_authorization(secrets_client: TestClient) -> None:
    response = secrets_client.get(
        "/secrets/kraken/status",
        params={"account_id": "acct"},
    )

    assert response.status_code == 401


def test_status_allows_service_token(secrets_client: TestClient) -> None:
    response = secrets_client.get(
        "/secrets/kraken/status",
        params={"account_id": "acct"},
        headers={"Authorization": "Bearer service-token-123"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["secret_name"] == "kraken-keys-acct"


def test_test_endpoint_requires_authorization(secrets_client: TestClient) -> None:
    response = secrets_client.post(
        "/secrets/kraken/test",
        json={"account_id": "acct"},
    )

    assert response.status_code == 401


def test_test_endpoint_allows_service_token(secrets_client: TestClient) -> None:
    response = secrets_client.post(
        "/secrets/kraken/test",
        json={"account_id": "acct"},
        headers={"Authorization": "Bearer service-token-123"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["result"] == "success"
