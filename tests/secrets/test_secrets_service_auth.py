from __future__ import annotations

import base64
import importlib
import sys

from types import ModuleType, SimpleNamespace
from typing import Generator

import pytest


fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient



class FakeSecretManager:
    def __init__(self) -> None:
        self.status_payload = {
            "secret_name": "kraken-keys-initial",
            "last_rotated": "never",
        }

    def upsert_secret(self, account_id: str, payload: dict[str, str], actor: str) -> dict[str, str]:
        self.status_payload = {
            "secret_name": f"kraken-keys-{account_id}",
            "last_rotated": "2024-01-01T00:00:00+00:00",
        }
        return dict(self.status_payload)

    def get_status(self, account_id: str) -> dict[str, str]:
        return dict(self.status_payload)

    def get_decrypted_credentials(self, account_id: str) -> dict[str, str]:
        return {"api_key": "demo", "api_secret": "secret"}


@pytest.fixture
def secrets_context(monkeypatch: pytest.MonkeyPatch) -> Generator[SimpleNamespace, None, None]:
    token = "valid-token"
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", base64.b64encode(b"0" * 32).decode())
    monkeypatch.setenv("KRAKEN_SECRETS_AUTH_TOKENS", f"{token}:admin")

    fake_config_module = ModuleType("kubernetes.config")
    fake_config_module.load_incluster_config = lambda: None
    fake_config_module.load_kube_config = lambda: None

    class FakeConfigException(Exception):
        pass

    fake_config_exception = ModuleType("kubernetes.config.config_exception")
    fake_config_exception.ConfigException = FakeConfigException

    class FakeCoreV1Api:
        def __init__(self, *args, **kwargs) -> None:  # noqa: D401
            """Placeholder core client."""

    class FakeApiException(Exception):
        def __init__(self, status: int = 500, reason: str | None = None) -> None:
            super().__init__(reason)
            self.status = status

    fake_client_module = ModuleType("kubernetes.client")
    fake_client_module.CoreV1Api = FakeCoreV1Api
    fake_client_module.ApiException = FakeApiException

    fake_rest_module = ModuleType("kubernetes.client.rest")
    fake_rest_module.ApiException = FakeApiException

    fake_kubernetes_module = ModuleType("kubernetes")
    fake_kubernetes_module.config = fake_config_module
    fake_kubernetes_module.client = fake_client_module

    monkeypatch.setitem(sys.modules, "kubernetes", fake_kubernetes_module)
    monkeypatch.setitem(sys.modules, "kubernetes.config", fake_config_module)
    monkeypatch.setitem(sys.modules, "kubernetes.config.config_exception", fake_config_exception)
    monkeypatch.setitem(sys.modules, "kubernetes.client", fake_client_module)
    monkeypatch.setitem(sys.modules, "kubernetes.client.rest", fake_rest_module)

    sys.modules.pop("secrets_service", None)
    service = importlib.import_module("secrets_service")
    service.secret_manager = FakeSecretManager()  # type: ignore[assignment]

    async def _fake_balance(*_, **__) -> dict[str, dict[str, str]]:
        return {"result": {"ZUSD": "1.0"}}

    service.kraken_get_balance = _fake_balance  # type: ignore[assignment]

    try:
        yield SimpleNamespace(service=service, token=token)
    finally:
        sys.modules.pop("secrets_service", None)
        for module_name in [
            "kubernetes",
            "kubernetes.config",
            "kubernetes.config.config_exception",
            "kubernetes.client",
            "kubernetes.client.rest",
        ]:
            sys.modules.pop(module_name, None)


@pytest.fixture
def client(secrets_context: SimpleNamespace) -> TestClient:
    return TestClient(secrets_context.service.app)


def _auth_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_store_secret_requires_authorization(client: TestClient) -> None:
    response = client.post(
        "/secrets/kraken",
        json={"account_id": "acct", "api_key": "key", "api_secret": "secret"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing or invalid authorization token"


def test_store_secret_rejects_invalid_token(client: TestClient) -> None:
    response = client.post(
        "/secrets/kraken",
        json={"account_id": "acct", "api_key": "key", "api_secret": "secret"},
        headers=_auth_header("wrong"),
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Caller is not authorized to access Kraken secrets"


def test_store_secret_succeeds_with_valid_token(
    client: TestClient, secrets_context: SimpleNamespace
) -> None:
    response = client.post(
        "/secrets/kraken",
        json={
            "account_id": "acct",
            "api_key": "key",
            "api_secret": "secret",
        },
        headers=_auth_header(secrets_context.token),
    )

    assert response.status_code == 201
    assert response.json()["secret_name"] == "kraken-keys-acct"


def test_status_endpoint_requires_authorization(client: TestClient) -> None:
    response = client.get("/secrets/kraken/status", params={"account_id": "acct"})

    assert response.status_code == 401


def test_status_endpoint_authorized_success(
    client: TestClient, secrets_context: SimpleNamespace
) -> None:
    response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "acct"},
        headers=_auth_header(secrets_context.token),
    )

    assert response.status_code == 200
    assert response.json()["secret_name"] == "kraken-keys-acct"


def test_test_endpoint_requires_authorization(client: TestClient) -> None:
    response = client.post(
        "/secrets/kraken/test",
        json={"account_id": "acct"},
    )

    assert response.status_code == 401


def test_test_endpoint_authorized_success(
    client: TestClient, secrets_context: SimpleNamespace
) -> None:
    response = client.post(
        "/secrets/kraken/test",
        json={"account_id": "acct"},
        headers=_auth_header(secrets_context.token),
    )

    assert response.status_code == 200
    assert response.json()["result"] == "success"


