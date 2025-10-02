from __future__ import annotations

import base64
import importlib
import sys
from datetime import datetime, timezone
from typing import Dict, Tuple

import pytest
fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _configure_environment(monkeypatch: pytest.MonkeyPatch, token: str) -> None:
    encryption_key = base64.b64encode(b"0" * 32).decode()
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", encryption_key)
    monkeypatch.setenv("SECRETS_SERVICE_AUTH_TOKENS", token)

    import kubernetes.config as k8s_config

    monkeypatch.setattr(k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(k8s_config, "load_kube_config", lambda: None)


class _DummyManager:
    def __init__(self) -> None:
        self._status: Dict[str, str] = {
            "secret_name": "kraken-keys-admin",
            "last_rotated": datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        }

    def upsert_secret(self, account_id: str, payload: Dict[str, str], actor: str) -> Dict[str, str]:
        return {
            "secret_name": f"kraken-keys-{account_id}",
            "last_rotated": datetime.now(timezone.utc).isoformat(),
        }

    def get_status(self, account_id: str) -> Dict[str, str]:
        return dict(self._status)

    def get_decrypted_credentials(self, account_id: str) -> Dict[str, str]:
        return {"api_key": "key", "api_secret": "secret"}


@pytest.fixture()
def secrets_client(monkeypatch: pytest.MonkeyPatch) -> Tuple[TestClient, str]:
    token = "unit-test-token"
    _configure_environment(monkeypatch, token)

    sys.modules.pop("secrets_service", None)
    secrets_service = importlib.import_module("secrets_service")

    secrets_service.app.dependency_overrides[secrets_service.get_secret_manager] = lambda: _DummyManager()

    async def _fake_balance(**_: Dict[str, str]) -> Dict[str, Dict[str, str]]:
        return {"error": [], "result": {}}

    monkeypatch.setattr(secrets_service, "kraken_get_balance", _fake_balance)

    client = TestClient(secrets_service.app)
    try:
        yield client, token
    finally:
        client.close()
        secrets_service.app.dependency_overrides.clear()
        sys.modules.pop("secrets_service", None)


def test_missing_authorization_is_rejected(secrets_client: Tuple[TestClient, str]) -> None:
    client, _ = secrets_client

    response = client.get("/secrets/kraken/status", params={"account_id": "admin"})

    assert response.status_code == 401
    assert response.json()["detail"] == "Authorization header is required"


def test_invalid_token_is_rejected(secrets_client: Tuple[TestClient, str]) -> None:
    client, _ = secrets_client

    response = client.post(
        "/secrets/kraken",
        json={"account_id": "admin", "api_key": "key", "api_secret": "secret"},
        headers={"Authorization": "Bearer wrong-token", "X-Forwarded-Proto": "https"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Caller is not authorized"


def test_authorized_request_succeeds(secrets_client: Tuple[TestClient, str]) -> None:
    client, token = secrets_client

    response = client.get(
        "/secrets/kraken/status",
        params={"account_id": "admin"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["secret_name"] == "kraken-keys-admin"
