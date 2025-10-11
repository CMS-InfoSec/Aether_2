"""Unit tests for the secrets management service."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Dict

import pytest
from fastapi.testclient import TestClient

from services.secrets import secrets_service


class FakeCoreV1Api:
    def __init__(self) -> None:
        self.patched: Dict[str, Dict[str, object]] = {}
        self.created: Dict[str, Dict[str, object]] = {}
        self._secret = SimpleNamespace(
            metadata=SimpleNamespace(
                annotations={
                    secrets_service.ANNOTATION_CREATED_AT: datetime(2023, 1, 1, tzinfo=timezone.utc).isoformat(),
                    secrets_service.ANNOTATION_ROTATED_AT: datetime(2023, 1, 2, tzinfo=timezone.utc).isoformat(),
                },
                name="kraken-keys-company",
            )
        )

    def read_namespaced_secret(self, name: str, namespace: str):
        return self._secret

    def patch_namespaced_secret(self, name: str, namespace: str, body: Dict[str, object]):
        self.patched[f"{namespace}/{name}"] = body

    def create_namespaced_secret(self, namespace: str, body: Dict[str, object]):
        self.created[f"{namespace}/{body['metadata']['name']}"] = body


@pytest.fixture
def secrets_client(monkeypatch: pytest.MonkeyPatch, fake_auditor) -> TestClient:
    fake_api = FakeCoreV1Api()

    secrets_service.app.dependency_overrides[secrets_service.ensure_secure_transport] = lambda: None
    secrets_service.app.dependency_overrides[secrets_service.require_admin_account] = lambda: "company"
    secrets_service.app.dependency_overrides[secrets_service.require_mfa_context] = lambda: "verified"
    secrets_service.app.dependency_overrides[
        secrets_service.require_dual_director_confirmation
    ] = lambda: ("director-a", "director-b")
    secrets_service.app.dependency_overrides[secrets_service.get_core_v1_api] = lambda: fake_api
    monkeypatch.setattr(secrets_service, "validate_kraken_credentials", lambda *_, **__: True)
    monkeypatch.setattr(secrets_service, "_auditor", fake_auditor)

    client = TestClient(secrets_service.app)
    client._fake_api = fake_api  # type: ignore[attr-defined]
    return client


def teardown_module(module) -> None:  # type: ignore[override]
    secrets_service.app.dependency_overrides.clear()


def test_rotate_secret_requires_tls_guard() -> None:
    client = TestClient(secrets_service.app)
    payload = {
        "account_id": "company",
        "api_key": "abcdef",
        "api_secret": "abcdefghijkl",
    }
    response = client.post("/secrets/kraken", json=payload)
    assert response.status_code == 400


def test_rotate_secret_enforces_account_match(secrets_client: TestClient) -> None:
    payload = {
        "account_id": "other",
        "api_key": "abcdef",
        "api_secret": "abcdefghijkl",
    }
    response = secrets_client.post("/secrets/kraken", json=payload, headers={"X-Account-ID": "company", "X-MFA-Token": "verified"})
    assert response.status_code == 403


def test_rotate_secret_updates_kubernetes(secrets_client: TestClient, fake_auditor) -> None:
    payload = {
        "account_id": "company",
        "api_key": "abcdef",
        "api_secret": "abcdefghijkl",
    }
    response = secrets_client.post(
        "/secrets/kraken",
        json=payload,
        headers={"X-Account-ID": "company", "X-MFA-Token": "verified"},
    )
    assert response.status_code == 204

    fake_api: FakeCoreV1Api = secrets_client._fake_api  # type: ignore[attr-defined]
    assert fake_api.patched
    assert fake_auditor.events


def test_secret_status_returns_metadata(secrets_client: TestClient) -> None:
    response = secrets_client.get(
        "/secrets/kraken/status",
        params={"account_id": "company"},
        headers={"X-Account-ID": "company", "X-MFA-Token": "verified"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["secret_name"] == "kraken-keys-company"
