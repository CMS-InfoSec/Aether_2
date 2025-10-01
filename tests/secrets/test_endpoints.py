from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from services.secrets.main import app

ADMIN_ACCOUNTS = ["admin-alpha", "admin-beta", "admin-gamma"]


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.fixture(name="secret_factory")
def secret_factory_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def factory(account_id: str) -> Path:
        secret_path = tmp_path / f"{account_id}.json"
        secret_path.write_text(json.dumps({"key": f"key-{account_id}", "secret": f"sec-{account_id}"}))
        monkeypatch.setenv(f"AETHER_{account_id.upper()}_KRAKEN_SECRET_PATH", str(secret_path))
        return secret_path

    return factory


@pytest.mark.parametrize("account_id", ADMIN_ACCOUNTS)
def test_get_kraken_secret_allows_admin_accounts(
    client: TestClient, secret_factory, account_id: str
) -> None:
    secret_factory(account_id)
    payload = {
        "account_id": account_id,
        "isolation_segment": "seg-sec",
        "scope": "trading",
    }

    response = client.post("/secrets/kraken", json=payload, headers={"X-Account-Id": account_id})

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id
    assert body["key"] == f"key-{account_id}"
    assert body["secret"] == f"sec-{account_id}"


def test_get_kraken_secret_rejects_non_admin(client: TestClient, secret_factory) -> None:
    secret_factory("admin-alpha")
    payload = {
        "account_id": "admin-alpha",
        "isolation_segment": "seg-sec",
        "scope": "trading",
    }

    response = client.post("/secrets/kraken", json=payload, headers={"X-Account-Id": "guest"})

    assert response.status_code == 403


def test_get_kraken_secret_mismatched_account(client: TestClient, secret_factory) -> None:
    secret_factory("admin-alpha")
    payload = {
        "account_id": "admin-alpha",
        "isolation_segment": "seg-sec",
        "scope": "trading",
    }

    response = client.post("/secrets/kraken", json=payload, headers={"X-Account-Id": "admin-beta"})

    assert response.status_code == 400
    assert response.json()["detail"] == "Account isolation attributes do not match header context"


def test_get_kraken_secret_missing_file(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    account_id = "admin-alpha"
    monkeypatch.setenv("AETHER_ADMIN_ALPHA_KRAKEN_SECRET_PATH", "/tmp/missing.json")
    payload = {
        "account_id": account_id,
        "isolation_segment": "seg-sec",
        "scope": "trading",
    }

    response = client.post("/secrets/kraken", json=payload, headers={"X-Account-Id": account_id})

    assert response.status_code == 404
