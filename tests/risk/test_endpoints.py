from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.risk.main import app

ADMIN_ACCOUNTS = ["admin-alpha", "admin-beta", "admin-gamma"]


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.mark.parametrize("account_id", ADMIN_ACCOUNTS)
def test_validate_risk_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    payload = {
        "account_id": account_id,
        "isolation_segment": "seg-risk",
        "net_exposure": 500_000,
        "symbol": "ETH-USD",
        "fee_tier": "standard",
        "include_fees": True,
    }

    response = client.post("/risk/validate", json=payload, headers={"X-Account-Id": account_id})

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id
    assert body["valid"] is True
    assert body["fee_tier"] == "standard"


def test_validate_risk_rejects_non_admin(client: TestClient) -> None:
    payload = {
        "account_id": "admin-alpha",
        "isolation_segment": "seg-risk",
        "net_exposure": 500_000,
        "symbol": "ETH-USD",
        "fee_tier": "standard",
        "include_fees": True,
    }

    response = client.post("/risk/validate", json=payload, headers={"X-Account-Id": "ops-user"})

    assert response.status_code == 403


def test_validate_risk_mismatched_account(client: TestClient) -> None:
    payload = {
        "account_id": "admin-beta",
        "isolation_segment": "seg-risk",
        "net_exposure": 500_000,
        "symbol": "ETH-USD",
        "fee_tier": "standard",
        "include_fees": True,
    }

    response = client.post("/risk/validate", json=payload, headers={"X-Account-Id": "admin-gamma"})

    assert response.status_code == 400
    assert response.json()["detail"] == "Account isolation attributes do not match header context"
