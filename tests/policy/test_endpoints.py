from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.policy.main import app

ADMIN_ACCOUNTS = ["admin-alpha", "admin-beta", "admin-gamma"]


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.mark.parametrize("account_id", ADMIN_ACCOUNTS)
def test_decide_policy_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    payload = {
        "account_id": account_id,
        "isolation_segment": "seg-1",
        "instrument": "BTC-USD",
        "quantity": 1.5,
        "price": 25000,
        "fee_tier": "standard",
        "include_fees": True,
    }

    response = client.post("/policy/decide", json=payload, headers={"X-Account-Id": account_id})

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id
    assert body["isolation_segment"] == "seg-1"
    assert body["fee_tier"] == "standard"
    assert body["total_fees"] > 0


def test_decide_policy_rejects_non_admin(client: TestClient) -> None:
    payload = {
        "account_id": "admin-alpha",
        "isolation_segment": "seg-1",
        "instrument": "BTC-USD",
        "quantity": 1.5,
        "price": 25000,
        "fee_tier": "standard",
        "include_fees": True,
    }

    response = client.post("/policy/decide", json=payload, headers={"X-Account-Id": "trader-1"})

    assert response.status_code == 403


def test_decide_policy_mismatched_account(client: TestClient) -> None:
    payload = {
        "account_id": "admin-beta",
        "isolation_segment": "seg-1",
        "instrument": "BTC-USD",
        "quantity": 1.5,
        "price": 25000,
        "fee_tier": "standard",
        "include_fees": True,
    }

    response = client.post("/policy/decide", json=payload, headers={"X-Account-Id": "admin-alpha"})

    assert response.status_code == 400
    assert response.json()["detail"] == "Account isolation attributes do not match header context"
