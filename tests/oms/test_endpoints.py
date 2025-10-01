from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.oms.main import app

ADMIN_ACCOUNTS = ["admin-alpha", "admin-beta", "admin-gamma"]


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.mark.parametrize("account_id", ADMIN_ACCOUNTS)
def test_place_order_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    payload = {
        "account_id": account_id,
        "isolation_segment": "seg-oms",
        "order_id": "ord-123",
        "side": "buy",
        "quantity": 10,
        "price": 101.5,
        "fee_tier": "standard",
        "include_fees": True,
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-Id": account_id})

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id
    assert body["status"] == "accepted"
    assert body["fee_tier"] == "standard"


def test_place_order_rejects_non_admin(client: TestClient) -> None:
    payload = {
        "account_id": "admin-alpha",
        "isolation_segment": "seg-oms",
        "order_id": "ord-123",
        "side": "buy",
        "quantity": 10,
        "price": 101.5,
        "fee_tier": "standard",
        "include_fees": True,
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-Id": "trade"})

    assert response.status_code == 403


def test_place_order_mismatched_account(client: TestClient) -> None:
    payload = {
        "account_id": "admin-beta",
        "isolation_segment": "seg-oms",
        "order_id": "ord-123",
        "side": "buy",
        "quantity": 10,
        "price": 101.5,
        "fee_tier": "standard",
        "include_fees": True,
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-Id": "admin-alpha"})

    assert response.status_code == 400
    assert response.json()["detail"] == "Account isolation attributes do not match header context"


def test_place_order_validates_side(client: TestClient) -> None:
    payload = {
        "account_id": "admin-alpha",
        "isolation_segment": "seg-oms",
        "order_id": "ord-123",
        "side": "hold",
        "quantity": 10,
        "price": 101.5,
        "fee_tier": "standard",
        "include_fees": True,
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-Id": "admin-alpha"})

    assert response.status_code == 422
