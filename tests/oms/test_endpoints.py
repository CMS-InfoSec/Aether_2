from __future__ import annotations

from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from services.common import security
from services.oms.main import app
from shared.k8s import KrakenSecretStore

ADMIN_ACCOUNTS = ["admin-alpha", "admin-beta", "admin-gamma"]


@pytest.fixture(name="client")
def client_fixture(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setattr(security, "ADMIN_ACCOUNTS", set(ADMIN_ACCOUNTS))
    KrakenSecretStore.reset()

    store = KrakenSecretStore()
    for account in ADMIN_ACCOUNTS:
        store.write_credentials(
            account,
            api_key=f"test-key-{account}",
            api_secret=f"test-secret-{account}",
        )

    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()
        KrakenSecretStore.reset()


@pytest.mark.parametrize("account_id", ADMIN_ACCOUNTS)
def test_place_order_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    payload = {
        "account_id": account_id,
        "order_id": "ord-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": account_id})

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "accepted": True,
        "routed_venue": "kraken",
        "fee": payload["fee"],
    }


def test_place_order_rejects_non_admin(client: TestClient) -> None:
    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "trade"})

    assert response.status_code == 403


def test_place_order_mismatched_account(client: TestClient) -> None:
    payload = {
        "account_id": "admin-beta",
        "order_id": "ord-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 403
    assert response.json()["detail"] == "Account mismatch between header and payload."


def test_place_order_validates_side(client: TestClient) -> None:
    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-123",
        "side": "hold",
        "instrument": "BTC-USD",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 422
