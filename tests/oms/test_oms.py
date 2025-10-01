from __future__ import annotations

from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS
from services.oms.main import app
from shared.k8s import KrakenSecretStore


@pytest.fixture(name="client")
def client_fixture() -> Iterator[TestClient]:
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


def test_oms_place_authorized_accounts(client: TestClient) -> None:
    payload = {
        "account_id": "admin-eu",
        "order_id": "1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 10.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }
    for account in ADMIN_ACCOUNTS:
        payload["account_id"] = account
        response = client.post("/oms/place", json=payload, headers={"X-Account-ID": account})
        assert response.status_code == 200
        data = response.json()
        assert set(data.keys()) == {"accepted", "routed_venue", "fee"}
        assert data["fee"] == payload["fee"]


def test_oms_place_rejects_non_admin_account(client: TestClient) -> None:
    payload = {
        "account_id": "shadow",
        "order_id": "1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 10.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }
    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "shadow"})
    assert response.status_code == 403
