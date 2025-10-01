from __future__ import annotations

from copy import deepcopy

import pytest
from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS
from services.risk.main import app


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.fixture(name="base_payload")
def base_payload_fixture() -> dict[str, object]:
    return {
        "account_id": "admin-eu",
        "instrument": "ETH-USD",
        "net_exposure": 100_000.0,
        "gross_notional": 25_000.0,
        "projected_loss": 10_000.0,
        "projected_fee": 500.0,
        "var_95": 50_000.0,
        "spread_bps": 12.5,
        "latency_ms": 120.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }


@pytest.mark.parametrize("account_id", sorted(ADMIN_ACCOUNTS))
def test_validate_risk_authorized_accounts(client: TestClient, base_payload: dict[str, object], account_id: str) -> None:
    payload = deepcopy(base_payload)
    payload["account_id"] = account_id
    if account_id == "admin-us":
        payload["instrument"] = "SOL-USD"

    response = client.post("/risk/validate", json=payload, headers={"X-Account-ID": account_id})

    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) == {"valid", "reasons", "fee"}
    assert isinstance(body["valid"], bool)
    assert isinstance(body["reasons"], list)


def test_validate_risk_rejects_non_admin(client: TestClient, base_payload: dict[str, object]) -> None:
    payload = deepcopy(base_payload)
    response = client.post("/risk/validate", json=payload, headers={"X-Account-ID": "ops-user"})

    assert response.status_code == 403


def test_validate_risk_mismatched_account(client: TestClient, base_payload: dict[str, object]) -> None:
    payload = deepcopy(base_payload)
    payload["account_id"] = "admin-us"
    response = client.post("/risk/validate", json=payload, headers={"X-Account-ID": "admin-eu"})

    assert response.status_code == 403
    assert response.json()["detail"] == "Account mismatch between header and payload."
