from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS
from services.risk.main import app

client = TestClient(app)


def test_risk_validate_authorized_accounts():
    payload = {
        "account_id": "company",
        "instrument": "ETH-USD",
        "net_exposure": 1000.0,
        "gross_notional": 10.0,
        "projected_loss": 5_000.0,
        "projected_fee": 100.0,
        "var_95": 25_000.0,
        "spread_bps": 10.0,
        "latency_ms": 100.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }
    for account in ADMIN_ACCOUNTS:
        payload["account_id"] = account
        payload["instrument"] = "ETH-USD"
        response = client.post("/risk/validate", json=payload, headers={"X-Account-ID": account})
        assert response.status_code == 200
        data = response.json()
        assert set(data.keys()) == {"valid", "reasons", "fee"}
        assert data["fee"] == payload["fee"]


def test_risk_validate_rejects_non_admin_account():
    payload = {
        "account_id": "shadow",
        "instrument": "ETH-USD",
        "net_exposure": 1000.0,
        "gross_notional": 10.0,
        "projected_loss": 5_000.0,
        "projected_fee": 100.0,
        "var_95": 25_000.0,
        "spread_bps": 10.0,
        "latency_ms": 100.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }
    response = client.post("/risk/validate", json=payload, headers={"X-Account-ID": "shadow"})
    assert response.status_code == 403
