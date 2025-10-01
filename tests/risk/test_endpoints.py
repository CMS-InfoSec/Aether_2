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
    fee = {"currency": "USD", "maker": 0.1, "taker": 0.2}
    return {
        "account_id": "admin-eu",
        "intent": {
            "policy_decision": {
                "request": {
                    "account_id": "admin-eu",
                    "order_id": "order-001",
                    "instrument": "ETH-USD",
                    "side": "BUY",
                    "quantity": 10.0,
                    "price": 2_500.0,
                    "fee": fee,
                },
                "response": {
                    "approved": True,
                    "reason": None,
                    "effective_fee": fee,
                    "expected_edge_bps": 20.0,
                    "fee_adjusted_edge_bps": 19.5,
                    "selected_action": "maker",
                    "action_templates": [
                        {
                            "name": "maker",
                            "venue_type": "maker",
                            "edge_bps": 20.0,
                            "fee_bps": 5.0,
                            "confidence": 0.9,
                        }
                    ],
                    "confidence": {
                        "model_confidence": 0.9,
                        "state_confidence": 0.85,
                        "execution_confidence": 0.88,
                    },
                    "features": [0.1, 0.2],
                    "book_snapshot": {
                        "mid_price": 2_500.0,
                        "spread_bps": 12.5,
                        "imbalance": 0.05,
                    },
                    "state": {
                        "regime": "normal",
                        "volatility": 0.45,
                        "liquidity_score": 0.75,
                        "conviction": 0.6,
                    },
                    "take_profit_bps": 25.0,
                    "stop_loss_bps": 15.0,
                },
            },
            "metrics": {
                "net_exposure": 100_000.0,
                "gross_notional": 25_000.0,
                "projected_loss": 10_000.0,
                "projected_fee": 500.0,
                "var_95": 50_000.0,
                "spread_bps": 12.5,
                "latency_ms": 120.0,
            },
        },
        "portfolio_state": {
            "nav": 1_000_000.0,
            "loss_to_date": 5_000.0,
            "fee_to_date": 500.0,
            "instrument_exposure": {"ETH-USD": 75_000.0},
        },
    }


@pytest.mark.parametrize("account_id", sorted(ADMIN_ACCOUNTS))
def test_validate_risk_authorized_accounts(client: TestClient, base_payload: dict[str, object], account_id: str) -> None:
    payload = deepcopy(base_payload)
    payload["account_id"] = account_id
    payload["intent"]["policy_decision"]["request"]["account_id"] = account_id
    if account_id == "admin-us":
        payload["intent"]["policy_decision"]["request"]["instrument"] = "SOL-USD"
        payload["portfolio_state"]["instrument_exposure"] = {"SOL-USD": 50_000.0}

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
    payload["intent"]["policy_decision"]["request"]["account_id"] = "admin-us"
    response = client.post("/risk/validate", json=payload, headers={"X-Account-ID": "admin-eu"})

    assert response.status_code == 403
    assert response.json()["detail"] == "Account mismatch between header and payload."
