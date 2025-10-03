"""Risk service unit tests covering schema validation and fee awareness."""

from __future__ import annotations

from typing import Dict

import sys
from pathlib import Path
from typing import Dict, Iterator

import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("fastapi")
pytest.importorskip("services.common.security")
from fastapi import status
from fastapi.testclient import TestClient

from risk_service import RiskEvaluationContext, app as risk_app, require_admin_account
from tests.helpers.authentication import override_admin_auth

config = dict(getattr(RiskEvaluationContext, "model_config", {}))
config["arbitrary_types_allowed"] = True
config["from_attributes"] = True
RiskEvaluationContext.model_config = config  # type: ignore[attr-defined]


@pytest.fixture
def risk_client() -> Iterator[TestClient]:
    with TestClient(risk_app) as client:
        yield client


def _base_request() -> Dict[str, object]:
    return {
        "account_id": "ACC-DEFAULT",
        "intent": {
            "policy_id": "policy-1",
            "instrument_id": "AAPL",
            "side": "buy",
            "quantity": 10.0,
            "price": 100.0,
        },
        "portfolio_state": {
            "net_asset_value": 1_000_000.0,
            "notional_exposure": 100_000.0,
            "realized_daily_loss": 1_000.0,
            "fees_paid": 1_000.0,
        },
    }


def test_risk_validation_passes_under_fee_budget(risk_client: TestClient) -> None:
    payload = _base_request()
    with override_admin_auth(
        risk_client.app, require_admin_account, payload["account_id"]
    ) as headers:
        response = risk_client.post(
            "/risk/validate",
            json=payload,
            headers={**headers, "X-Account-ID": payload["account_id"]},
        )
    assert response.status_code == 200
    body = response.json()
    assert body["pass"] is True
    assert body["reasons"] == []


def test_risk_validation_rejects_when_fee_budget_exhausted(
    risk_client: TestClient,
) -> None:
    payload = _base_request()
    payload["portfolio_state"]["fees_paid"] = 12_000.0
    with override_admin_auth(
        risk_client.app, require_admin_account, payload["account_id"]
    ) as headers:
        response = risk_client.post(
            "/risk/validate",
            json=payload,
            headers={**headers, "X-Account-ID": payload["account_id"]},
        )
    assert response.status_code == 200
    body = response.json()
    assert body["pass"] is False
    assert any("Fee budget exhausted" in reason for reason in body["reasons"])


def test_risk_validation_enforces_schema(risk_client: TestClient) -> None:
    payload = _base_request()
    payload["intent"]["side"] = "hold"
    with override_admin_auth(
        risk_client.app, require_admin_account, payload["account_id"]
    ) as headers:
        response = risk_client.post(
            "/risk/validate",
            json=payload,
            headers={**headers, "X-Account-ID": payload["account_id"]},
        )
    assert response.status_code == 422


def test_risk_limits_endpoint_returns_configuration(risk_client: TestClient) -> None:
    with override_admin_auth(
        risk_client.app, require_admin_account, "ACC-DEFAULT"
    ) as headers:
        response = risk_client.get(
            "/risk/limits",
            headers=headers,
        )
    assert response.status_code == 200
    body = response.json()
    assert body["limits"]["account_id"] == "ACC-DEFAULT"
    assert body["usage"]["account_id"] == "ACC-DEFAULT"


def test_risk_validation_rejects_unauthenticated_request(
    risk_client: TestClient,
) -> None:
    payload = _base_request()
    response = risk_client.post("/risk/validate", json=payload)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_risk_validation_rejects_account_mismatch(risk_client: TestClient) -> None:
    payload = _base_request()
    with override_admin_auth(
        risk_client.app, require_admin_account, payload["account_id"]
    ) as headers:
        payload["account_id"] = "ACC-OTHER"
        response = risk_client.post(
            "/risk/validate",
            json=payload,
            headers=headers,
        )
    assert response.status_code == status.HTTP_403_FORBIDDEN
    assert (
        response.json()["detail"]
        == "Account mismatch between authenticated session and payload."
    )
