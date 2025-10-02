from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from typing import Iterator, Tuple

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS


AccountClient = Tuple[TestClient, object]


@pytest.fixture()
def risk_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[AccountClient]:
    if importlib.util.find_spec("sqlalchemy") is None:
        pytest.skip("sqlalchemy is required for risk service tests")
    db_path = tmp_path / "risk.db"
    monkeypatch.setenv("RISK_DATABASE_URL", f"sqlite:///{db_path}")
    sys.modules.pop("risk_service", None)
    module = importlib.import_module("risk_service")

    with TestClient(module.app) as client:
        yield client, module
    module.ENGINE.dispose()
    sys.modules.pop("risk_service", None)


def _request_payload(account_id: str, instrument: str) -> dict[str, object]:
    return {
        "account_id": account_id,
        "intent": {
            "policy_id": "policy-123",
            "instrument_id": instrument,
            "side": "buy",
            "quantity": 1.0,
            "price": 1_000.0,
        },
        "portfolio_state": {
            "net_asset_value": 2_000_000.0,
            "notional_exposure": 100_000.0,
            "realized_daily_loss": 5_000.0,
            "fees_paid": 1_000.0,
            "var_95": 50_000.0,
            "var_99": 80_000.0,
        },
    }


@pytest.mark.parametrize(
    "account_id,instrument",
    (
        ("company", "BTC-USD"),
        ("director-1", "SOL-USD"),
        ("director-2", "ETH-USD"),
    ),
)
def test_validate_risk_all_admin_accounts(
    risk_app: AccountClient, account_id: str, instrument: str
) -> None:
    client, _ = risk_app
    response = client.post(
        "/risk/validate",
        json=_request_payload(account_id, instrument),
        headers={"X-Account-ID": account_id},
    )

    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) == {"pass", "reasons", "adjusted_qty", "cooldown"}
    assert body["pass"] is True
    assert body["reasons"] == []


def test_get_risk_limits_returns_whitelists(risk_app: AccountClient) -> None:
    client, _ = risk_app
    expected = {
        "company": ["BTC-USD", "ETH-USD"],
        "director-1": ["SOL-USD"],
        "director-2": ["BTC-USD", "ETH-USD"],
    }

    for account in sorted(ADMIN_ACCOUNTS):
        response = client.get("/risk/limits", params={"account_id": account})
        assert response.status_code == 200
        body = response.json()
        assert body["account_id"] == account
        assert body["limits"]["instrument_whitelist"] == expected[account]
        assert body["usage"] == {
            "account_id": account,
            "realized_daily_loss": 0.0,
            "fees_paid": 0.0,
            "net_asset_value": 0.0,
            "var_95": None,
            "var_99": None,
        }


def test_missing_account_returns_404(risk_app: AccountClient) -> None:
    client, _ = risk_app
    missing_payload = _request_payload("shadow", "BTC-USD")
    response = client.post(
        "/risk/validate",
        json=missing_payload,
        headers={"X-Account-ID": "shadow"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "No risk limits configured for account 'shadow'."

    limits_response = client.get("/risk/limits", params={"account_id": "shadow"})
    assert limits_response.status_code == 404


def test_validate_risk_rejects_mismatched_header(risk_app: AccountClient) -> None:
    client, _ = risk_app
    payload = _request_payload("company", "BTC-USD")

    response = client.post(
        "/risk/validate",
        json=payload,
        headers={"X-Account-ID": "director-1"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Account mismatch between header and payload."
