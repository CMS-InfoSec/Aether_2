from __future__ import annotations

import importlib.util
import sys
import sysconfig
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from services.policy.main import app

ADMIN_ACCOUNTS = ["company", "director-1", "director-2"]

_STDLIB_SECRETS = Path(sysconfig.get_paths()["stdlib"]) / "secrets.py"

if (
    "secrets" not in sys.modules
    or "tests/secrets" in str(getattr(sys.modules["secrets"], "__file__", ""))
):
    spec = importlib.util.spec_from_file_location("secrets", _STDLIB_SECRETS)
    if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        secrets_package_path = Path(__file__).resolve().parent.parent / "secrets"
        module.__path__ = [str(secrets_package_path)]
        if module.__spec__ is not None:
            module.__spec__.submodule_search_locations = [str(secrets_package_path)]
        sys.modules["secrets"] = module


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


def _decision_payload(account_id: str) -> dict:
    return {
        "account_id": account_id,
        "order_id": "endpoint-case",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 25_000,
        "fee": {"currency": "USD", "maker": 4.0, "taker": 8.0},
        "features": [18.0, 4.0, -2.0],
        "book_snapshot": {"mid_price": 30_000.0, "spread_bps": 3.0, "imbalance": 0.1},
        "state": {
            "regime": "neutral",
            "volatility": 0.3,
            "liquidity_score": 0.8,
            "conviction": 0.65,
        },
    }


@pytest.mark.parametrize("account_id", ADMIN_ACCOUNTS)
def test_decide_policy_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    payload = _decision_payload(account_id)

    response = client.post("/policy/decide", json=payload, headers={"X-Account-ID": account_id})

    assert response.status_code == 200
    body = response.json()
    assert body["approved"] in {True, False}
    assert body["effective_fee"] == payload["fee"]
    assert body["confidence"]["overall_confidence"] >= 0


def test_decide_policy_rejects_non_admin(client: TestClient) -> None:
    payload = _decision_payload("company")

    response = client.post("/policy/decide", json=payload, headers={"X-Account-ID": "shadow"})

    assert response.status_code == 403


def test_decide_policy_mismatched_account(client: TestClient) -> None:
    payload = _decision_payload("director-1")

    response = client.post("/policy/decide", json=payload, headers={"X-Account-ID": "company"})

    assert response.status_code == 403
    assert response.json()["detail"] == "Account mismatch between header and payload."
