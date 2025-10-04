import importlib
import os
import sys
from pathlib import Path

from decimal import Decimal

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("fastapi", reason="FastAPI is required for capital flow service tests")

from fastapi.testclient import TestClient

from auth.service import InMemorySessionStore


@pytest.fixture()
def capital_flow_client(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Provide a fresh capital flow service client backed by an isolated database."""

    monkeypatch.syspath_prepend(str(ROOT))
    monkeypatch.setenv(
        "PYTHONPATH",
        str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""),
    )
    monkeypatch.setenv("CAPITAL_FLOW_DATABASE_URL", f"sqlite:///{tmp_path}/capital_flows.db")

    previous_modules = {
        "services.common.security": sys.modules.get("services.common.security"),
        "services.common": sys.modules.get("services.common"),
        "services": sys.modules.get("services"),
        "capital_flow": sys.modules.get("capital_flow"),
    }

    for name in list(previous_modules):
        sys.modules.pop(name, None)
    module = importlib.import_module("capital_flow")

    security = importlib.import_module("services.common.security")

    client = TestClient(module.app)
    store = InMemorySessionStore()

    previous_store = getattr(security, "_DEFAULT_SESSION_STORE", None)
    security.set_default_session_store(store)

    try:
        yield client, module, store
    finally:
        security.set_default_session_store(previous_store)
        client.app.dependency_overrides.clear()
        for name, module in previous_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_deposit_requires_authentication(capital_flow_client) -> None:
    client, _, _ = capital_flow_client

    response = client.post(
        "/finance/deposit",
        json={"account_id": "company", "amount": "100.0", "currency": "USD"},
    )

    assert response.status_code == 401


def test_deposit_rejects_account_mismatch(capital_flow_client) -> None:
    client, _, store = capital_flow_client
    session = store.create("company")

    response = client.post(
        "/finance/deposit",
        headers=_auth_headers(session.token),
        json={"account_id": "Shadow", "amount": "50.0", "currency": "USD"},
    )

    assert response.status_code == 403


def test_list_flows_rejects_invalid_token(capital_flow_client) -> None:
    client, _, _ = capital_flow_client

    response = client.get(
        "/finance/flows",
        headers={"Authorization": "Bearer invalid"},
        params={"account_id": "company"},
    )

    assert response.status_code == 401


def test_deposit_and_list_flows_with_valid_session(capital_flow_client) -> None:
    client, _, store = capital_flow_client
    session = store.create("company")

    deposit_response = client.post(
        "/finance/deposit",
        headers=_auth_headers(session.token),
        json={"account_id": "COMPANY", "amount": "75.0", "currency": "USD"},
    )

    assert deposit_response.status_code == 201
    payload = deposit_response.json()
    assert payload["account_id"] == "COMPANY"
    assert Decimal(payload["nav_baseline"]) == Decimal("75.0")

    flows_response = client.get(
        "/finance/flows",
        headers=_auth_headers(session.token),
        params={"account_id": "company"},
    )

    assert flows_response.status_code == 200
    flows = flows_response.json()["flows"]
    assert len(flows) == 1
    assert flows[0]["account_id"] == "COMPANY"
    assert Decimal(flows[0]["nav_baseline"]) == Decimal("75.0")


def test_high_precision_deposit_and_withdrawal(capital_flow_client) -> None:
    client, _, store = capital_flow_client
    session = store.create("company")

    deposit_amount = "1000000.123456789123456789"
    withdrawal_amount = "0.123456789123456789"

    deposit_response = client.post(
        "/finance/deposit",
        headers=_auth_headers(session.token),
        json={"account_id": "company", "amount": deposit_amount, "currency": "USD"},
    )

    assert deposit_response.status_code == 201
    deposit_payload = deposit_response.json()
    assert deposit_payload["amount"] == deposit_amount
    assert Decimal(deposit_payload["nav_baseline"]) == Decimal(deposit_amount)

    withdrawal_response = client.post(
        "/finance/withdraw",
        headers=_auth_headers(session.token),
        json={"account_id": "company", "amount": withdrawal_amount, "currency": "USD"},
    )

    assert withdrawal_response.status_code == 201
    withdrawal_payload = withdrawal_response.json()
    assert withdrawal_payload["amount"] == withdrawal_amount
    assert Decimal(withdrawal_payload["nav_baseline"]) == Decimal("1000000.0")

    history = client.get(
        "/finance/flows",
        headers=_auth_headers(session.token),
        params={"account_id": "company"},
    )

    assert history.status_code == 200
    entries = history.json()["flows"]
    assert {entry["type"] for entry in entries} == {"deposit", "withdraw"}
    baseline_values = [Decimal(entry["nav_baseline"]) for entry in entries]
    assert Decimal("1000000.0") in baseline_values
