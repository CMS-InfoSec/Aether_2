from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Iterator

import sys

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def _import_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ModuleNotFoundError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


try:
    from auth.service import InMemorySessionStore
except ModuleNotFoundError:
    auth_module = _import_module("tests.config_service_auth", ROOT / "auth" / "service.py")
    InMemorySessionStore = auth_module.InMemorySessionStore  # type: ignore[attr-defined]

from config_service import app, reset_state, set_guarded_keys

try:
    from services.common.security import set_default_session_store
except ModuleNotFoundError:
    security_module = _import_module(
        "tests.config_service_security", ROOT / "services" / "common" / "security.py"
    )
    set_default_session_store = security_module.set_default_session_store  # type: ignore[attr-defined]


def setup_function() -> None:
    reset_state()
    set_guarded_keys(set())


@pytest.fixture
def config_client() -> Iterator[tuple[TestClient, InMemorySessionStore]]:
    store = InMemorySessionStore()
    set_default_session_store(store)
    app.state.session_store = store

    try:
        with TestClient(app) as client:
            yield client, store
    finally:
        set_default_session_store(None)
        if hasattr(app.state, "session_store"):
            delattr(app.state, "session_store")


def _auth_headers(store: InMemorySessionStore, account: str) -> dict[str, str]:
    session = store.create(account)
    return {"Authorization": f"Bearer {session.token}"}


def test_requires_authenticated_session(
    config_client: tuple[TestClient, InMemorySessionStore]
) -> None:
    client, _store = config_client

    current = client.get("/config/current")
    assert current.status_code == 401

    response = client.post(
        "/config/update",
        params={"account_id": "acct-unauth"},
        json={"key": "features.toggle_x", "value": True, "author": "ignored"},
    )
    assert response.status_code == 401


def test_rejects_non_admin_session(
    config_client: tuple[TestClient, InMemorySessionStore]
) -> None:
    client, store = config_client
    headers = _auth_headers(store, "guest")

    response = client.post(
        "/config/update",
        params={"account_id": "acct-unauth"},
        json={"key": "features.toggle_x", "value": True, "author": "ignored"},
        headers=headers,
    )
    assert response.status_code == 403


def test_update_and_history_for_account(
    config_client: tuple[TestClient, InMemorySessionStore]
) -> None:
    client, store = config_client
    headers = _auth_headers(store, "company")

    response = client.post(
        "/config/update",
        params={"account_id": "acct-1"},
        json={"key": "features.toggle_x", "value": True, "author": "alice"},
        headers=headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "applied"
    assert body["version"] == 1
    assert body["approvers"] == ["company"]
    assert body["required_approvals"] == 1

    current = client.get(
        "/config/current",
        params={"account_id": "acct-1"},
        headers=headers,
    )
    assert current.status_code == 200
    current_body = current.json()
    assert set(current_body.keys()) == {"features.toggle_x"}
    current_entry = current_body["features.toggle_x"]
    assert current_entry["account_id"] == "acct-1"
    assert current_entry["key"] == "features.toggle_x"
    assert current_entry["value"] is True
    assert current_entry["version"] == 1
    assert current_entry["approvers"] == ["company"]
    assert "id" in current_entry
    assert "ts" in current_entry

    history = client.get(
        "/config/history",
        params={"account_id": "acct-1", "key": "features.toggle_x"},
    )
    assert history.status_code == 200
    history_body = history.json()
    assert len(history_body) == 1
    assert history_body[0]["version"] == 1
    assert history_body[0]["approvers"] == ["company"]

    second_response = client.post(
        "/config/update",
        params={"account_id": "acct-1"},
        json={"key": "features.toggle_x", "value": False, "author": "bob"},
        headers=headers,
    )
    assert second_response.status_code == 200
    second_body = second_response.json()
    assert second_body["version"] == 2
    assert second_body["approvers"] == ["company"]

    history_body = client.get(
        "/config/history",
        params={"account_id": "acct-1", "key": "features.toggle_x"},
    ).json()
    assert [entry["version"] for entry in history_body] == [1, 2]


def test_guarded_key_requires_two_authors(
    config_client: tuple[TestClient, InMemorySessionStore]
) -> None:
    set_guarded_keys({"risk.max_notional"})
    client, store = config_client
    requester_headers = _auth_headers(store, "company")
    second_headers = _auth_headers(store, "director-1")

    first = client.post(
        "/config/update",
        params={"account_id": "acct-77"},
        json={"key": "risk.max_notional", "value": {"notional": 1000}, "author": "alice"},
        headers=requester_headers,
    )
    assert first.status_code == 202
    first_body = first.json()
    assert first_body["status"] == "pending"
    assert first_body["approvers"] == ["company"]
    assert first_body["version"] is None
    assert first_body["required_approvals"] == 2

    current = client.get(
        "/config/current",
        params={"account_id": "acct-77"},
        headers=requester_headers,
    )
    assert current.status_code == 200
    assert current.json() == {}

    duplicate_author = client.post(
        "/config/update",
        params={"account_id": "acct-77"},
        json={"key": "risk.max_notional", "value": {"notional": 1000}, "author": "alice"},
        headers=requester_headers,
    )
    assert duplicate_author.status_code == 400
    assert duplicate_author.json()["detail"] == "second_author_required"

    mismatched_value = client.post(
        "/config/update",
        params={"account_id": "acct-77"},
        json={"key": "risk.max_notional", "value": {"notional": 2000}, "author": "bob"},
        headers=second_headers,
    )
    assert mismatched_value.status_code == 400
    assert mismatched_value.json()["detail"] == "value_mismatch"

    second = client.post(
        "/config/update",
        params={"account_id": "acct-77"},
        json={"key": "risk.max_notional", "value": {"notional": 1000}, "author": "bob"},
        headers=second_headers,
    )
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["status"] == "applied"
    assert second_body["version"] == 1
    assert second_body["approvers"] == ["company", "director-1"]

    final_current = client.get(
        "/config/current",
        params={"account_id": "acct-77"},
        headers=second_headers,
    ).json()
    assert final_current["risk.max_notional"]["value"] == {"notional": 1000}
    assert final_current["risk.max_notional"]["approvers"] == ["company", "director-1"]

    history = client.get(
        "/config/history",
        params={"account_id": "acct-77", "key": "risk.max_notional"},
    )
    assert history.status_code == 200
    history_body = history.json()
    assert len(history_body) == 1
    assert history_body[0]["approvers"] == ["company", "director-1"]
