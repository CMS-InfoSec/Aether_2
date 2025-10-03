from __future__ import annotations

import os

from fastapi.testclient import TestClient

os.environ.setdefault("CONFIG_ALLOW_SQLITE_FOR_TESTS", "1")
os.environ.setdefault("CONFIG_DATABASE_URL", "sqlite+pysqlite:///:memory:")

from config_service import app, reset_state, set_guarded_keys


def setup_function() -> None:
    reset_state()
    set_guarded_keys(set())


def test_update_and_history_for_account() -> None:
    client = TestClient(app)

    response = client.post(
        "/config/update",
        params={"account_id": "acct-1"},
        json={"key": "features.toggle_x", "value": True, "author": "alice"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "applied"
    assert body["version"] == 1
    assert body["approvers"] == ["alice"]
    assert body["required_approvals"] == 1

    current = client.get("/config/current", params={"account_id": "acct-1"})
    assert current.status_code == 200
    current_body = current.json()
    assert set(current_body.keys()) == {"features.toggle_x"}
    current_entry = current_body["features.toggle_x"]
    assert current_entry["account_id"] == "acct-1"
    assert current_entry["key"] == "features.toggle_x"
    assert current_entry["value"] is True
    assert current_entry["version"] == 1
    assert current_entry["approvers"] == ["alice"]
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
    assert history_body[0]["approvers"] == ["alice"]

    second_response = client.post(
        "/config/update",
        params={"account_id": "acct-1"},
        json={"key": "features.toggle_x", "value": False, "author": "bob"},
    )
    assert second_response.status_code == 200
    second_body = second_response.json()
    assert second_body["version"] == 2
    assert second_body["approvers"] == ["bob"]

    history_body = client.get(
        "/config/history",
        params={"account_id": "acct-1", "key": "features.toggle_x"},
    ).json()
    assert [entry["version"] for entry in history_body] == [1, 2]


def test_guarded_key_requires_two_authors() -> None:
    set_guarded_keys({"risk.max_notional"})
    client = TestClient(app)

    first = client.post(
        "/config/update",
        params={"account_id": "acct-77"},
        json={"key": "risk.max_notional", "value": {"notional": 1000}, "author": "alice"},
    )
    assert first.status_code == 202
    first_body = first.json()
    assert first_body["status"] == "pending"
    assert first_body["approvers"] == ["alice"]
    assert first_body["version"] is None
    assert first_body["required_approvals"] == 2

    current = client.get("/config/current", params={"account_id": "acct-77"})
    assert current.status_code == 200
    assert current.json() == {}

    duplicate_author = client.post(
        "/config/update",
        params={"account_id": "acct-77"},
        json={"key": "risk.max_notional", "value": {"notional": 1000}, "author": "alice"},
    )
    assert duplicate_author.status_code == 400
    assert duplicate_author.json()["detail"] == "second_author_required"

    mismatched_value = client.post(
        "/config/update",
        params={"account_id": "acct-77"},
        json={"key": "risk.max_notional", "value": {"notional": 2000}, "author": "bob"},
    )
    assert mismatched_value.status_code == 400
    assert mismatched_value.json()["detail"] == "value_mismatch"

    second = client.post(
        "/config/update",
        params={"account_id": "acct-77"},
        json={"key": "risk.max_notional", "value": {"notional": 1000}, "author": "bob"},
    )
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["status"] == "applied"
    assert second_body["version"] == 1
    assert second_body["approvers"] == ["alice", "bob"]

    final_current = client.get("/config/current", params={"account_id": "acct-77"}).json()
    assert final_current["risk.max_notional"]["value"] == {"notional": 1000}
    assert final_current["risk.max_notional"]["approvers"] == ["alice", "bob"]

    history = client.get(
        "/config/history",
        params={"account_id": "acct-77", "key": "risk.max_notional"},
    )
    assert history.status_code == 200
    history_body = history.json()
    assert len(history_body) == 1
    assert history_body[0]["approvers"] == ["alice", "bob"]

