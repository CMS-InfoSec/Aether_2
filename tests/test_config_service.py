from __future__ import annotations

from fastapi.testclient import TestClient

from config_service import ConfigChangeStore, app


def setup_function() -> None:
    ConfigChangeStore.reset()


def test_non_guarded_change_applies_immediately() -> None:
    ConfigChangeStore.set_guarded_keys({"risk.limits"})
    client = TestClient(app)

    response = client.post(
        "/config/update",
        json={"key": "features.toggle_x", "new_value": True, "author": "alice"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "applied"
    assert body["requires_second_approval"] is False
    assert body["version"] == 1
    assert body["approvals"] == ["alice"]

    assert ConfigChangeStore.pending_changes() == ()
    versions = ConfigChangeStore.config_versions()
    assert len(versions) == 1
    latest = versions[0]
    assert latest.config_key == "features.toggle_x"
    assert latest.value is True
    assert latest.approvals == ("alice",)

    audit_actions = [entry.action for entry in ConfigChangeStore.audit_entries()]
    assert audit_actions == ["config_update_requested", "config_update_applied"]


def test_guarded_change_requires_two_distinct_authors() -> None:
    ConfigChangeStore.set_guarded_keys({"risk.max_notional"})
    client = TestClient(app)

    response = client.post(
        "/config/update",
        json={
            "key": "risk.max_notional",
            "new_value": {"notional": 1_000_000},
            "author": "alice",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert body["version"] is None
    assert body["approvals"] == ["alice"]
    assert body["requires_second_approval"] is True

    pending_response = client.get("/config/pending")
    assert pending_response.status_code == 200
    pending = pending_response.json()
    assert len(pending) == 1
    assert pending[0]["request_id"] == body["request_id"]

    duplicate = client.post(
        "/config/approve",
        json={"request_id": body["request_id"], "author": "alice"},
    )
    assert duplicate.status_code == 400
    assert duplicate.json()["detail"] == "duplicate_approval"

    approval = client.post(
        "/config/approve",
        json={"request_id": body["request_id"], "author": "bob"},
    )
    assert approval.status_code == 200
    approval_body = approval.json()
    assert approval_body["status"] == "applied"
    assert approval_body["version"] == 1
    assert approval_body["approvals"] == ["alice", "bob"]

    assert client.get("/config/pending").json() == []

    versions = ConfigChangeStore.config_versions()
    assert len(versions) == 1
    latest = versions[0]
    assert latest.config_key == "risk.max_notional"
    assert latest.value == {"notional": 1_000_000}
    assert latest.approvals == ("alice", "bob")

    audit_actions = [entry.action for entry in ConfigChangeStore.audit_entries()]
    assert audit_actions == [
        "config_update_requested",
        "config_update_approved",
        "config_update_applied",
    ]

