"""Smoke tests validating override service persistence semantics."""

from __future__ import annotations

import sys

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from tests.helpers.override_service import bootstrap_override_service


def _record_override(client: TestClient, intent_id: str, actor: str) -> None:
    response = client.post(
        "/override/trade",
        json={"intent_id": intent_id, "decision": "approve", "reason": "smoke test"},
        headers={"X-Account-ID": actor, "X-Actor": actor},
    )
    assert response.status_code == 201


def _intent_ids(client: TestClient) -> set[str]:
    response = client.get("/override/history", headers={"X-Account-ID": "company"})
    assert response.status_code == 200
    return {entry["intent_id"] for entry in response.json()["overrides"]}


def test_overrides_persist_across_restarts_and_replicas(tmp_path, monkeypatch) -> None:
    """Overrides should persist across restarts and be visible to new replicas."""

    db_filename = "shared-override.db"

    module_a = bootstrap_override_service(tmp_path, monkeypatch, reset=True, db_filename=db_filename)
    with TestClient(module_a.app) as client_a:
        _record_override(client_a, "order-1", "company")
    module_a.ENGINE.dispose()

    module_b = bootstrap_override_service(tmp_path, monkeypatch, db_filename=db_filename)
    with TestClient(module_b.app) as client_b:
        intents = _intent_ids(client_b)
        assert "order-1" in intents
        _record_override(client_b, "order-2", "company")
    module_b.ENGINE.dispose()

    module_c = bootstrap_override_service(tmp_path, monkeypatch, db_filename=db_filename)
    with TestClient(module_c.app) as client_c:
        intents = _intent_ids(client_c)
        assert {"order-1", "order-2"}.issubset(intents)

    latest = module_c.latest_override("order-2")
    assert latest is not None
    assert latest.intent_id == "order-2"
    assert latest.actor == "company"
    module_c.ENGINE.dispose()
    sys.modules.pop("override_service", None)
