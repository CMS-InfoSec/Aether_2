"""Authorization tests for the override service FastAPI endpoints."""

from __future__ import annotations

import sys
from typing import Iterator, Tuple

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from tests.helpers.override_service import bootstrap_override_service


@pytest.fixture
def override_client(tmp_path, monkeypatch) -> Iterator[Tuple[object, TestClient]]:
    """Provide a freshly-initialized override service TestClient for each test."""

    module = bootstrap_override_service(tmp_path, monkeypatch, reset=True)

    with TestClient(module.app) as client:
        yield module, client

    module.app.dependency_overrides.clear()
    module.ENGINE.dispose()
    sys.modules.pop("override_service", None)


def test_record_override_stores_authenticated_actor(override_client) -> None:
    _module, client = override_client

    response = client.post(
        "/override/trade",
        json={
            "intent_id": "abc-123",
            "decision": "approve",
            "reason": "Manual approval for testing",
        },
        headers={
            "X-Account-ID": "company",
            "X-Actor": "spoofed-actor",
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["actor"] == "company"
    assert body["account_id"] == "company"

    history = client.get("/override/history", headers={"X-Account-ID": "company"})
    assert history.status_code == 200
    records = history.json()["overrides"]
    assert len(records) == 1
    assert records[0]["actor"] == "company"


def test_record_override_rejects_non_admin_account(override_client) -> None:
    _module, client = override_client

    response = client.post(
        "/override/trade",
        json={
            "intent_id": "blocked-1",
            "decision": "reject",
            "reason": "Insufficient collateral",
        },
        headers={"X-Account-ID": "trader-1"},
    )

    assert response.status_code == 403


def test_override_history_requires_authenticated_admin(override_client) -> None:
    _module, client = override_client

    client.post(
        "/override/trade",
        json={
            "intent_id": "history-1",
            "decision": "approve",
            "reason": "Approval for audit",
        },
        headers={"X-Account-ID": "company"},
    )

    response = client.get("/override/history", headers={"X-Account-ID": "company"})
    assert response.status_code == 200
    assert any(entry["intent_id"] == "history-1" for entry in response.json()["overrides"])


def test_override_history_rejects_missing_credentials(override_client) -> None:
    _module, client = override_client

    response = client.get("/override/history")
    assert response.status_code == 401
