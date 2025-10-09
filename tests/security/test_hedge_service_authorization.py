"""Authorization tests for hedge service override endpoints."""

from __future__ import annotations

from typing import Dict

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.hedge.hedge_service as hedge_service
from services.common.security import require_admin_account


@pytest.fixture()
def hedge_client() -> TestClient:
    service = hedge_service.get_hedge_service()
    service._override = None  # type: ignore[attr-defined]
    service._history.clear()
    service._last_diagnostics = None

    app = FastAPI()
    app.include_router(hedge_service.router)
    client = TestClient(app)
    try:
        yield client
    finally:
        app.dependency_overrides.clear()


def test_set_override_requires_admin(hedge_client: TestClient) -> None:
    response = hedge_client.post(
        "/hedge/override",
        json={"target_pct": 55.0, "reason": "stability check"},
    )
    assert response.status_code == 401


def test_set_override_allows_admin(hedge_client: TestClient) -> None:
    hedge_client.app.dependency_overrides[require_admin_account] = lambda: "company"
    try:
        response = hedge_client.post(
            "/hedge/override",
            json={"target_pct": 42.0, "reason": "rebalance window"},
        )
    finally:
        hedge_client.app.dependency_overrides.pop(require_admin_account, None)

    assert response.status_code == 200
    payload: Dict[str, object] = response.json()
    assert payload["target_pct"] == 42.0


def test_history_requires_admin(hedge_client: TestClient) -> None:
    response = hedge_client.get("/hedge/history")
    assert response.status_code == 401


def test_history_allows_admin(hedge_client: TestClient) -> None:
    hedge_client.app.dependency_overrides[require_admin_account] = lambda: "director-1"
    try:
        response = hedge_client.get("/hedge/history")
    finally:
        hedge_client.app.dependency_overrides.pop(require_admin_account, None)

    assert response.status_code == 200
    assert response.json() == []
