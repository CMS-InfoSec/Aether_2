"""Tests for the explainability UI service security and responses."""

from __future__ import annotations

from typing import Dict

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for explain service tests")

from fastapi.testclient import TestClient
from fastapi import HTTPException

from auth.service import InMemorySessionStore
from services.common import security
from services.ui import explain_service


@pytest.fixture
def client() -> TestClient:
    """Return a test client bound to the explainability service app."""

    return TestClient(explain_service.app)


@pytest.fixture
def session_store(monkeypatch: pytest.MonkeyPatch) -> InMemorySessionStore:
    """Configure an in-memory session store for authentication dependencies."""

    store = InMemorySessionStore()
    security.set_default_session_store(store)
    try:
        yield store
    finally:
        security.set_default_session_store(None)


def test_trade_explanation_missing_auth_returns_401(client: TestClient) -> None:
    response = client.get("/explain/trade", params={"trade_id": "fill-1"})
    assert response.status_code == 401


def test_trade_explanation_requires_admin_privileges(
    client: TestClient, session_store: InMemorySessionStore
) -> None:
    session = session_store.create("portfolio-analyst")

    response = client.get(
        "/explain/trade",
        params={"trade_id": "fill-1"},
        headers={"Authorization": f"Bearer {session.token}"},
    )

    assert response.status_code == 403


def test_trade_explanation_allows_admin_identity(
    client: TestClient, session_store: InMemorySessionStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    session = session_store.create("company")

    trade_payload = {
        "fill_id": "fill-123",
        "account_id": "company",
        "instrument": "AAPL",
        "features": {"alpha": 0.6, "beta": -0.2, "gamma": 0.1},
        "metadata": {"regime": "bull"},
    }

    class StubModel:
        def explain(self, features: Dict[str, float]) -> Dict[str, float]:
            return features

    monkeypatch.setattr(explain_service, "_load_trade_record", lambda trade_id: trade_payload)
    monkeypatch.setattr(explain_service, "get_active_model", lambda account, instrument: StubModel())

    response = client.get(
        "/explain/trade",
        params={"trade_id": "fill-123"},
        headers={"Authorization": f"Bearer {session.token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["trade_id"] == "fill-123"
    assert payload["regime"] == "bull"
    assert payload["model_used"] == "StubModel"
    assert payload["top_features"][0]["feature"] == "alpha"


def test_database_url_requires_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("REPORT_DATABASE_URL", "TIMESCALE_DSN", "DATABASE_URL"):
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(HTTPException) as exc_info:
        explain_service._database_url()

    assert exc_info.value.status_code == 500


def test_database_url_normalizes_timescale_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPORT_DATABASE_URL", "timescale://user:pass@host:5432/db")

    resolved = explain_service._database_url()

    assert resolved == "postgresql://user:pass@host:5432/db"


def test_database_url_rejects_sqlite(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPORT_DATABASE_URL", "sqlite:///tmp/test.db")

    with pytest.raises(HTTPException) as exc_info:
        explain_service._database_url()

    assert exc_info.value.status_code == 500
