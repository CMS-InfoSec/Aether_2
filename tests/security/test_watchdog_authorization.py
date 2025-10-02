"""Authorization tests for the watchdog oversight endpoint."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for watchdog authorization tests")

from fastapi.testclient import TestClient

import watchdog
from auth.service import InMemorySessionStore
from services.common.security import set_default_session_store


@pytest.fixture
def watchdog_client() -> tuple[TestClient, InMemorySessionStore]:
    """Provide a TestClient with an isolated session store."""

    store = InMemorySessionStore()
    set_default_session_store(store)
    watchdog.app.state.session_store = store

    try:
        with TestClient(watchdog.app) as client:
            yield client, store
    finally:
        set_default_session_store(None)
        if hasattr(watchdog.app.state, "session_store"):
            delattr(watchdog.app.state, "session_store")


def test_oversight_status_requires_authenticated_session(
    watchdog_client: tuple[TestClient, InMemorySessionStore]
) -> None:
    client, _store = watchdog_client

    response = client.get("/oversight/status")

    assert response.status_code == 401


def test_oversight_status_rejects_non_admin_sessions(
    watchdog_client: tuple[TestClient, InMemorySessionStore]
) -> None:
    client, store = watchdog_client
    session = store.create("guest")

    response = client.get(
        "/oversight/status",
        headers={"Authorization": f"Bearer {session.token}"},
    )

    assert response.status_code == 403


def test_oversight_status_allows_admin_sessions(
    watchdog_client: tuple[TestClient, InMemorySessionStore]
) -> None:
    client, store = watchdog_client
    session = store.create("company")

    response = client.get(
        "/oversight/status",
        headers={"Authorization": f"Bearer {session.token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert "total_vetoes" in payload
