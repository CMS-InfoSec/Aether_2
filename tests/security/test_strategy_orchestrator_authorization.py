from __future__ import annotations

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for API authorization tests")

from fastapi.testclient import TestClient

from auth.service import InMemorySessionStore
import strategy_orchestrator
from services.common.security import reload_admin_accounts, set_default_session_store
from tests.test_strategy_orchestrator import _make_request


@pytest.fixture(autouse=True)
def configure_security() -> InMemorySessionStore:
    store = InMemorySessionStore()
    set_default_session_store(store)
    reload_admin_accounts(["company", "director-1", "director-2"])
    try:
        yield store
    finally:
        set_default_session_store(None)
        reload_admin_accounts()


@pytest.fixture
def client() -> TestClient:
    return TestClient(strategy_orchestrator.app)


def test_strategy_endpoints_require_authorization(client: TestClient) -> None:
    request_payload = _make_request().model_dump(mode="json")
    test_matrix = [
        (client.post, "/strategy/register", {"json": {"name": "gamma", "description": "New", "max_nav_pct": 0.1}}),
        (client.post, "/strategy/toggle", {"json": {"name": "alpha", "enabled": False}}),
        (client.get, "/strategy/status", {}),
        (client.post, "/strategy/intent", {"json": {"strategy_name": "alpha", "request": request_payload}}),
        (client.get, "/strategy/signals", {}),
    ]

    for method, path, kwargs in test_matrix:
        response = method(path, **kwargs)
        assert response.status_code == 401


def test_non_admin_session_is_rejected(client: TestClient, configure_security: InMemorySessionStore) -> None:
    session = configure_security.create("analyst-1")
    headers = {"Authorization": f"Bearer {session.token}"}

    response = client.get("/strategy/status", headers=headers)

    assert response.status_code == 403


def test_admin_session_allows_access(client: TestClient, configure_security: InMemorySessionStore) -> None:
    session = configure_security.create("company")
    headers = {"Authorization": f"Bearer {session.token}"}

    response = client.get("/strategy/status", headers=headers)

    assert response.status_code == 200
