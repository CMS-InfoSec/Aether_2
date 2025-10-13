from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Iterator, Tuple

import pytest

pytest.importorskip("fastapi", reason="FastAPI is required for advisor service tests")

from fastapi.testclient import TestClient

from auth.service import InMemorySessionStore
from tests.helpers.advisor_service import bootstrap_advisor_service


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture()
def advisor_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Tuple[TestClient, object, InMemorySessionStore]]:
    """Provide a configured advisor service client backed by an isolated database."""

    monkeypatch.syspath_prepend(str(ROOT))
    monkeypatch.setenv("PYTHONPATH", str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))

    previous_modules = {
        "services.common.security": sys.modules.get("services.common.security"),
        "services.common": sys.modules.get("services.common"),
        "services": sys.modules.get("services"),
        "advisor_service": sys.modules.get("advisor_service"),
    }

    for name in list(previous_modules):
        sys.modules.pop(name, None)

    module = bootstrap_advisor_service(tmp_path, monkeypatch, reset=True)
    security = sys.modules["services.common.security"]

    store = getattr(module, "SESSION_STORE", None)
    if not isinstance(store, InMemorySessionStore):
        store = InMemorySessionStore()
        module.app.state.session_store = store
        setattr(module, "SESSION_STORE", store)
    previous_store = getattr(security, "_DEFAULT_SESSION_STORE", None)
    security.set_default_session_store(store)

    with TestClient(module.app) as client:
        try:
            yield client, module, store
        finally:
            security.set_default_session_store(previous_store)
            client.app.dependency_overrides.clear()
            module.ENGINE.dispose()
            for name, previous in previous_modules.items():
                if previous is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = previous


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_query_requires_authentication(advisor_client) -> None:
    client, _, _ = advisor_client

    response = client.post(
        "/advisor/query",
        json={"user_id": "company", "question": "What changed?"},
    )

    assert response.status_code == 401


def test_query_rejects_mismatched_user(advisor_client) -> None:
    client, _, store = advisor_client
    session = store.create("company")

    response = client.post(
        "/advisor/query",
        headers=_auth_headers(session.token),
        json={"user_id": "shadow", "question": "Investigate recent losses."},
    )

    assert response.status_code == 403


def test_query_records_authorized_actor(advisor_client) -> None:
    client, module, store = advisor_client
    session = store.create("company")

    response = client.post(
        "/advisor/query",
        headers=_auth_headers(session.token),
        json={"user_id": "COMPANY", "question": "Summarise overnight performance."},
    )

    assert response.status_code == 200
    payload = response.json()
    assert "answer" in payload
    assert payload["context"]["logs"] == []

    with module.SessionLocal() as db:
        records = db.query(module.AdvisorQuery).all()

    assert len(records) == 1
    entry = records[0]
    assert entry.user_id.lower() == "company"
    assert entry.question == "Summarise overnight performance."


def test_insecure_store_persists_across_reloads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state_dir = tmp_path / "state"
    monkeypatch.setenv("AETHER_STATE_DIR", str(state_dir))

    for name in list(sys.modules):
        if name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, name, raising=False)

    monkeypatch.setitem(sys.modules, "sqlalchemy", ModuleType("sqlalchemy"))

    sys.modules.pop("advisor_service", None)
    module = importlib.import_module("advisor_service")

    store = module._get_in_memory_store("memory://advisor-test")
    store.reset()
    store.add(module.AdvisorQuery(user_id="company", question="How did we perform?", answer="Stable."))

    sys.modules.pop("advisor_service", None)
    module_reloaded = importlib.import_module("advisor_service")
    store_reloaded = module_reloaded._get_in_memory_store("memory://advisor-test")

    records = store_reloaded.all()
    assert records, "Expected advisor history to persist across reloads"
    assert records[0].question == "How did we perform?"


def test_readyz_reports_success(advisor_client) -> None:
    client, _, _ = advisor_client

    response = client.get("/readyz")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["checks"]["postgres_read"]["status"] == "ok"


def test_readyz_returns_503_on_failure(advisor_client) -> None:
    client, module, _ = advisor_client

    original_factory = getattr(module.app.state, "db_session_factory", None)
    module.app.state.db_session_factory = None
    try:
        response = client.get("/readyz")
    finally:
        module.app.state.db_session_factory = original_factory

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "error"
    assert payload["checks"]["postgres_read"]["status"] == "error"
