from __future__ import annotations

import os
import sys
from pathlib import Path

import importlib

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for API authorization tests")

os.environ.setdefault("STRATEGY_DATABASE_URL", "postgresql+psycopg://user:pass@localhost:5432/strategy")

root = Path(__file__).resolve().parents[2]
root_str = str(root)
sys.path = [p for p in sys.path if p != root_str]
sys.path.insert(0, root_str)

SECURITY_MODULE = "services.common.security"
try:
    security = importlib.import_module(SECURITY_MODULE)
except ModuleNotFoundError:
    spec = importlib.util.spec_from_file_location(
        SECURITY_MODULE,
        root / "services" / "common" / "security.py",
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive fallback
        raise
    security = importlib.util.module_from_spec(spec)
    sys.modules[SECURITY_MODULE] = security
    spec.loader.exec_module(security)

from fastapi.testclient import TestClient

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from auth.service import InMemorySessionStore
import strategy_orchestrator
from tests.test_strategy_orchestrator import _make_request

reload_admin_accounts = security.reload_admin_accounts
set_default_session_store = security.set_default_session_store
from tests.test_strategy_orchestrator import _make_request


@pytest.fixture(autouse=True)
def configure_security() -> InMemorySessionStore:
    store = InMemorySessionStore()
    set_default_session_store(store)
    reload_admin_accounts(["company", "director-1", "director-2"])
    strategy_orchestrator.SESSION_STORE = store
    strategy_orchestrator.app.state.session_store = store
    engine = create_engine(
        "sqlite:///:memory:", future=True, connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    strategy_orchestrator.Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    registry = strategy_orchestrator.StrategyRegistry(
        session_factory,
        risk_engine_url="http://risk.test",
        default_strategies=[],
    )

    class _SignalBusStub:
        def list_signals(self) -> list[object]:
            return []

    strategy_orchestrator._set_components(registry, _SignalBusStub())
    try:
        yield store
    finally:
        set_default_session_store(None)
        reload_admin_accounts()
        strategy_orchestrator.SESSION_STORE = None
        strategy_orchestrator.app.state.session_store = None
        strategy_orchestrator.REGISTRY = None
        strategy_orchestrator.SIGNAL_BUS = None
        strategy_orchestrator.INITIALIZATION_ERROR = None


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
