"""Security-focused tests for the HITL FastAPI service."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator, Tuple

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for HITL service tests")

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

services_init = ROOT / "services" / "__init__.py"
if services_init.exists():
    sys.modules.pop("services", None)
    sys.modules.pop("services.common", None)
    spec = importlib.util.spec_from_file_location(
        "services",
        services_init,
        submodule_search_locations=[str(services_init.parent)],
    )
    if spec is not None and spec.loader is not None:
        module = importlib.util.module_from_spec(spec)
        module.__path__ = [str(services_init.parent)]  # type: ignore[attr-defined]
        sys.modules["services"] = module
        spec.loader.exec_module(module)

from fastapi.testclient import TestClient

from auth.service import InMemorySessionStore

security: Any | None = None


@pytest.fixture()
def hitl_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Tuple[TestClient, Any]]:
    """Provide a HITL service client backed by an isolated database."""

    global security

    monkeypatch.setenv("HITL_DATABASE_URL", f"sqlite:///{tmp_path}/hitl.db")

    security = importlib.import_module("services.common.security")
    security.set_default_session_store(InMemorySessionStore(ttl_minutes=60))

    sys.modules.pop("hitl_service", None)
    module = importlib.import_module("hitl_service")

    try:
        with TestClient(module.app) as client:
            yield client, module
    finally:
        module.app.dependency_overrides.clear()
        sys.modules.pop("hitl_service", None)
        if security is not None:
            security.set_default_session_store(None)
        security = None


def _review_payload(account_id: str, *, intent_id: str = "intent-123") -> dict[str, Any]:
    return {
        "account_id": account_id,
        "intent_id": intent_id,
        "trade_details": {
            "symbol": "BTC",
            "side": "buy",
            "notional": 1_000_000.0,
            "model_confidence": 0.1,
            "metadata": {},
        },
    }


def _admin_headers(account_id: str = "company") -> dict[str, str]:
    if security is None:  # pragma: no cover - defensive guard for misconfigured tests
        raise RuntimeError("Security module not initialised for tests.")

    store = security._DEFAULT_SESSION_STORE
    if store is None:  # pragma: no cover - defensive guard for misconfigured tests
        raise RuntimeError("Session store is not configured for tests.")
    session = store.create(account_id)
    return {
        "Authorization": f"Bearer {session.token}",
        "X-Account-ID": account_id,
    }


def test_review_trade_requires_authentication(hitl_client: Tuple[TestClient, Any]) -> None:
    client, _ = hitl_client

    response = client.post("/hitl/review", json=_review_payload("company"))

    assert response.status_code == 401


def test_review_trade_rejects_mismatched_account(hitl_client: Tuple[TestClient, Any]) -> None:
    client, _ = hitl_client

    response = client.post(
        "/hitl/review",
        json=_review_payload("ops"),
        headers=_admin_headers("company"),
    )

    assert response.status_code == 403


def test_review_trade_allows_matching_admin(hitl_client: Tuple[TestClient, Any]) -> None:
    client, _ = hitl_client

    response = client.post(
        "/hitl/review",
        json=_review_payload("company"),
        headers=_admin_headers("company"),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["intent_id"] == "intent-123"


def test_pending_requires_authentication(hitl_client: Tuple[TestClient, Any]) -> None:
    client, _ = hitl_client

    response = client.get("/hitl/pending")

    assert response.status_code == 401


def test_pending_filters_to_authenticated_account(hitl_client: Tuple[TestClient, Any]) -> None:
    client, _ = hitl_client

    client.post(
        "/hitl/review",
        json=_review_payload("company", intent_id="intent-1"),
        headers=_admin_headers("company"),
    )
    client.post(
        "/hitl/review",
        json=_review_payload("director-1", intent_id="intent-2"),
        headers=_admin_headers("director-1"),
    )

    response = client.get("/hitl/pending", headers=_admin_headers("company"))

    assert response.status_code == 200
    payload = response.json()
    assert all(item["account_id"].lower() == "company" for item in payload)


def test_approve_trade_rejects_mismatched_account(hitl_client: Tuple[TestClient, Any]) -> None:
    client, _ = hitl_client

    client.post(
        "/hitl/review",
        json=_review_payload("company"),
        headers=_admin_headers("company"),
    )

    response = client.post(
        "/hitl/approve",
        json={"intent_id": "intent-123", "approved": True},
        headers=_admin_headers("director-1"),
    )

    assert response.status_code == 403


def test_approve_trade_allows_matching_admin(hitl_client: Tuple[TestClient, Any]) -> None:
    client, _ = hitl_client

    client.post(
        "/hitl/review",
        json=_review_payload("company", intent_id="intent-9"),
        headers=_admin_headers("company"),
    )

    response = client.post(
        "/hitl/approve",
        json={"intent_id": "intent-9", "approved": True},
        headers=_admin_headers("company"),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "approved"


def test_sqlalchemy2_sessions_initialise(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("hitl_service")

    class _ProbeSession:
        def __init__(self) -> None:
            self.closed = False

        def get(self, *args: Any, **kwargs: Any) -> None:
            return None

        def scalars(self, *args: Any, **kwargs: Any) -> list[Any]:
            return []

        def execute(self, *args: Any, **kwargs: Any) -> SimpleNamespace:
            return SimpleNamespace(rowcount=0)

        def close(self) -> None:
            self.closed = True

    def _sessionmaker_factory(*args: Any, **kwargs: Any):
        def _factory() -> _ProbeSession:
            return _ProbeSession()

        return _factory

    monkeypatch.setattr(module, "create_engine", lambda url, **_: SimpleNamespace(url=url))
    monkeypatch.setattr(module, "sessionmaker", lambda **__: _sessionmaker_factory())
    monkeypatch.setattr(
        module,
        "Base",
        SimpleNamespace(metadata=SimpleNamespace(create_all=lambda **__: None)),
    )
    engine, session_factory, store = module._initialise_sqlalchemy_backend("postgresql://example")

    assert engine is not None
    assert session_factory is not None
    assert store is not None

