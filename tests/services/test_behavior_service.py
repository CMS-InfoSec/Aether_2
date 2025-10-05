from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path

from fastapi.testclient import TestClient

from auth.service import InMemorySessionStore


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

assert (ROOT / "behavior_service.py").exists()

os.environ.setdefault("BEHAVIOR_ALLOW_SQLITE_FOR_TESTS", "1")
os.environ.setdefault("BEHAVIOR_DATABASE_URL", "sqlite:///:memory:")

services_init = ROOT / "services" / "__init__.py"
spec = importlib.util.spec_from_file_location("services", services_init)
module = importlib.util.module_from_spec(spec)
module.__path__ = [str(ROOT / "services")]  # type: ignore[assignment]
assert spec.loader is not None
spec.loader.exec_module(module)
sys.modules["services"] = module

common_init = ROOT / "services" / "common" / "__init__.py"
common_spec = importlib.util.spec_from_file_location(
    "services.common",
    common_init,
    submodule_search_locations=[str(ROOT / "services" / "common")],
)
common_module = importlib.util.module_from_spec(common_spec)
assert common_spec.loader is not None
common_spec.loader.exec_module(common_module)
sys.modules["services.common"] = common_module


def _load_behavior_service():
    try:
        import behavior_service as module
    except ModuleNotFoundError as exc:  # pragma: no cover - defensive for optional deps
        if exc.name != "services.alert_manager":
            raise
        stub = types.ModuleType("services.alert_manager")

        class _RiskEvent:  # minimal stub for dependency injection
            def __init__(self, *args: object, **kwargs: object) -> None:  # noqa: D401 - simple stub
                pass

        stub.RiskEvent = _RiskEvent  # type: ignore[attr-defined]
        stub.get_alert_manager_instance = lambda: None  # type: ignore[attr-defined]
        sys.modules.setdefault("services.alert_manager", stub)
        sys.modules.pop("behavior_service", None)
        import behavior_service as module
    return module


behavior_service = _load_behavior_service()


def _make_client() -> tuple[TestClient, InMemorySessionStore]:
    behavior_service._initialize_database()
    client = TestClient(behavior_service.app)
    store = InMemorySessionStore(ttl_minutes=60)
    client.app.state.session_store = store
    return client, store


def _auth_headers(store: InMemorySessionStore, account_id: str) -> dict[str, str]:
    session = store.create(account_id)
    token = session.token
    return {"Authorization": f"Bearer {token}", "X-Account-ID": account_id}


def test_scan_behavior_requires_authentication() -> None:
    client, _ = _make_client()

    response = client.post(
        "/behavior/scan",
        json={"account_id": "company", "lookback_minutes": 60},
    )

    assert response.status_code == 401


def test_scan_behavior_rejects_mismatched_account() -> None:
    client, store = _make_client()

    response = client.post(
        "/behavior/scan",
        headers=_auth_headers(store, "company"),
        json={"account_id": "trading-desk", "lookback_minutes": 60},
    )

    assert response.status_code == 403
    assert (
        response.json()["detail"]
        == "Authenticated account is not authorized for the requested account."
    )


def test_scan_behavior_allows_authenticated_request(monkeypatch) -> None:
    client, store = _make_client()

    monkeypatch.setattr(
        behavior_service.detector,
        "scan_account",
        lambda account_id, lookback_minutes: [],
    )

    response = client.post(
        "/behavior/scan",
        headers=_auth_headers(store, "company"),
        json={"account_id": "company", "lookback_minutes": 30},
    )

    assert response.status_code == 200
    assert response.json() == {"incidents": []}


def test_behavior_status_requires_matching_account() -> None:
    client, store = _make_client()

    response = client.get(
        "/behavior/status",
        headers=_auth_headers(store, "company"),
        params={"account_id": "director-1"},
    )

    assert response.status_code == 403
    assert (
        response.json()["detail"]
        == "Authenticated account is not authorized for the requested account."
    )


def test_behavior_status_returns_history_for_authorized_account() -> None:
    client, store = _make_client()

    response = client.get(
        "/behavior/status",
        headers=_auth_headers(store, "company"),
        params={"account_id": "company"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["account_id"] == "company"
    assert isinstance(payload["incidents"], list)

