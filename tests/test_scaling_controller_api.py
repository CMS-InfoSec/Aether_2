from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from typing import Tuple
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ImportError(f"Unable to load module spec for {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


if "auth" not in sys.modules:
    _load_module("auth", ROOT / "auth" / "__init__.py")
if "auth.service" not in sys.modules:
    _load_module("auth.service", ROOT / "auth" / "service.py")

if "services" not in sys.modules:
    _load_module("services", ROOT / "services" / "__init__.py")
if "services.common" not in sys.modules:
    _load_module("services.common", ROOT / "services" / "common" / "__init__.py")
if "services.common.security" not in sys.modules:
    _load_module("services.common.security", ROOT / "services" / "common" / "security.py")

from auth.service import InMemorySessionStore
from services.common.security import set_default_session_store


@pytest.fixture
def scaling_api_client(monkeypatch: pytest.MonkeyPatch) -> Tuple[TestClient, InMemorySessionStore]:
    module = importlib.import_module("scaling_controller")

    status_payload = module.ScalingStatus(oms_replicas=3, gpu_nodes=1, pending_jobs=0)

    class _StubController:
        def __init__(self, status: module.ScalingStatus) -> None:
            self._status = status

        @property
        def status(self) -> module.ScalingStatus:
            return self._status

    monkeypatch.setattr(module, "_controller", _StubController(status_payload))

    app = FastAPI()
    app.include_router(module.router)

    store = InMemorySessionStore()
    set_default_session_store(store)

    client = TestClient(app)
    try:
        yield client, store
    finally:
        client.app.dependency_overrides.clear()
        set_default_session_store(None)


def test_scaling_status_requires_authentication(
    scaling_api_client: Tuple[TestClient, InMemorySessionStore]
) -> None:
    client, _ = scaling_api_client
    response = client.get("/infra/scaling/status")
    assert response.status_code == 401
    assert response.json()["detail"] == "Missing Authorization header."


def test_scaling_status_rejects_mismatched_account_header(
    scaling_api_client: Tuple[TestClient, InMemorySessionStore]
) -> None:
    client, store = scaling_api_client
    session = store.create("company")
    response = client.get(
        "/infra/scaling/status",
        headers={
            "Authorization": f"Bearer {session.token}",
            "X-Account-ID": "director-1",
        },
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "Account header does not match authenticated session."


def test_scaling_status_rejects_non_admin_account(
    scaling_api_client: Tuple[TestClient, InMemorySessionStore]
) -> None:
    client, store = scaling_api_client
    session = store.create("viewer")
    response = client.get(
        "/infra/scaling/status",
        headers={"Authorization": f"Bearer {session.token}"},
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "Account is not authorized for administrative access."


def test_scaling_status_returns_state_for_admin(
    scaling_api_client: Tuple[TestClient, InMemorySessionStore]
) -> None:
    client, store = scaling_api_client
    session = store.create("company")
    response = client.get(
        "/infra/scaling/status",
        headers={"Authorization": f"Bearer {session.token}"},
    )
    assert response.status_code == 200
    assert response.json() == {
        "oms_replicas": 3,
        "gpu_nodes": 1,
        "pending_jobs": 0,
    }
