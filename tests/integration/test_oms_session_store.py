from __future__ import annotations

import importlib
import os
import sys
import types
from pathlib import Path
from typing import Any, Dict

from auth.service import InMemorySessionStore


if "aiohttp" not in sys.modules:

    class _StubSession:
        async def __aenter__(self) -> "_StubSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def close(self) -> None:
            return None

        async def post(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("aiohttp stub invoked")

        async def get(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("aiohttp stub invoked")

    class _ClientTimeout:
        def __init__(self, total: float | None = None) -> None:
            self.total = total

    aiohttp_stub = types.SimpleNamespace(
        ClientSession=lambda *args, **kwargs: _StubSession(),
        ClientTimeout=_ClientTimeout,
        ClientError=Exception,
    )
    sys.modules["aiohttp"] = aiohttp_stub

os.environ.setdefault("SESSION_REDIS_URL", "memory://oms-test")

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

existing_services = sys.modules.get("services")
if existing_services is not None:
    module_path = getattr(existing_services, "__file__", "")
    if not module_path or not module_path.startswith(str(ROOT)):
        sys.modules.pop("services", None)
        sys.modules.pop("services.common", None)
        sys.modules.pop("services.oms", None)
else:
    sys.modules.pop("services.common", None)
    sys.modules.pop("services.oms", None)

from services.oms import main


def _issue_session_token(account_id: str) -> str:
    store = getattr(main.app.state, "session_store", None)
    if store is None:
        raise RuntimeError("OMS session store is not configured")
    session = store.create(account_id)
    return session.token


def test_memory_session_store_initialises(monkeypatch) -> None:
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://local")

    import services.oms.main as main_module

    reloaded = importlib.reload(main_module)

    assert isinstance(reloaded.app.state.session_store, InMemorySessionStore)

    monkeypatch.setenv("SESSION_REDIS_URL", "memory://oms-test")
    importlib.reload(main_module)


def test_valid_session_token_allows_access(monkeypatch) -> None:
    monkeypatch.setattr("shared.graceful_shutdown.install_sigterm_handler", lambda manager: None)
    monkeypatch.setattr(main.shutdown_manager, "start_draining", lambda *args, **kwargs: False)
    monkeypatch.setattr(main.shutdown_manager, "_draining", False)
    monkeypatch.setattr(main.shadow_oms, "snapshot", lambda account_id: {"account_id": account_id})

    token = _issue_session_token("company")
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Account-ID": "company",
    }

    with TestClient(main.app) as client:
        response = client.get("/oms/shadow_pnl", params={"account_id": "company"}, headers=headers)

    assert response.status_code == 200
    body: Dict[str, Any] = response.json()
    assert body["account_id"] == "company"


def test_invalid_session_token_returns_unauthorized(monkeypatch) -> None:
    monkeypatch.setattr("shared.graceful_shutdown.install_sigterm_handler", lambda manager: None)
    monkeypatch.setattr(main.shutdown_manager, "start_draining", lambda *args, **kwargs: False)
    monkeypatch.setattr(main.shutdown_manager, "_draining", False)
    monkeypatch.setattr(main.shadow_oms, "snapshot", lambda account_id: {"account_id": account_id})

    headers = {
        "Authorization": "Bearer invalid-token",
        "X-Account-ID": "company",
    }

    with TestClient(main.app) as client:
        response = client.get("/oms/shadow_pnl", params={"account_id": "company"}, headers=headers)

    assert response.status_code == 401
    assert "invalid" in response.json()["detail"].lower()


def test_session_survives_new_app_instance(monkeypatch) -> None:
    monkeypatch.setenv("SESSION_REDIS_URL", "redis://test-redis/0")

    store: Dict[str, str] = {}

    class _FakeRedisClient:
        def __init__(self) -> None:
            self._store = store

        @classmethod
        def from_url(cls, url: str, **_: Any) -> "_FakeRedisClient":
            del url
            return cls()

        def setex(self, key: str, ttl: int, value: str) -> None:
            del ttl
            self._store[key] = value

        def get(self, key: str) -> str | None:
            return self._store.get(key)

        def delete(self, key: str) -> None:
            self._store.pop(key, None)

    redis_stub = types.SimpleNamespace(Redis=_FakeRedisClient)
    monkeypatch.setitem(sys.modules, "redis", redis_stub)

    import services.oms.main as main  # late import to honour patched env

    main = importlib.reload(main)
    monkeypatch.setattr(
        "shared.graceful_shutdown.install_sigterm_handler", lambda manager: None
    )
    monkeypatch.setattr(main.shutdown_manager, "start_draining", lambda *a, **k: False)
    monkeypatch.setattr(main.shutdown_manager, "_draining", False)
    monkeypatch.setattr(main.shadow_oms, "snapshot", lambda account_id: {"account_id": account_id})

    token = main.app.state.session_store.create("company").token

    main = importlib.reload(main)
    session = main.app.state.session_store.get(token)

    assert session is not None, "session token should remain valid across instances"
    assert session.admin_id == "company"

    # Restore the default in-memory configuration for the remainder of the suite.
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://oms-test")
    importlib.reload(main)
