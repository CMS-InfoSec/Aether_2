from __future__ import annotations

from __future__ import annotations

import sys
import types
from typing import Any, Dict


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


from fastapi.testclient import TestClient

from services.oms import main


def _issue_session_token(account_id: str) -> str:
    store = getattr(main.app.state, "session_store", None)
    if store is None:
        raise RuntimeError("OMS session store is not configured")
    session = store.create(account_id)
    return session.token


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
