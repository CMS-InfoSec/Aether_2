from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("fastapi", reason="FastAPI is required for report service tests")

from fastapi.testclient import TestClient

from auth.service import InMemorySessionStore


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture()
def report_service_client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.syspath_prepend(str(ROOT))
    monkeypatch.setenv(
        "PYTHONPATH",
        str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""),
    )

    previous_modules = {
        "services.common.security": sys.modules.get("services.common.security"),
        "services.common": sys.modules.get("services.common"),
        "services": sys.modules.get("services"),
        "report_service": sys.modules.get("report_service"),
    }

    for name in list(previous_modules):
        sys.modules.pop(name, None)

    module = importlib.import_module("report_service")
    security = importlib.import_module("services.common.security")

    client = TestClient(module.app)
    store = InMemorySessionStore()

    previous_store = getattr(security, "_DEFAULT_SESSION_STORE", None)
    security.set_default_session_store(store)

    try:
        yield client, module, store
    finally:
        client.close()
        security.set_default_session_store(previous_store)
        client.app.dependency_overrides.clear()
        for name, previous in previous_modules.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous


def test_daily_report_requires_pandas(monkeypatch: pytest.MonkeyPatch, report_service_client) -> None:
    client, module, store = report_service_client
    session = store.create("company")

    monkeypatch.setattr(module, "pd", None, raising=False)

    response = client.get(
        "/reports/daily",
        headers=_auth_headers(session.token),
        params={"account_id": "company"},
    )

    assert response.status_code == 503
    body = response.json()
    assert "pandas" in body["detail"].lower()
