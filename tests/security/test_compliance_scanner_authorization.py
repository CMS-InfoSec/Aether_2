"""Authorization tests for the compliance scanner service."""

from __future__ import annotations

import datetime as dt
import importlib

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for API authorization tests")
pytest.importorskip(
    "sqlalchemy",
    reason="sqlalchemy is required for the compliance scanner authorization tests",
)

from fastapi.testclient import TestClient

from services.common.security import require_admin_account


@pytest.fixture
def compliance_scanner_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Create a test client for the compliance scanner admin endpoints."""

    monkeypatch.setenv("COMPLIANCE_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    module = importlib.import_module("compliance_scanner")

    async def fake_refresh_watchlists() -> None:
        module._last_updated = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
        module._symbols_checked = 5

    async def fake_refresh_periodically() -> None:
        return None

    monkeypatch.setattr(module, "_refresh_watchlists", fake_refresh_watchlists)
    monkeypatch.setattr(module, "_refresh_periodically", fake_refresh_periodically)

    client = TestClient(module.app)
    try:
        yield client
    finally:
        client.app.dependency_overrides.clear()
        module._last_updated = None
        module._symbols_checked = 0
        module._background_task = None


def test_compliance_scan_status_requires_authentication(
    compliance_scanner_client: TestClient,
) -> None:
    response = compliance_scanner_client.get("/compliance/scan/status")
    assert response.status_code in {401, 403}


def test_compliance_scan_run_requires_authentication(
    compliance_scanner_client: TestClient,
) -> None:
    response = compliance_scanner_client.post("/compliance/scan/run")
    assert response.status_code in {401, 403}


def test_compliance_scan_endpoints_allow_admin_override(
    compliance_scanner_client: TestClient,
) -> None:
    compliance_scanner_client.app.dependency_overrides[require_admin_account] = lambda: "root"
    try:
        status_response = compliance_scanner_client.get("/compliance/scan/status")
        run_response = compliance_scanner_client.post("/compliance/scan/run")
    finally:
        compliance_scanner_client.app.dependency_overrides.pop(require_admin_account, None)

    assert status_response.status_code == 200
    assert run_response.status_code == 200
    assert "symbols_checked" in status_response.json()
    assert "last_updated" in run_response.json()
