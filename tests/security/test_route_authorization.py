"""Authorization tests for admin and auditor protected endpoints."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for API authorization tests")

from fastapi import FastAPI
from fastapi.testclient import TestClient

import alert_prioritizer
import compliance_pack
import multiformat_export
import services.report_service as account_report_service
from audit_mode import AuditorPrincipal
from services.alerts import alert_dedupe
from services.common.security import require_admin_account


@pytest.fixture
def report_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Create a test client for the account report endpoints."""

    app = FastAPI()
    app.include_router(account_report_service.router)

    class StubReportService:
        def recent_feature_attribution(self, account_id: str | None = None) -> Dict[str, Any]:
            return {
                "account_id": account_id or "default",
                "feature_attribution": [],
            }

    stub_service = StubReportService()
    app.dependency_overrides[account_report_service.get_report_service] = lambda: stub_service

    client = TestClient(app)
    try:
        yield client
    finally:
        app.dependency_overrides.clear()


def test_recent_xai_requires_admin_header(report_client: TestClient) -> None:
    response = report_client.get("/reports/xai")
    assert response.status_code == 403


def test_recent_xai_allows_admin_override(report_client: TestClient) -> None:
    report_client.app.dependency_overrides[require_admin_account] = lambda: "company"
    try:
        response = report_client.get("/reports/xai")
    finally:
        report_client.app.dependency_overrides.pop(require_admin_account, None)

    assert response.status_code == 200
    assert response.json()["account_id"] == "default"


@pytest.fixture
def export_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Client for the log export endpoint with exporter stubbed out."""

    app = FastAPI()
    app.include_router(multiformat_export.router)

    class StubExporter:
        def export(self, *, for_date: dt.date) -> multiformat_export.ExportResult:
            artifact = multiformat_export.ExportArtifact(
                format="json",
                object_key="logs/export.json",
                content_type="application/json",
                data=b"{}",
            )
            return multiformat_export.ExportResult(
                run_id="run-123",
                export_date=for_date,
                artifacts={"json": artifact},
            )

    monkeypatch.setattr(multiformat_export, "_get_exporter", lambda: StubExporter())

    client = TestClient(app)
    try:
        yield client
    finally:
        app.dependency_overrides.clear()


def test_export_logs_rejects_non_auditors(export_client: TestClient) -> None:
    response = export_client.get(
        "/logs/export",
        params={"date": "2024-01-01", "format": "json"},
        headers={"X-Account-ID": "analyst"},
    )
    assert response.status_code == 403


def test_export_logs_allows_auditor_override(export_client: TestClient) -> None:
    export_client.app.dependency_overrides[multiformat_export.require_auditor_identity] = (
        lambda: AuditorPrincipal(account_id="auditor-1")
    )
    try:
        response = export_client.get(
            "/logs/export",
            params={"date": "2024-01-01", "format": "json"},
        )
    finally:
        export_client.app.dependency_overrides.pop(multiformat_export.require_auditor_identity, None)

    assert response.status_code == 200
    assert response.headers["X-Run-ID"] == "run-123"


@pytest.fixture
def compliance_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Client for the compliance pack export endpoint."""

    app = FastAPI()
    app.include_router(compliance_pack.router)

    @dataclass
    class StubPack:
        data: bytes = b"{}"
        content_type: str = "application/json"
        sha256: str = "abc123"
        s3_bucket: str = "bucket"
        s3_key: str = "key"
        export_format: str = "sec"
        export_date: dt.date = dt.date(2024, 1, 1)
        generated_at: dt.datetime = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)

    class StubExporter:
        def export(self, *, for_date: dt.date, export_format: str) -> StubPack:
            return StubPack(export_format=export_format, export_date=for_date)

    monkeypatch.setattr(compliance_pack, "_cached_exporter", lambda: StubExporter())

    client = TestClient(app)
    try:
        yield client
    finally:
        app.dependency_overrides.clear()


def test_export_compliance_pack_rejects_non_auditors(compliance_client: TestClient) -> None:
    response = compliance_client.get(
        "/compliance/export",
        params={"date": "2024-01-01", "format": "sec"},
        headers={"X-Account-ID": "auditor-1"},
    )
    assert response.status_code == 403


def test_export_compliance_pack_allows_auditor_override(compliance_client: TestClient) -> None:
    compliance_client.app.dependency_overrides[compliance_pack.require_auditor_identity] = (
        lambda: AuditorPrincipal(account_id="auditor-1")
    )
    try:
        response = compliance_client.get(
            "/compliance/export",
            params={"date": "2024-01-01", "format": "sec"},
        )
    finally:
        compliance_client.app.dependency_overrides.pop(compliance_pack.require_auditor_identity, None)

    assert response.status_code == 200
    assert response.headers["X-Compliance-SHA256"] == "abc123"


@pytest.fixture
def prioritizer_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Client for the alert prioritizer endpoint."""

    app = FastAPI()
    app.include_router(alert_prioritizer.router)

    class StubService:
        async def get_prioritized_alerts(self) -> List[Dict[str, Any]]:
            return [{"id": "a1", "severity": "high"}]

        async def close(self) -> None:
            return None

    monkeypatch.setattr(alert_prioritizer, "_service", StubService())

    client = TestClient(app)
    try:
        yield client
    finally:
        app.dependency_overrides.clear()


def test_prioritized_alerts_require_admin(prioritizer_client: TestClient) -> None:
    response = prioritizer_client.get("/alerts/prioritized")
    assert response.status_code == 403


def test_prioritized_alerts_allow_admin_override(prioritizer_client: TestClient) -> None:
    prioritizer_client.app.dependency_overrides[require_admin_account] = lambda: "company"
    try:
        response = prioritizer_client.get("/alerts/prioritized")
    finally:
        prioritizer_client.app.dependency_overrides.pop(require_admin_account, None)

    assert response.status_code == 200
    assert response.json() == [{"id": "a1", "severity": "high"}]


@pytest.fixture
def alert_dedupe_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Client for the alert dedupe endpoints."""

    app = FastAPI()
    app.include_router(alert_dedupe.router)

    class StubService:
        async def refresh(self) -> List[Dict[str, Any]]:
            return [{"id": "alert"}]

        def policies(self) -> Dict[str, Any]:
            return {"suppression_window_seconds": 600}

    stub_service = StubService()
    app.dependency_overrides[alert_dedupe.get_alert_dedupe_service] = lambda: stub_service

    client = TestClient(app)
    try:
        yield client
    finally:
        app.dependency_overrides.clear()


def test_alert_dedupe_endpoints_require_admin(alert_dedupe_client: TestClient) -> None:
    active_response = alert_dedupe_client.get("/alerts/active")
    policies_response = alert_dedupe_client.get("/alerts/policies")
    assert active_response.status_code == 403
    assert policies_response.status_code == 403


def test_alert_dedupe_endpoints_allow_admin_override(alert_dedupe_client: TestClient) -> None:
    alert_dedupe_client.app.dependency_overrides[require_admin_account] = lambda: "company"
    try:
        active_response = alert_dedupe_client.get("/alerts/active")
        policies_response = alert_dedupe_client.get("/alerts/policies")
    finally:
        alert_dedupe_client.app.dependency_overrides.pop(require_admin_account, None)

    assert active_response.status_code == 200
    assert policies_response.status_code == 200
    assert active_response.json() == [{"id": "alert"}]
    assert policies_response.json() == {"suppression_window_seconds": 600}
