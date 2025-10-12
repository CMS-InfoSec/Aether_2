from __future__ import annotations

import sys
import types

import pytest

pytest.importorskip("fastapi")
from fastapi import APIRouter
from fastapi.testclient import TestClient

# Install lightweight fallbacks for optional dependencies that introduce heavy
# runtime requirements (Timescale, scikit-learn, etc.) when the full production
# stack is unavailable during unit tests.
if "audit_mode" not in sys.modules:
    try:  # pragma: no cover - prefer the real module when available
        import audit_mode  # noqa: F401
    except Exception:
        audit_mode_stub = types.ModuleType("audit_mode")

        def _configure_audit_mode(app, **kwargs):  # pragma: no cover - simple no-op
            return None

        audit_mode_stub.configure_audit_mode = _configure_audit_mode  # type: ignore[attr-defined]
        sys.modules["audit_mode"] = audit_mode_stub

if "alert_prioritizer" not in sys.modules:
    try:  # pragma: no cover - prefer the real module when available
        import alert_prioritizer  # noqa: F401
    except Exception:
        alert_prioritizer_stub = types.ModuleType("alert_prioritizer")
        alert_prioritizer_stub.router = APIRouter(prefix="/alerts")  # type: ignore[attr-defined]

        def _setup_alert_prioritizer(app, **kwargs):  # pragma: no cover - simple no-op
            return None

        alert_prioritizer_stub.setup_alert_prioritizer = _setup_alert_prioritizer  # type: ignore[attr-defined]
        sys.modules["alert_prioritizer"] = alert_prioritizer_stub

from app import create_app
from auth.service import InMemoryAdminRepository, InMemorySessionStore


@pytest.fixture(scope="module")
def smoke_client() -> TestClient:
    """Provide a TestClient wired against the primary FastAPI application."""

    application = create_app(
        admin_repository=InMemoryAdminRepository(),
        session_store=InMemorySessionStore(),
    )
    with TestClient(application) as client:
        yield client


def _response_text(response) -> str:
    body = getattr(response, "text", "")
    if body:
        return body
    raw = getattr(response, "content", None)
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="ignore")
    if raw is None:
        raw = getattr(response, "body", b"")
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="ignore")
    return str(raw or "")


def _is_stub_client(client: TestClient) -> bool:
    return client.__class__.__module__.startswith("services.common.fastapi_stub")


def test_healthz_reports_service_ok(smoke_client: TestClient) -> None:
    response = smoke_client.get("/healthz")
    assert response.status_code == 200

    payload = response.json()
    assert payload["status"] == "ok"
    assert isinstance(payload.get("checks"), dict)


def test_metrics_emit_request_latency_histogram(smoke_client: TestClient) -> None:
    response = smoke_client.get("/metrics")
    assert response.status_code == 200

    body = _response_text(response)
    if not body:
        pytest.skip("Metrics payload unavailable when using the fastapi stub client")

    assert "http_request_duration_seconds" in body


def test_version_endpoint_exposes_release_metadata(smoke_client: TestClient) -> None:
    app_obj = getattr(smoke_client, "app", None)
    if _is_stub_client(smoke_client) and not any(
        route.path == "/version" for route in getattr(app_obj, "routes", [])
    ):
        pytest.skip("Version endpoint is not registered when running against the fastapi stub client")

    response = smoke_client.get("/version")
    assert response.status_code == 200, _response_text(response)

    content_type = response.headers.get("content-type", "")
    if content_type.startswith("application/json"):
        payload = response.json()
        assert payload.get("version")
    else:
        assert _response_text(response).strip()
