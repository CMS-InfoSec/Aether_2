from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from auth.service import InMemoryAdminRepository

from app import create_app
from auth.service import InMemorySessionStore
from logging_export import ExportResult


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:

    app = create_app(admin_repository=InMemoryAdminRepository())

    with TestClient(app) as test_client:
        yield test_client


def test_export_status_returns_empty_payload(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    monkeypatch.setattr("services.logging_export.latest_export", lambda: None)

    response = client.get("/logging/export/status")

    assert response.status_code == 200
    assert response.json() == {"last_export": None, "hash": None}


def test_export_status_returns_latest_metadata(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    export_date = dt.date(2024, 1, 1)
    exported_at = dt.datetime(2024, 1, 2, 0, 5, tzinfo=dt.timezone.utc)
    result = ExportResult(
        export_date=export_date,
        exported_at=exported_at,
        s3_bucket="exports",
        s3_key="log-exports/audit-reg-log-2024-01-01.json.gz",
        sha256="abc123",
    )
    monkeypatch.setattr("services.logging_export.latest_export", lambda: result)

    response = client.get("/logging/export/status")

    assert response.status_code == 200
    assert response.json() == {
        "last_export": exported_at.isoformat(),
        "hash": "abc123",
    }


def test_seconds_until_midnight(monkeypatch: pytest.MonkeyPatch) -> None:
    from daily_export import _seconds_until_midnight_utc

    now = dt.datetime(2024, 6, 1, 23, 30, tzinfo=dt.timezone.utc)
    seconds = _seconds_until_midnight_utc(now)

    assert pytest.approx(seconds, rel=1e-6) == 30 * 60


def test_resolve_export_date_defaults_to_yesterday(monkeypatch: pytest.MonkeyPatch) -> None:
    from daily_export import _resolve_export_date

    today = dt.datetime(2024, 6, 2, 0, 5, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(dt, "datetime", type("_dt", (), {"now": staticmethod(lambda tz=None: today)}))

    resolved = _resolve_export_date(None)

    assert resolved == dt.date(2024, 6, 1)

