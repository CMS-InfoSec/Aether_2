from __future__ import annotations

import importlib

import pytest
from fastapi import HTTPException

pytest.importorskip("pandas", exc_type=ImportError)

import report_service as module


@pytest.fixture(autouse=True)
def _reload_module(monkeypatch: pytest.MonkeyPatch):
    for key in ("REPORT_DATABASE_URL", "TIMESCALE_DSN", "DATABASE_URL"):
        monkeypatch.delenv(key, raising=False)
    yield
    importlib.reload(module)


def test_database_url_requires_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REPORT_DATABASE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(HTTPException) as excinfo:
        module._database_url()

    assert excinfo.value.status_code == 503
    assert "Report service database URL is not configured" in excinfo.value.detail


def test_database_url_normalizes_timescale_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPORT_DATABASE_URL", "timescale://user:pass@host:5432/db")

    resolved = module._database_url()

    assert resolved == "postgresql://user:pass@host:5432/db"


def test_database_url_rejects_sqlite(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPORT_DATABASE_URL", "sqlite:///tmp/reports.db")

    with pytest.raises(HTTPException) as excinfo:
        module._database_url()

    assert excinfo.value.status_code == 500
    assert "PostgreSQL/Timescale" in excinfo.value.detail
