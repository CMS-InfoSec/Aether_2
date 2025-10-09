from __future__ import annotations

from typing import Dict

from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.health import setup_health_checks


def test_setup_health_checks_without_custom_checks() -> None:
    app = FastAPI()
    setup_health_checks(app)

    with TestClient(app) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "checks": {}}


def test_setup_health_checks_reports_success_metadata() -> None:
    app = FastAPI()

    def _cache_check() -> Dict[str, object]:
        return {"details": "warmed"}

    setup_health_checks(app, {"cache": _cache_check})

    with TestClient(app) as client:
        response = client.get("/healthz")

    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["checks"]["cache"] == {"status": "ok", "details": "warmed"}


def test_setup_health_checks_propagates_failures() -> None:
    app = FastAPI()

    def _broken_check() -> None:
        raise RuntimeError("boom")

    setup_health_checks(app, {"database": _broken_check})

    with TestClient(app) as client:
        response = client.get("/healthz")

    payload = response.json()
    assert payload["status"] == "error"
    assert payload["checks"]["database"]["status"] == "error"
    assert "boom" in payload["checks"]["database"]["error"]


def test_setup_health_checks_supports_async_checks() -> None:
    app = FastAPI()

    async def _async_check() -> Dict[str, object]:
        return {"latency_ms": 3}

    setup_health_checks(app, {"async_component": _async_check})

    with TestClient(app) as client:
        response = client.get("/healthz")

    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["checks"]["async_component"] == {
        "status": "ok",
        "latency_ms": 3,
    }
