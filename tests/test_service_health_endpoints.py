from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

from datetime import datetime, timezone

import httpx
from fastapi.testclient import TestClient

from tests.helpers.risk import risk_service_instance


def _reload(module_name: str):
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


def test_services_policy_policy_service_healthz(monkeypatch) -> None:
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://policy-health")
    module = _reload("services.policy.policy_service")
    monkeypatch.setattr(
        module,
        "MODELS",
        SimpleNamespace(predict_intent=lambda **_: {}),
        raising=False,
    )

    with TestClient(module.app) as client:
        response = client.get("/healthz")

    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["checks"]["session_store"]["status"] == "ok"
    assert payload["checks"]["policy_model"]["status"] == "ok"


def test_safe_mode_healthz(monkeypatch) -> None:
    monkeypatch.delenv("SAFE_MODE_REDIS_URL", raising=False)
    module = _reload("safe_mode")

    with TestClient(module.app) as client:
        response = client.get("/healthz")

    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["checks"]["controller"]["status"] == "ok"


def test_risk_service_healthz(tmp_path, monkeypatch) -> None:
    class _MockResponse:
        def __init__(self, payload: dict[str, object]):
            self._payload = payload
            self.status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class _MockAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def get(self, url: str, *args, **kwargs):
            if url.endswith("/allocator/status"):
                return _MockResponse({"total_nav": 1_000_000.0, "accounts": []})
            if url.endswith("/universe/approved"):
                return _MockResponse(
                    {
                        "symbols": ["BTC-USD"],
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "thresholds": {},
                    }
                )
            raise AssertionError(f"unexpected URL requested during health check: {url}")

    monkeypatch.setattr(httpx, "AsyncClient", _MockAsyncClient)
    monkeypatch.setenv("CAPITAL_ALLOCATOR_URL", "http://allocator.local")
    monkeypatch.setenv("UNIVERSE_SERVICE_URL", "http://universe.local")

    with risk_service_instance(tmp_path, monkeypatch) as module:
        with TestClient(module.app) as client:
            response = client.get("/healthz")

    payload = response.json()
    assert payload["status"] == "ok"
    checks = payload["checks"]
    assert checks["database"]["status"] == "ok"
    assert "sqlite" in checks["database"]["url"]
    assert checks["capital_allocator"]["status"] == "ok"
    assert checks["capital_allocator"]["configured"] is True
    assert checks["universe_service"]["status"] == "ok"
    assert checks["universe_service"]["symbol_count"] == 1
