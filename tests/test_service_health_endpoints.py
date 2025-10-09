from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

from fastapi.testclient import TestClient


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
