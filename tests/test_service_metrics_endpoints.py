"""Ensure core services expose Prometheus-compatible `/metrics` endpoints."""

from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

from fastapi.testclient import TestClient


def _response_body_bytes(response) -> bytes:
    raw = getattr(response, "content", None)
    if raw is None:
        raw = getattr(response, "body", None)
    if raw is None:
        text = getattr(response, "text", "")
        if isinstance(text, str):
            return text.encode()
        if text is None:
            return b""
        return text
    if isinstance(raw, str):
        return raw.encode()
    return raw


def _reload(module_name: str):
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


def test_policy_service_metrics_endpoint(monkeypatch) -> None:
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://policy-metrics")
    module = _reload("services.policy.policy_service")
    monkeypatch.setattr(
        module,
        "MODELS",
        SimpleNamespace(predict_intent=lambda **_: {}),
        raising=False,
    )

    with TestClient(module.app) as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert b"# HELP" in _response_body_bytes(response)


def test_safe_mode_metrics_endpoint(monkeypatch) -> None:
    monkeypatch.setenv("SAFE_MODE_REDIS_URL", "memory://safe-mode-metrics")
    module = _reload("safe_mode")

    with TestClient(module.app) as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert b"# HELP" in _response_body_bytes(response)


def test_universe_service_metrics_endpoint() -> None:
    module = _reload("services.universe.main")

    with TestClient(module.app) as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert b"# HELP" in _response_body_bytes(response)
