"""Compatibility wrapper exposing :mod:`fastapi.testclient` from the local stub."""

from __future__ import annotations

from . import _ensure_stub

_stub = _ensure_stub()
try:
    TestClient = getattr(_stub, "TestClient")
except AttributeError:  # pragma: no cover - defensive fallback for stub races
    try:
        from services.common.fastapi_stub import TestClient as _fallback
    except Exception as exc:  # pragma: no cover - surface a descriptive import error
        raise ImportError("FastAPI TestClient is unavailable") from exc
    else:
        TestClient = _fallback

__all__ = ["TestClient"]

