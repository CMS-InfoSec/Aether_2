"""Compatibility wrapper exposing :mod:`fastapi.testclient` from the local stub."""

from __future__ import annotations

from . import _ensure_stub

_stub = _ensure_stub()
TestClient = getattr(_stub, "TestClient")

__all__ = ["TestClient"]

