"""Minimal Starlette compatibility layer for tests."""

from __future__ import annotations

from services.common.fastapi_stub import status as _status

status = _status

__all__ = ["status"]
