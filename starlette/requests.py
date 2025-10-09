"""Starlette request compatibility shims for environments without Starlette."""

from __future__ import annotations

from services.common.fastapi_stub import Request as _Request

__all__ = ["Request"]

Request = _Request
