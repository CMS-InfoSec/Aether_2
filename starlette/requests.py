"""Proxy to :mod:`starlette.requests`."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_requests = load_dependency(
    "starlette.requests", install_hint="pip install starlette"
)

globals().update(vars(_real_requests))
