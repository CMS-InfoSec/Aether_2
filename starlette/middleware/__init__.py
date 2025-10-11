"""Proxy to :mod:`starlette.middleware`."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_middleware = load_dependency(
    "starlette.middleware", install_hint="pip install starlette"
)

globals().update(vars(_real_middleware))
