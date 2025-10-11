"""Proxy to :mod:`starlette.middleware.trustedhost`."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_trustedhost = load_dependency(
    "starlette.middleware.trustedhost", install_hint="pip install starlette"
)

globals().update(vars(_real_trustedhost))
