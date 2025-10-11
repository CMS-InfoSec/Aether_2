"""Proxy to the real :mod:`websockets` package."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_websockets = load_dependency(
    "websockets", install_hint="pip install websockets"
)

globals().update(vars(_real_websockets))
