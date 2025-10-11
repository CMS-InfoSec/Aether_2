"""Proxy to :mod:`starlette.datastructures` from the real package."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_datastructures = load_dependency(
    "starlette.datastructures", install_hint="pip install starlette"
)

globals().update(vars(_real_datastructures))
