"""Delegate to the upstream Starlette package with informative errors."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_starlette = load_dependency(
    "starlette", install_hint="pip install starlette"
)

globals().update(vars(_real_starlette))
