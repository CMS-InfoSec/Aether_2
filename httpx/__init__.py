"""Proxy to the real :mod:`httpx` package."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_httpx = load_dependency("httpx", install_hint="pip install httpx")

globals().update(vars(_real_httpx))
