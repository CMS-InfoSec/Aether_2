"""Proxy to the real :mod:`gevent` package."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_gevent = load_dependency("gevent", install_hint="pip install gevent")

globals().update(vars(_real_gevent))
