"""Proxy to the real :mod:`prometheus_client` package."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_prometheus = load_dependency(
    "prometheus_client", install_hint="pip install prometheus-client"
)

globals().update(vars(_real_prometheus))
