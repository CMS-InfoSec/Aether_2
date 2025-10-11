"""Proxy to :mod:`aiokafka.errors`."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_errors = load_dependency(
    "aiokafka.errors", install_hint="pip install aiokafka"
)

globals().update(vars(_real_errors))
