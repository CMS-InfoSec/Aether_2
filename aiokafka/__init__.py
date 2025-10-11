"""Proxy to the real :mod:`aiokafka` package."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_aiokafka = load_dependency(
    "aiokafka", install_hint="pip install aiokafka"
)

globals().update(vars(_real_aiokafka))
