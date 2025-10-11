"""Proxy to the real :mod:`cryptography` package."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_crypto = load_dependency(
    "cryptography", install_hint="pip install cryptography"
)

globals().update(vars(_real_crypto))
