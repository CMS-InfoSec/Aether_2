"""Proxy to :mod:`cryptography.fernet`."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_fernet = load_dependency(
    "cryptography.fernet", install_hint="pip install cryptography"
)

globals().update(vars(_real_fernet))
