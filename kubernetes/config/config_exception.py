"""Exception types mirroring the real Kubernetes config module."""

from __future__ import annotations

__all__ = ["ConfigException"]


class ConfigException(Exception):
    """Raised when Kubernetes configuration cannot be loaded."""

    pass
