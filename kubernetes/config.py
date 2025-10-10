"""Minimal configuration helpers mirroring the real Kubernetes client API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = [
    "ConfigException",
    "Configuration",
    "load_incluster_config",
    "load_kube_config",
]


class ConfigException(Exception):
    """Raised when configuration loading fails in the real client."""


@dataclass
class Configuration:
    """Very small subset of the Kubernetes ``Configuration`` object."""

    verify_ssl: bool = True

    @classmethod
    def get_default_copy(cls) -> "Configuration":
        return cls()


def load_incluster_config(*_: Any, **__: Any) -> None:
    """Mimic the real helper by silently succeeding when called."""

    return None


def load_kube_config(*_: Any, **__: Any) -> None:
    """Mimic the real helper by silently succeeding when called."""

    return None
