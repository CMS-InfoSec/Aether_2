"""Service layer scaffolding for the Aether platform.

This module collects shared service wiring that can be imported by individual service
implementations during development.  Keeping the module in ``src/`` ensures editable
installs expose a stable import path (``aether.services``).
"""

from __future__ import annotations

from typing import Protocol

__all__ = ["Service"]


class Service(Protocol):
    """Protocol representing a long-running service component."""

    name: str

    def start(self) -> None:
        """Start the service."""

    def stop(self) -> None:
        """Stop the service and release resources."""
