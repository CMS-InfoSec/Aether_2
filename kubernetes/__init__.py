"""Lightweight Kubernetes client shim used in dependency-light environments.

This stub provides just enough of the ``kubernetes`` package to allow the
secrets service and its associated tests to run without the real dependency.
The implementation focuses on the configuration helpers and ``CoreV1Api``
subset that the application exercises when persisting and reading secrets.
"""

from __future__ import annotations

from . import config  # noqa: F401  (re-exported for callers expecting submodules)
from . import client  # noqa: F401

__all__ = ["client", "config"]
