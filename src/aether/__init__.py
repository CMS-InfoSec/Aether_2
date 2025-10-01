"""Top level package for the Aether platform services."""

from __future__ import annotations

from importlib import metadata as _metadata

__all__ = ["get_version"]


def get_version() -> str:
    """Return the installed package version.

    When running from a source checkout (editable install) the version metadata is
    available via :mod:`importlib.metadata`.  If the package has not been installed the
    lookup gracefully falls back to the declared project version.
    """

    try:
        return _metadata.version("aether")
    except _metadata.PackageNotFoundError:  # pragma: no cover - only hit in dev shells
        return "0.1.0"
