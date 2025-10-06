"""Pytest session hooks applied across the entire repository.

This module ensures the repository root is present on ``sys.path`` when the
suite is executed via the ``pytest`` entrypoint.  Several of the production
services (for example ``services.system.health_service``) live at the repository
root rather than underneath the ``src/`` package directory.  When ``pytest`` is
invoked directly (the most common pattern in CI), Python initialises
``sys.path[0]`` with the location of the ``pytest`` console script instead of
the project directory, causing imports such as ``import services.system`` to
fail with ``ModuleNotFoundError``.

By explicitly prepending the repository root to ``sys.path`` we guarantee the
behaviour matches ``python -m pytest`` and the running application, preventing
spurious import failures during the readiness checks.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_repo_root_on_path() -> None:
    """Add the repository root to ``sys.path`` if it is missing."""

    repo_root = Path(__file__).resolve().parent
    repo_str = str(repo_root)
    if repo_str not in sys.path:
        sys.path.insert(0, repo_str)


_ensure_repo_root_on_path()
