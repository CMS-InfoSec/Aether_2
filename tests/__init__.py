"""Ensure the repository root is importable when tests run as packages."""

from __future__ import annotations

import importlib
import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_ROOT)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# Preload the primary packages used across the suite so dotted imports succeed
# even when pytest collects tests as a package.
for _package in ("services", "shared", "ml"):
    try:  # pragma: no cover - defensive in case optional packages are missing
        importlib.import_module(_package)
    except ModuleNotFoundError:
        continue

__all__ = []

