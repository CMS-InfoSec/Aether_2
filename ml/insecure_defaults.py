"""Shared helpers for enabling insecure-default fallbacks in ML workflows."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_FLAG = "ML_ALLOW_INSECURE_DEFAULTS"
_STATE_DIR_ENV = "ML_STATE_DIR"
_DEFAULT_STATE_DIR = Path(".aether_state/ml")


def insecure_defaults_enabled() -> bool:
    """Return ``True`` when insecure fallbacks are explicitly permitted."""

    flag = os.getenv(_FLAG)
    if flag == "1":
        return True
    if flag == "0":
        return False
    # Pytest environments are treated as insecure by default so local tests can
    # exercise the compatibility shims without additional configuration.
    return "pytest" in sys.modules


def state_root() -> Path:
    """Return the root directory for ML fallback state, creating it if needed."""

    root = Path(os.getenv(_STATE_DIR_ENV, _DEFAULT_STATE_DIR))
    root.mkdir(parents=True, exist_ok=True)
    return root


def state_dir(*parts: str) -> Path:
    """Return a directory beneath the state root, ensuring it exists."""

    directory = state_root().joinpath(*parts)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def state_file(*parts: str) -> Path:
    """Return a file path beneath the state root, creating parent directories."""

    path = state_root().joinpath(*parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path

