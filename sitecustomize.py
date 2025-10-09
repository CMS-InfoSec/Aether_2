"""Runtime patches ensuring unit tests import the project secrets package safely."""
from __future__ import annotations

import importlib
from importlib.machinery import ModuleSpec
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent
_TESTS_SECRETS_DIR = _PROJECT_ROOT / "tests" / "secrets"


def _extend_stdlib_secrets_namespace() -> None:
    """Allow ``secrets.test_*`` imports without shadowing the stdlib module."""

    if not _TESTS_SECRETS_DIR.is_dir():
        return

    stdlib_secrets = importlib.import_module("secrets")
    locations = [str(_TESTS_SECRETS_DIR)]

    setattr(stdlib_secrets, "__path__", locations)

    spec: ModuleSpec | None = getattr(stdlib_secrets, "__spec__", None)
    if spec is not None:
        spec.submodule_search_locations = locations  # type: ignore[attr-defined]


_extend_stdlib_secrets_namespace()
