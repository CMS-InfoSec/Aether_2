"""Runtime patches ensuring unit tests import the project secrets package safely."""
from __future__ import annotations

import importlib
import sys
from importlib.machinery import ModuleSpec
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent
_TESTS_SECRETS_DIR = _PROJECT_ROOT / "tests" / "secrets"


def _ensure_project_root_on_path() -> None:
    """Guarantee the repository root is importable before pytest stubs load."""

    root = str(_PROJECT_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)


def _ensure_services_namespace() -> None:
    """Attach the project services package to pytest stubs missing search paths."""

    services_module = sys.modules.get("services")
    if services_module is None:
        return

    services_path = str(_PROJECT_ROOT / "services")
    locations = list(getattr(services_module, "__path__", []) or [])
    if services_path not in locations:
        locations.append(services_path)
        services_module.__path__ = locations  # type: ignore[attr-defined]

    spec: ModuleSpec | None = getattr(services_module, "__spec__", None)
    if spec is not None:
        spec.submodule_search_locations = locations  # type: ignore[attr-defined]


def _ensure_common_namespace() -> None:
    """Attach the project common package to pytest stubs missing search paths."""

    common_module = sys.modules.get("common")
    if common_module is None:
        return

    common_path = str(_PROJECT_ROOT / "common")
    locations = list(getattr(common_module, "__path__", []) or [])
    if common_path not in locations:
        locations.append(common_path)
        common_module.__path__ = locations  # type: ignore[attr-defined]

    spec: ModuleSpec | None = getattr(common_module, "__spec__", None)
    if spec is not None:
        spec.submodule_search_locations = locations  # type: ignore[attr-defined]


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
_ensure_project_root_on_path()
_ensure_services_namespace()
_ensure_common_namespace()
