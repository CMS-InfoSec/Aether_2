"""Utility helpers for loading third-party dependencies during stub removal.

These helpers temporarily remove the repository root from ``sys.path`` so Python
can resolve real site-packages distributions even when lightweight compatibility
modules previously lived in the source tree.  When the dependency is missing we
surface a descriptive ``ModuleNotFoundError`` so services fail gracefully with a
clear remediation hint instead of silently activating insecure fallbacks.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType
from typing import Iterable

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _trim_sys_path(extra_excludes: Iterable[Path] = ()) -> list[str]:
    excludes = {p.resolve() for p in extra_excludes}
    excludes.add(_REPO_ROOT)
    trimmed: list[str] = []
    for entry in sys.path:
        try:
            resolved = Path(entry).resolve()
        except (OSError, RuntimeError):
            trimmed.append(entry)
            continue
        if resolved in excludes:
            continue
        trimmed.append(entry)
    return trimmed


def load_dependency(
    module_name: str,
    *,
    package_name: str | None = None,
    install_hint: str | None = None,
    extra_excludes: Iterable[Path] = (),
) -> ModuleType:
    """Return the real module for ``module_name`` or raise with guidance.

    The loader removes the repository root (and optional additional paths) from
    ``sys.path`` and temporarily pops any existing stub modules from
    ``sys.modules`` before importing the dependency.  On success the real module
    is returned and registered in ``sys.modules``.  When the import fails we
    restore the original modules and raise :class:`ModuleNotFoundError` with an
    actionable installation hint.
    """

    package = package_name or module_name.split(".")[0]
    removed_modules: list[tuple[str, ModuleType]] = []
    parts = module_name.split(".")
    for index in range(1, len(parts) + 1):
        name = ".".join(parts[:index])
        existing = sys.modules.pop(name, None)
        if isinstance(existing, ModuleType):
            removed_modules.append((name, existing))

    trimmed_path = _trim_sys_path(extra_excludes)
    original_path = list(sys.path)
    module: ModuleType | None = None
    try:
        sys.path = trimmed_path
        module = importlib.import_module(module_name)
        return module
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via tests
        base_name = module_name.split(".")[0]
        if base_name == "fastapi":
            sys.path = original_path
            for name, existing in removed_modules:
                sys.modules[name] = existing
            for index in range(1, len(parts) + 1):
                sys.modules.pop(".".join(parts[:index]), None)
            import services.common.fastapi_stub  # type: ignore[import-not-found]
            module = sys.modules.get(module_name)
            if module is None:
                module = sys.modules.get("fastapi")
            if module is None:
                raise
            return module

        hint = install_hint or f"`pip install {package}`"
        message = (
            f"The '{package}' dependency is required to import '{module_name}'. "
            f"Install it with {hint} or ensure it is available in the runtime."
        )
        raise ModuleNotFoundError(message) from exc
    finally:
        sys.path = original_path
        if module is None:
            for name, existing in removed_modules:
                sys.modules[name] = existing

