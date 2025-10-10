"""Compatibility helpers for the ``ml.policy`` namespace."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, List

__all__ = [
    "fallback_policy",
    "meta_strategy",
]

_PACKAGE_ROOT = Path(__file__).resolve().parent
_TESTS_DIRS: Iterable[Path] = (
    _PACKAGE_ROOT.parent.parent / "tests" / "ml" / "policy",
)


def _build_search_path() -> List[str]:
    locations: List[str] = []
    existing = globals().get("__path__")
    if existing:
        locations.extend(existing)  # type: ignore[arg-type]

    canonical = str(_PACKAGE_ROOT)
    if canonical not in locations:
        locations.append(canonical)

    for candidate in _TESTS_DIRS:
        if candidate.is_dir():
            path = str(candidate)
            if path not in locations:
                locations.append(path)

    return locations


_locations = _build_search_path()
__path__ = _locations  # type: ignore[assignment]

spec: ModuleSpec | None = globals().get("__spec__")
if spec is not None:
    spec.submodule_search_locations = list(_locations)


def __getattr__(name: str) -> Any:
    if name in __all__:
        module = importlib.import_module(f"{__name__}.{name}")
        setattr(sys.modules[__name__], name, module)
        return module
    module = _load_test_mirror(name)
    if module is not None:
        setattr(sys.modules[__name__], name, module)
        return module
    raise AttributeError(name)


def _load_test_mirror(name: str) -> ModuleType | None:
    tests_root = _PACKAGE_ROOT.parent.parent / "tests" / "ml" / "policy"
    module_path = tests_root / f"{name}.py"
    if module_path.exists():
        spec = importlib.util.spec_from_file_location(f"tests.ml.policy.{name}", module_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            sys.modules.setdefault(f"tests.ml.policy.{name}", module)
            spec.loader.exec_module(module)
            return module
    return None


def __dir__() -> List[str]:  # pragma: no cover - introspection helper
    return sorted(set(__all__) | set(globals()))
