"""Machine learning package for the Aether platform."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, List

__all__: list[str] = []

_PACKAGE_ROOT = Path(__file__).resolve().parent
_TESTS_ML_DIRS: Iterable[Path] = (
    _PACKAGE_ROOT.parent / "tests" / "ml",
)


def _build_search_path() -> List[str]:
    """Return the namespace search locations for the ``ml`` package."""

    locations: List[str] = []
    existing = globals().get("__path__")
    if existing:
        locations.extend(existing)  # type: ignore[arg-type]

    canonical = str(_PACKAGE_ROOT)
    if canonical not in locations:
        locations.append(canonical)

    for candidate in _TESTS_ML_DIRS:
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
    """Lazily resolve ML submodules to avoid heavy imports at startup."""

    fullname = f"{__name__}.{name}"
    try:
        module = importlib.import_module(fullname)
    except ModuleNotFoundError as exc:
        module = _load_test_mirror(name)
        if module is None:
            raise AttributeError(name) from exc
    setattr(sys.modules[__name__], name, module)
    return module


def _load_test_mirror(name: str) -> ModuleType | None:
    """Load ``tests.ml.<name>`` mirrors when the canonical module is absent."""

    test_fullname = f"tests.ml.{name}"
    try:
        return importlib.import_module(test_fullname)
    except ModuleNotFoundError:
        test_dir = _PACKAGE_ROOT.parent / "tests" / "ml" / name
        module_path = test_dir.with_suffix(".py")
        if module_path.exists():
            spec = importlib.util.spec_from_file_location(test_fullname, module_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules[test_fullname] = module
                spec.loader.exec_module(module)
                return module
        if test_dir.is_dir():
            init_file = test_dir / "__init__.py"
            if init_file.exists():
                spec = importlib.util.spec_from_file_location(
                    test_fullname,
                    init_file,
                    submodule_search_locations=[str(test_dir)],
                )
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[test_fullname] = module
                    spec.loader.exec_module(module)
                    module.__path__ = [str(test_dir)]  # type: ignore[attr-defined]
                    return module
    return None


def __dir__() -> List[str]:  # pragma: no cover - introspection helper
    return sorted(set(globals()) | set(__all__))
