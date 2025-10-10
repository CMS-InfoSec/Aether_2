"""Services package initialiser.

The production code expects to import modules directly from ``services`` while
the regression suite mirrors a subset of those modules beneath
``tests/services``.  When pytest collects the fixtures it attempts imports such
as ``services.test_alert_manager`` which should resolve to the mirror modules.

Historically this wiring lived in ``tests/services/__init__.py`` and a
``sitecustomize`` hook, but that arrangement depended on import order.  If a
test imported ``services.test_*`` before the helper executed Python would only
see the real package paths and the import would fail.

To keep the behaviour deterministic we extend ``__path__`` here so the
regression mirrors are always available once the package is imported.  The
production modules remain the first entries on the search path, preserving the
expected resolution order.
"""

from __future__ import annotations

import importlib
import importlib.util
from importlib.machinery import ModuleSpec
from pathlib import Path
from typing import Iterable, List

from types import ModuleType

import sys

__all__: list[str] = []

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_PACKAGE_PATH = str(Path(__file__).resolve().parent)
_TESTS_DIRS: Iterable[Path] = (
    _PROJECT_ROOT / "tests" / "services",
)

def _build_search_path() -> List[str]:
    """Return the list of search locations for the ``services`` namespace."""

    locations: List[str] = []

    existing = globals().get("__path__")
    if existing:
        locations.extend(existing)  # type: ignore[arg-type]

    if _DEFAULT_PACKAGE_PATH not in locations:
        locations.append(_DEFAULT_PACKAGE_PATH)

    for candidate in _TESTS_DIRS:
        if candidate.is_dir():
            location = str(candidate)
            if location not in locations:
                locations.append(location)

    return locations


_locations = _build_search_path()
__path__ = _locations  # type: ignore[assignment]

spec: ModuleSpec | None = globals().get("__spec__")
if spec is not None:
    spec.submodule_search_locations = list(_locations)

if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

sys.modules.setdefault("services_real", sys.modules[__name__])


def _register_test_mirrors() -> None:
    """Expose ``tests/services`` modules under the ``services`` namespace."""

    tests_dir = _PROJECT_ROOT / "tests" / "services"
    if not tests_dir.is_dir():
        return

    for path in sorted(tests_dir.glob("test_*.py")):
        alias = f"{__name__}.{path.stem}"
        if alias in sys.modules:
            continue

        spec = importlib.util.spec_from_file_location(alias, path)
        if spec is None or spec.loader is None:
            continue

        module = importlib.util.module_from_spec(spec)
        try:
            sys.modules[alias] = module
            spec.loader.exec_module(module)
        except Exception:  # pragma: no cover - optional deps may be absent
            sys.modules.pop(alias, None)
            continue


_register_test_mirrors()


def __getattr__(name: str) -> ModuleType:
    """Lazily import ``services.<name>`` and cache the submodule."""

    fullname = f"{__name__}.{name}"
    try:
        module = importlib.import_module(fullname)
    except ModuleNotFoundError as exc:  # pragma: no cover - mirror module absent
        module = _load_test_mirror(name, fullname)
        if module is None:
            raise AttributeError(name) from exc
    else:
        if not _is_canonical_module(module):
            replacement = _load_canonical_module(fullname)
            if replacement is not None:
                module = replacement
    setattr(sys.modules[__name__], name, module)
    return module


def _is_canonical_module(module: ModuleType) -> bool:
    module_file = getattr(module, "__file__", None)
    if module_file is None:
        return False
    return str(module_file).startswith(_DEFAULT_PACKAGE_PATH)


def _load_canonical_module(fullname: str) -> ModuleType | None:
    module_path = _PROJECT_ROOT / (fullname.replace(".", "/") + ".py")
    if not module_path.exists():
        return None
    spec = importlib.util.spec_from_file_location(fullname, module_path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    spec.loader.exec_module(module)
    return module


def _load_test_mirror(name: str, fullname: str) -> ModuleType | None:
    module: ModuleType | None = None
    test_fullname = f"tests.services.{name}"
    try:
        module = importlib.import_module(test_fullname)
    except ModuleNotFoundError:
        test_dir = _PROJECT_ROOT / "tests" / "services" / name
        module_path = test_dir.with_suffix(".py")
        if module_path.exists():
            spec = importlib.util.spec_from_file_location(fullname, module_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules[fullname] = module
                spec.loader.exec_module(module)
        elif test_dir.is_dir():
            init_file = test_dir / "__init__.py"
            if init_file.exists():
                spec = importlib.util.spec_from_file_location(
                    fullname, init_file, submodule_search_locations=[str(test_dir)]
                )
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[fullname] = module
                    spec.loader.exec_module(module)
                    module.__path__ = [str(test_dir)]  # type: ignore[attr-defined]
    return module
