"""Expose the real ``services`` package when pytest collects fixtures."""

from __future__ import annotations

import importlib
import importlib.util
from importlib.machinery import ModuleSpec
from pathlib import Path

import sys

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SERVICES_DIR = _PROJECT_ROOT / "services"

if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

_REAL_SPEC = importlib.util.spec_from_file_location(
    "services",
    _SERVICES_DIR / "__init__.py",
    submodule_search_locations=[str(_SERVICES_DIR)],
)

_REAL_SERVICES = None
if _REAL_SPEC is not None and _REAL_SPEC.loader is not None:
    module = importlib.util.module_from_spec(_REAL_SPEC)
    # Pre-register the module so ``services/__init__.py`` can resolve
    # ``sys.modules[__name__]`` while it initialises.  Python inserts modules
    # into ``sys.modules`` before executing them during normal imports; we
    # replicate that behaviour here to avoid ``KeyError`` when the production
    # package accesses its own entry.
    sys.modules.setdefault("services", module)
    try:
        _REAL_SPEC.loader.exec_module(module)
    except Exception:
        # If the import fails, clean up the partially initialised entry so
        # pytest does not retain a broken module reference.
        sys.modules.pop("services", None)
        raise
    _REAL_SERVICES = module  # type: ignore[assignment]
    sys.modules["services"] = module
    sys.modules.setdefault("services_real", module)
    globals().update(module.__dict__)
    __path__ = list(module.__path__)  # type: ignore[assignment]

__all__: list[str] = []


def __getattr__(name: str):
    if _REAL_SERVICES is not None:
        try:
            return getattr(_REAL_SERVICES, name)
        except AttributeError:
            pass
    raise AttributeError(name)

if str(_SERVICES_DIR) not in __path__:
    __path__.append(str(_SERVICES_DIR))  # type: ignore[attr-defined]

spec: ModuleSpec | None = globals().get("__spec__")
if spec is not None:
    locations = list(spec.submodule_search_locations or [])
    if str(_SERVICES_DIR) not in locations:
        locations.append(str(_SERVICES_DIR))
        spec.submodule_search_locations = locations


def _register_test_mirrors() -> None:
    """Expose ``tests.services`` modules through the ``services`` namespace."""

    tests_dir = Path(__file__).resolve().parent
    try:
        importlib.import_module("services.core")
    except Exception:
        pass
    for path in tests_dir.glob("test_*.py"):
        module_name = path.stem
        alias = f"services.{module_name}"
        if alias in sys.modules:
            continue

        real_name = f"{__name__}.{module_name}"
        try:
            module = importlib.import_module(real_name)
        except Exception:
            # Optional dependencies (prometheus, aiohttp, etc.) can cause the
            # mirror imports to raise during registration.  Skip those modules;
            # pytest will report the skip or failure when it imports the test
            # directly.
            continue
        sys.modules[alias] = module


_register_test_mirrors()
