"""Expose the real ``services`` package when pytest collects fixtures."""

from __future__ import annotations

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
    _REAL_SPEC.loader.exec_module(module)
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
