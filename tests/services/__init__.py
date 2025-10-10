"""Expose the real ``services`` package when pytest collects fixtures."""

from __future__ import annotations

from importlib.machinery import ModuleSpec
from pathlib import Path

import sys

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SERVICES_DIR = _PROJECT_ROOT / "services"

if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

if str(_SERVICES_DIR) not in __path__:
    __path__.append(str(_SERVICES_DIR))  # type: ignore[attr-defined]

spec: ModuleSpec | None = globals().get("__spec__")
if spec is not None:
    locations = list(spec.submodule_search_locations or [])
    if str(_SERVICES_DIR) not in locations:
        locations.append(str(_SERVICES_DIR))
        spec.submodule_search_locations = locations
