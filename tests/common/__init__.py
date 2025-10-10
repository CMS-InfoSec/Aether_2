"""Allow the test ``common`` package to proxy the real project modules."""

from __future__ import annotations

from importlib.machinery import ModuleSpec
from pathlib import Path

import sys

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_COMMON_DIR = _PROJECT_ROOT / "common"

if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

if str(_COMMON_DIR) not in __path__:
    __path__.append(str(_COMMON_DIR))  # type: ignore[attr-defined]

spec: ModuleSpec | None = globals().get("__spec__")
if spec is not None:
    locations = list(spec.submodule_search_locations or [])
    if str(_COMMON_DIR) not in locations:
        locations.append(str(_COMMON_DIR))
        spec.submodule_search_locations = locations
