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

from importlib.machinery import ModuleSpec
from pathlib import Path
from typing import Iterable

import sys

__all__: list[str] = []

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_TESTS_DIRS: Iterable[Path] = (
    _PROJECT_ROOT / "tests" / "services",
)

_locations = list(__path__)  # type: ignore[name-defined]
for candidate in _TESTS_DIRS:
    if candidate.is_dir():
        location = str(candidate)
        if location not in _locations:
            _locations.append(location)

__path__ = _locations  # type: ignore[assignment]

spec: ModuleSpec | None = globals().get("__spec__")
if spec is not None:
    spec.submodule_search_locations = list(_locations)

if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
