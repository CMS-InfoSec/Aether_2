
"""Analytics service modules.

The regression suite occasionally reloads ``services.analytics`` using
``importlib.util.spec_from_file_location`` without providing
``submodule_search_locations``.  When that happens Python treats the
resulting module as a plain module rather than a package, preventing
submodule imports such as ``services.analytics.whale_detector``.

To keep those imports reliable we normalise ``__path__`` here and attach
the package directory to the module spec when it is available.
"""

from __future__ import annotations

import importlib
import sys
from importlib.machinery import ModuleSpec
from pathlib import Path
from typing import List


def _normalise_search_path() -> List[str]:
    locations: List[str] = []
    existing = globals().get("__path__")
    if existing:
        locations.extend(existing)  # type: ignore[arg-type]

    package_dir = str(Path(__file__).resolve().parent)
    if package_dir not in locations:
        locations.append(package_dir)

    return locations


_locations = _normalise_search_path()
__path__ = _locations  # type: ignore[assignment]

spec: ModuleSpec | None = globals().get("__spec__")
if spec is not None:
    spec.submodule_search_locations = list(_locations)


def __getattr__(name: str):
    """Lazily import analytics submodules when accessed via ``fromlist``."""

    fullname = f"{__name__}.{name}"
    try:
        module = importlib.import_module(fullname)
    except ModuleNotFoundError as exc:  # pragma: no cover - passthrough for typos
        raise AttributeError(name) from exc

    setattr(sys.modules[__name__], name, module)
    return module

