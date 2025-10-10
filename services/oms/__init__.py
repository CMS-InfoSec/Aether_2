"""Order management service package.

Pytest fixtures occasionally reload ``services.oms`` using
``spec_from_file_location`` without specifying search locations.  When that
happens Python treats the loaded object as a regular module, making imports such
as ``services.oms.kraken_rest`` fail.  We normalise ``__path__`` here so the
package remains importable regardless of how it is loaded.
"""

from __future__ import annotations

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


__path__ = _normalise_search_path()  # type: ignore[assignment]

spec: ModuleSpec | None = globals().get("__spec__")
if spec is not None:
    spec.submodule_search_locations = list(__path__)
