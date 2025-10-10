"""Order management service package.

Pytest fixtures occasionally reload ``services.oms`` using
``spec_from_file_location`` without specifying search locations.  When that
happens Python treats the loaded object as a regular module, making imports such
as ``services.oms.kraken_rest`` fail.  We normalise ``__path__`` here so the
package remains importable regardless of how it is loaded.

Historically a number of modules imported the package using
``from services.oms import reconcile`` or similar.  When pytest swaps in
temporary stubs this implicit re-export breaks, leaving the package without the
expected attributes and causing import errors across the OMS stack.  We lazily
expose the canonical submodules through ``__all__`` so the package behaves
consistently even when the import machinery has been customised by the tests
while avoiding circular imports during start-up.
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


_SUBMODULES = {
    "circuit_breaker_store": ".circuit_breaker_store",
    "error_handler": ".error_handler",
    "idempotency_backend": ".idempotency_backend",
    "idempotency_store": ".idempotency_store",
    "impact_store": ".impact_store",
    "kraken_client": ".kraken_client",
    "kraken_rest": ".kraken_rest",
    "kraken_ws": ".kraken_ws",
    "main": ".main",
    "oms_service": ".oms_service",
    "reconcile": ".reconcile",
    "routing": ".routing",
    "shadow_oms": ".shadow_oms",
    "sim_broker": ".sim_broker",
    "warm_start": ".warm_start",
}

__all__ = list(_SUBMODULES)


def __getattr__(name: str):  # pragma: no cover - exercised indirectly by imports
    if name in _SUBMODULES:
        from importlib import import_module

        module = import_module(f"{__name__}{_SUBMODULES[name]}")
        globals()[name] = module
        return module
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
