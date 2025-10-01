"""Proxy module to expose the standard-library secrets helpers for tests."""

from __future__ import annotations

import sys
import sysconfig
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

_stdlib_dir = Path(sysconfig.get_paths()["stdlib"])
_spec = spec_from_file_location("_stdlib_secrets", _stdlib_dir / "secrets.py")
if _spec is None or _spec.loader is None:  # pragma: no cover - defensive guard
    raise ImportError("Unable to locate standard library secrets module")
_stdlib_module = module_from_spec(_spec)
_spec.loader.exec_module(_stdlib_module)

__all__ = getattr(_stdlib_module, "__all__", [])  # type: ignore[assignment]
for name in __all__:
    globals()[name] = getattr(_stdlib_module, name)

# Ensure commonly used attributes are available even if not listed in __all__.
for attr in ("SystemRandom", "choice", "randbelow", "randbits", "token_hex"):
    if hasattr(_stdlib_module, attr):
        globals()[attr] = getattr(_stdlib_module, attr)

globals()["__doc__"] = getattr(_stdlib_module, "__doc__")
globals()["__spec__"] = getattr(_stdlib_module, "__spec__")

# Register module so downstream imports retrieve this proxy with stdlib behaviour.
sys.modules["secrets"] = sys.modules[__name__]
