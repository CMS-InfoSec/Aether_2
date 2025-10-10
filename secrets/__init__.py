"""Compatibility shim for the standard library :mod:`secrets` module.

This repository ships several regression tests under ``tests/secrets`` that
expect to import modules such as ``secrets.test_authorization`` while still
retaining the full API of Python's standard :mod:`secrets` implementation.  The
runtime environment used for these tests does not automatically execute a
``sitecustomize`` module, so delegating to that hook is insufficient.  Instead
we provide a tiny package that loads the standard library module under a
private name, re-exports its public interface, and then exposes the
``tests/secrets`` directory as a package path so the regression modules can be
discovered.
"""

from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable
import sysconfig

_STDLIB_SECRETS_PATH = Path(sysconfig.get_path("stdlib")) / "secrets.py"
_STDLIB_SPEC = spec_from_file_location("_stdlib_secrets", _STDLIB_SECRETS_PATH)
if _STDLIB_SPEC is None or _STDLIB_SPEC.loader is None:  # pragma: no cover - defensive guard
    raise ImportError("Unable to load the standard library secrets module")

_stdlib_secrets = module_from_spec(_STDLIB_SPEC)
assert isinstance(_stdlib_secrets, ModuleType)  # narrow type for mypy
_STDLIB_SPEC.loader.exec_module(_stdlib_secrets)  # type: ignore[call-arg]

# Re-export the public interface provided by the standard module.
__all__: list[str] = list(getattr(_stdlib_secrets, "__all__", ()))
for name in __all__:
    globals()[name] = getattr(_stdlib_secrets, name)

# Preserve other documented attributes such as ``__doc__`` and ``SystemRandom``.
for name, value in _stdlib_secrets.__dict__.items():
    if name in {"__all__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}:
        continue
    if name not in globals():
        globals()[name] = value

__doc__ = getattr(_stdlib_secrets, "__doc__", __doc__)


def __getattr__(name: str) -> Any:
    """Delegate attribute access to the wrapped standard library module."""

    try:
        return getattr(_stdlib_secrets, name)
    except AttributeError as exc:  # pragma: no cover - mirrors stdlib behaviour
        raise AttributeError(name) from exc


def __dir__() -> Iterable[str]:  # pragma: no cover - simple proxy
    return sorted(set(__all__) | set(dir(_stdlib_secrets)))


# Make the regression modules available as ``secrets.test_*`` imports.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_TESTS_SECRETS_DIR = _PROJECT_ROOT / "tests" / "secrets"
__path__: list[str] = []
if _TESTS_SECRETS_DIR.is_dir():
    __path__.append(str(_TESTS_SECRETS_DIR))
    if __spec__ is not None:
        __spec__.submodule_search_locations = list(__path__)  # type: ignore[attr-defined]

