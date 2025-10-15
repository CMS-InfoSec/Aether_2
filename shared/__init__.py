"""Shared utilities exposed as a namespace package with optional dependencies."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

__all__ = ["readiness", "readyz_router"]


def __getattr__(name: str) -> ModuleType:
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    try:
        module = import_module(f"{__name__}.{name}")
    except ModuleNotFoundError as exc:
        if name == "readyz_router" and exc.name and exc.name.startswith("fastapi"):
            raise ModuleNotFoundError(
                "fastapi is required to use shared.readyz_router"
            ) from exc
        raise

    globals()[name] = module
    return module


def __dir__() -> list[str]:
    return sorted(__all__)
