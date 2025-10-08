"""Typed helpers for optional PyYAML imports."""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Iterable, Optional, Protocol, runtime_checkable


@runtime_checkable
class YamlModule(Protocol):
    """Subset of the :mod:`yaml` API used within the codebase."""

    def safe_load_all(self, stream: str) -> Iterable[object]:
        """Yield documents from *stream* using PyYAML's safe loader."""


def load_yaml_module() -> Optional[YamlModule]:
    """Return the PyYAML module if it is available."""

    try:
        module = importlib.import_module("yaml")
    except ModuleNotFoundError:
        return None

    if isinstance(module, ModuleType) and isinstance(module, YamlModule):
        return module
    return None

