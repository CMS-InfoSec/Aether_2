"""Minimal Starlette datastructures for middleware stubs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

__all__ = ["Headers"]


@dataclass
class Headers:
    """Simplified representation of the ASGI headers mapping."""

    items: Mapping[str, str] | Iterable[tuple[str, str]]

    def get(self, key: str, default: str | None = None) -> str | None:
        if isinstance(self.items, Mapping):
            return self.items.get(key, default)
        for candidate, value in self.items:
            if candidate.lower() == key.lower():
                return value
        return default
