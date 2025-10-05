"""Utilities for shared session configuration across services."""
from __future__ import annotations

import os
from typing import Mapping


class _EnvMapping(Mapping[str, str]):
    """A mapping wrapper ensuring string access to environment variables."""

    def __init__(self, data: Mapping[str, str]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> str:
        return str(self._data[key])

    def __iter__(self):  # type: ignore[override]
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


def _coerce_env(env: Mapping[str, str] | None) -> Mapping[str, str]:
    if env is None:
        return _EnvMapping(os.environ)
    return env


def load_session_ttl_minutes(
    *, env: Mapping[str, str] | None = None, default: int = 60
) -> int:
    """Return a positive TTL in minutes for administrator sessions.

    The helper centralises parsing of the ``SESSION_TTL_MINUTES`` environment
    variable so all services enforce the same validation semantics.  Any value
    that cannot be parsed as a strictly positive integer triggers a
    ``RuntimeError`` so misconfigurations fail fast during startup instead of
    yielding sessions that expire immediately.
    """

    if default <= 0:
        raise RuntimeError("Session TTL default must be a positive integer.")

    source = _coerce_env(env)
    raw_value = source.get("SESSION_TTL_MINUTES")
    if raw_value is None:
        return default

    value = str(raw_value).strip()
    if not value:
        raise RuntimeError(
            "SESSION_TTL_MINUTES must be a positive integer representing minutes."
        )

    try:
        ttl = int(value, 10)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise RuntimeError(
            "SESSION_TTL_MINUTES must be a positive integer representing minutes."
        ) from exc

    if ttl <= 0:
        raise RuntimeError(
            "SESSION_TTL_MINUTES must be a positive integer representing minutes."
        )

    return ttl


__all__ = ["load_session_ttl_minutes"]
