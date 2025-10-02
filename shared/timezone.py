"""Utilities for formatting timestamps in Europe/London with DST awareness."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from zoneinfo import ZoneInfo

_LONDON_TZ = ZoneInfo("Europe/London")


def _coerce_datetime(value: Any) -> datetime:
    """Convert *value* to :class:`datetime`.

    ``pandas.Timestamp`` and ``numpy.datetime64`` instances expose ``to_pydatetime``
    which we invoke when available so downstream code can operate on the standard
    library ``datetime`` type. Values that are already ``datetime`` instances are
    returned as-is. For naive datetimes we assume they are expressed in UTC because
    that is the platform-wide storage convention.
    """

    if isinstance(value, datetime):
        candidate = value
    elif hasattr(value, "to_pydatetime"):
        candidate = value.to_pydatetime()  # type: ignore[assignment]
    else:
        raise TypeError(f"Unsupported timestamp type: {type(value)!r}")

    if candidate.tzinfo is None:
        return candidate.replace(tzinfo=timezone.utc)
    return candidate


def as_london_time(value: Any) -> datetime:
    """Return *value* converted to the Europe/London timezone.

    The helper accepts ``datetime`` objects and pandas timestamps. DST transitions
    are handled via :mod:`zoneinfo`, ensuring the correct GMT/BST offset is applied
    for reporting and frontend presentation layers.
    """

    aware = _coerce_datetime(value)
    return aware.astimezone(_LONDON_TZ)


def format_london_time(value: Any, fmt: str = "%Y-%m-%d %H:%M:%S %Z") -> str:
    """Format *value* using the Europe/London timezone.

    Parameters
    ----------
    value:
        Any datetime-like object accepted by :func:`as_london_time`.
    fmt:
        ``strftime``-style format string. Defaults to ``"%Y-%m-%d %H:%M:%S %Z"``.

    Returns
    -------
    str
        A string representation of ``value`` with DST-aware timezone information.
    """

    return as_london_time(value).strftime(fmt)


__all__ = ["as_london_time", "format_london_time"]
