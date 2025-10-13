"""Timezone helpers for London-aware formatting without heavy dependencies."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Final, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

ZoneInfoType: type[Any] | None

try:  # pragma: no cover - prefer stdlib tzdata when available
    from zoneinfo import ZoneInfo as ZoneInfoType
except (ModuleNotFoundError, KeyError):  # pragma: no cover - stripped tzdata
    ZoneInfoType = None

_UTC: Final = timezone.utc
_GMT: Final = timezone(timedelta(0), name="GMT")
_BST: Final = timezone(timedelta(hours=1), name="BST")

if ZoneInfoType is not None:  # pragma: no branch - mainline path
    try:
        _london_tz: Optional["ZoneInfo"] = ZoneInfoType("Europe/London")
    except Exception:  # pragma: no cover - tzdata missing at runtime
        _london_tz = None
else:  # pragma: no cover - fallback approximation
    _london_tz = None

_LONDON_TZ: Final[Optional["ZoneInfo"]] = _london_tz


def _coerce_utc(instant: datetime) -> datetime:
    """Return *instant* as an aware datetime in UTC."""

    if instant.tzinfo is None:
        return instant.replace(tzinfo=_UTC)
    return instant.astimezone(_UTC)


def _fallback_offset(instant: datetime) -> timezone:
    """Approximate the Europe/London offset when tzdata is unavailable."""

    year = instant.year

    def _last_sunday(month: int) -> datetime:
        last_day = max(
            day
            for day in range(31, 24, -1)
            if datetime(year, month, day).weekday() == 6
        )
        return datetime(year, month, last_day, 1, tzinfo=_UTC)

    dst_start = _last_sunday(3)
    dst_end = _last_sunday(10)

    if dst_start <= instant < dst_end:
        return _BST
    return _GMT


def as_london_time(instant: datetime) -> datetime:
    """Convert *instant* to Europe/London time, preserving microseconds."""

    utc = _coerce_utc(instant)
    if _LONDON_TZ is not None:
        return utc.astimezone(_LONDON_TZ)

    offset = _fallback_offset(utc)
    return utc.astimezone(offset)


def format_london_time(instant: datetime) -> str:
    """Format *instant* using ``YYYY-MM-DD HH:MM:SS <TZ>`` semantics."""

    local = as_london_time(instant)
    return f"{local:%Y-%m-%d %H:%M:%S} {local.tzname()}"


__all__ = ["as_london_time", "format_london_time"]
