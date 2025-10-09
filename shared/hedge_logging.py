"""CSV-backed journal for hedge rebalance activity.

This module mirrors the structure of :mod:`shared.trade_logging` but focuses on
capturing the lifecycle of hedge adjustments.  Each rebalance event is stored
as a structured log entry and appended to a CSV file so downstream services can
generate hedge performance reports.
"""

from __future__ import annotations

import csv
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from threading import Lock
from typing import Dict, Iterator, List, Optional


logger = logging.getLogger("hedge.journal")


_DEFAULT_LOG_PATH = Path("reports/hedge_log.csv")
_CSV_FIELDS = [
    "timestamp",
    "account_id",
    "symbol",
    "side",
    "previous_allocation",
    "target_allocation",
    "delta_usd",
    "quantity",
    "price",
    "risk_score",
    "drawdown_pct",
    "atr",
    "realized_vol",
    "order_id",
]


def _coerce_decimal(value: Decimal | float | int | str | None) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return Decimal(stripped)
        except InvalidOperation:
            return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def _format_decimal(value: Decimal | float | int | str | None) -> str:
    decimal_value = _coerce_decimal(value)
    if decimal_value is None:
        return ""
    normalized = decimal_value.normalize()
    if normalized == normalized.to_integral():
        return f"{normalized.to_integral()}"
    return format(normalized, "f")


@dataclass(slots=True)
class HedgeLogEntry:
    """Snapshot describing a hedge rebalance event."""

    timestamp: datetime
    account_id: str
    symbol: str
    side: str
    previous_allocation: Decimal
    target_allocation: Decimal
    delta_usd: Decimal
    quantity: Decimal
    price: Decimal
    risk_score: Decimal
    drawdown_pct: Decimal
    atr: Optional[Decimal] = None
    realized_vol: Optional[Decimal] = None
    order_id: Optional[str] = None

    def as_dict(self) -> Dict[str, str]:
        ts = self.timestamp.astimezone(timezone.utc)
        return {
            "timestamp": ts.isoformat(),
            "account_id": self.account_id,
            "symbol": self.symbol,
            "side": self.side.lower(),
            "previous_allocation": _format_decimal(self.previous_allocation),
            "target_allocation": _format_decimal(self.target_allocation),
            "delta_usd": _format_decimal(self.delta_usd),
            "quantity": _format_decimal(self.quantity),
            "price": _format_decimal(self.price),
            "risk_score": _format_decimal(self.risk_score),
            "drawdown_pct": _format_decimal(self.drawdown_pct),
            "atr": _format_decimal(self.atr),
            "realized_vol": _format_decimal(self.realized_vol),
            "order_id": self.order_id or "",
        }


class HedgeLogger:
    """Append-only CSV logger for hedge adjustments."""

    def __init__(self, *, path: Path | None = None) -> None:
        env_path = os.getenv("HEDGE_LOG_PATH")
        resolved = Path(env_path) if env_path else None
        if path is not None:
            resolved = path
        self._path = resolved if resolved is not None else _DEFAULT_LOG_PATH
        self._lock = Lock()

    @property
    def path(self) -> Path:
        return self._path

    def log(self, entry: HedgeLogEntry) -> None:
        payload = entry.as_dict()
        logger.info("hedge.rebalance", extra={"hedge": payload})
        self._write_csv(payload)

    def _write_csv(self, row: Dict[str, str]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            file_exists = self._path.exists()
            with self._path.open("a", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=_CSV_FIELDS)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)


_HEDGE_LOGGER: HedgeLogger | None = None


def get_hedge_logger() -> HedgeLogger:
    global _HEDGE_LOGGER
    if _HEDGE_LOGGER is None:
        _HEDGE_LOGGER = HedgeLogger()
    return _HEDGE_LOGGER


@contextmanager
def override_hedge_logger(logger_instance: HedgeLogger) -> Iterator[HedgeLogger]:
    global _HEDGE_LOGGER
    previous = _HEDGE_LOGGER
    _HEDGE_LOGGER = logger_instance
    try:
        yield logger_instance
    finally:
        _HEDGE_LOGGER = previous


def log_hedge_event(entry: HedgeLogEntry) -> None:
    get_hedge_logger().log(entry)


HEDGE_LOG_COLUMNS: tuple[str, ...] = tuple(_CSV_FIELDS)


def _normalize_bound(value: date | datetime | None, *, default_time: time) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.combine(value, default_time)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _parse_timestamp(raw: str | None) -> Optional[datetime]:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed


def iter_hedge_log_rows(
    *,
    account_id: str | None = None,
    start: date | datetime | None = None,
    end: date | datetime | None = None,
    path: Path | None = None,
) -> Iterator[Dict[str, str]]:
    """Yield hedge journal rows filtered by account and time window."""

    if path is None:
        path = get_hedge_logger().path

    if not path.exists():
        return iter(())

    normalized_account = account_id.lower() if account_id else None
    start_dt = _normalize_bound(start, default_time=time.min)
    end_dt = _normalize_bound(end, default_time=time.max)

    def _generator() -> Iterator[Dict[str, str]]:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                row_account = (row.get("account_id") or "").lower()
                if normalized_account and row_account != normalized_account:
                    continue

                timestamp = _parse_timestamp(row.get("timestamp"))
                if start_dt and timestamp and timestamp < start_dt:
                    continue
                if end_dt and timestamp and timestamp > end_dt:
                    continue
                yield row

    return _generator()


def read_hedge_log(
    *,
    account_id: str | None = None,
    start: date | datetime | None = None,
    end: date | datetime | None = None,
    path: Path | None = None,
) -> List[Dict[str, str]]:
    """Return hedge journal rows filtered by the supplied criteria."""

    return list(iter_hedge_log_rows(account_id=account_id, start=start, end=end, path=path))


__all__ = [
    "HedgeLogEntry",
    "HedgeLogger",
    "HEDGE_LOG_COLUMNS",
    "get_hedge_logger",
    "override_hedge_logger",
    "log_hedge_event",
    "iter_hedge_log_rows",
    "read_hedge_log",
]

