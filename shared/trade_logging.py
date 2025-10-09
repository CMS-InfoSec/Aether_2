"""Structured trade execution logging utilities."""

from __future__ import annotations

import csv
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Dict, Iterator, Optional


logger = logging.getLogger("trade.journal")


_DEFAULT_LOG_PATH = Path("reports/trade_log.csv")
_CSV_FIELDS = [
    "timestamp",
    "account_id",
    "client_order_id",
    "exchange_order_id",
    "symbol",
    "side",
    "quantity",
    "price",
    "pre_trade_mid",
    "pnl",
    "transport",
    "simulated",
]


def _coerce_decimal(value: Decimal | float | int | str | None) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        return Decimal(value)
    return Decimal(str(value))


def _format_decimal(value: Decimal | float | int | str | None) -> str:
    decimal_value = _coerce_decimal(value)
    if decimal_value is None:
        return ""
    normalized = decimal_value.normalize()
    # Ensure integer values render without scientific notation
    if normalized == normalized.to_integral():
        return f"{normalized.to_integral()}"
    return format(normalized, "f")


@dataclass(slots=True)
class TradeLogEntry:
    """Data structure capturing the essential attributes of an executed trade."""

    timestamp: datetime
    account_id: str
    client_order_id: str
    exchange_order_id: Optional[str]
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    pnl: Decimal
    pre_trade_mid: Optional[Decimal] = None
    transport: Optional[str] = None
    simulated: bool = False

    def as_dict(self) -> Dict[str, str]:
        ts = self.timestamp.astimezone(timezone.utc)
        return {
            "timestamp": ts.isoformat(),
            "account_id": self.account_id,
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id or "",
            "symbol": self.symbol,
            "side": self.side.lower(),
            "quantity": _format_decimal(self.quantity),
            "price": _format_decimal(self.price),
            "pre_trade_mid": _format_decimal(self.pre_trade_mid),
            "pnl": _format_decimal(self.pnl),
            "transport": (self.transport or "").lower(),
            "simulated": "true" if self.simulated else "false",
        }


class TradeLogger:
    """Append-only CSV + structured logging for trade executions."""

    def __init__(self, *, path: Path | None = None) -> None:
        env_path = os.getenv("TRADE_LOG_PATH")
        resolved = Path(env_path) if env_path else None
        if path is not None:
            resolved = path
        self._path = resolved if resolved is not None else _DEFAULT_LOG_PATH
        self._lock = Lock()

    def log(self, entry: TradeLogEntry) -> None:
        payload = entry.as_dict()
        logger.info("trade.executed", extra={"trade": payload})
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


_TRADE_LOGGER: TradeLogger | None = None


def get_trade_logger() -> TradeLogger:
    global _TRADE_LOGGER
    if _TRADE_LOGGER is None:
        _TRADE_LOGGER = TradeLogger()
    return _TRADE_LOGGER


@contextmanager
def override_trade_logger(logger_instance: TradeLogger) -> Iterator[TradeLogger]:
    global _TRADE_LOGGER
    previous = _TRADE_LOGGER
    _TRADE_LOGGER = logger_instance
    try:
        yield logger_instance
    finally:
        _TRADE_LOGGER = previous


__all__ = [
    "TradeLogEntry",
    "TradeLogger",
    "get_trade_logger",
    "override_trade_logger",
]
