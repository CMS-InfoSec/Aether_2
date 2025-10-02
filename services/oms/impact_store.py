"""Persistent storage for realized impact analytics.

This module provides a small abstraction over TimescaleDB for recording
historical fills alongside the pre-trade mid price and computing realized
slippage curves.  The implementation gracefully degrades to an in-memory
store when TimescaleDB is unavailable so unit tests and local development
environments without the database continue to function.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from threading import Lock
from typing import Any, DefaultDict, Dict, Iterable, List, Tuple

from services.common.config import get_timescale_session

try:  # pragma: no cover - optional dependency in CI
    import psycopg
    from psycopg import sql
except Exception:  # pragma: no cover - fallback when psycopg is unavailable
    psycopg = None
    sql = None  # type: ignore


LOGGER = logging.getLogger(__name__)


@dataclass
class ImpactFill:
    """Structured representation of a recorded fill."""

    account_id: str
    client_order_id: str
    symbol: str
    side: str
    filled_qty: Decimal
    avg_price: Decimal
    pre_trade_mid: Decimal
    impact_bps: Decimal
    recorded_at: datetime
    simulated: bool = False


class ImpactAnalyticsStore:
    """Timescale-backed persistence layer for trade impact analytics."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._initialized_accounts: set[str] = set()
        self._memory: DefaultDict[Tuple[str, str], List[ImpactFill]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def record_fill(
        self,
        *,
        account_id: str,
        client_order_id: str,
        symbol: str,
        side: str,
        filled_qty: Decimal,
        avg_price: Decimal,
        pre_trade_mid: Decimal,
        impact_bps: Decimal,
        recorded_at: datetime,
        simulated: bool = False,
    ) -> None:
        fill = ImpactFill(
            account_id=account_id,
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            filled_qty=filled_qty,
            avg_price=avg_price,
            pre_trade_mid=pre_trade_mid,
            impact_bps=impact_bps,
            recorded_at=recorded_at,
            simulated=simulated,
        )

        if psycopg is None:
            self._append_memory(fill)
            return

        from asyncio import to_thread

        await to_thread(self._record_fill_sync, fill)

    async def impact_curve(
        self,
        *,
        account_id: str,
        symbol: str,
        limit: int = 500,
        bucket_count: int = 10,
    ) -> List[Dict[str, float]]:
        if psycopg is None:
            rows = [
                (fill.filled_qty, fill.impact_bps)
                for fill in self._memory.get((account_id, symbol), [])
            ]
            return self._build_curve(rows, bucket_count=bucket_count)

        from asyncio import to_thread

        return await to_thread(
            self._impact_curve_sync,
            account_id,
            symbol,
            limit,
            bucket_count,
        )

    # ------------------------------------------------------------------
    # Synchronous helpers executed in a threadpool
    # ------------------------------------------------------------------
    def _record_fill_sync(self, fill: ImpactFill) -> None:
        assert psycopg is not None and sql is not None  # nosec - guarded by caller

        session = get_timescale_session(fill.account_id)
        try:
            with psycopg.connect(session.dsn, autocommit=True) as conn:
                self._ensure_schema(conn, session.account_schema)
                insert_sql = sql.SQL(
                    """
                    INSERT INTO {}.{} (
                        recorded_at,
                        account_id,
                        client_order_id,
                        symbol,
                        side,
                        filled_qty,
                        avg_price,
                        pre_trade_mid,
                        impact_bps,
                        simulated
                    )
                    VALUES (
                        %(recorded_at)s,
                        %(account_id)s,
                        %(client_order_id)s,
                        %(symbol)s,
                        %(side)s,
                        %(filled_qty)s,
                        %(avg_price)s,
                        %(pre_trade_mid)s,
                        %(impact_bps)s,
                        %(simulated)s
                    )
                    ON CONFLICT DO NOTHING
                """
                ).format(
                    sql.Identifier(session.account_schema),
                    sql.Identifier("oms_fill_impact"),
                )
                params = {
                    "recorded_at": fill.recorded_at,
                    "account_id": fill.account_id,
                    "client_order_id": fill.client_order_id,
                    "symbol": fill.symbol,
                    "side": fill.side,
                    "filled_qty": fill.filled_qty,
                    "avg_price": fill.avg_price,
                    "pre_trade_mid": fill.pre_trade_mid,
                    "impact_bps": fill.impact_bps,
                    "simulated": fill.simulated,
                }
                with conn.cursor() as cursor:
                    cursor.execute(insert_sql, params)
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning(
                "Failed to persist impact analytics for %s: %s. Falling back to in-memory store.",
                fill.account_id,
                exc,
            )
            self._append_memory(fill)

    def _impact_curve_sync(
        self,
        account_id: str,
        symbol: str,
        limit: int,
        bucket_count: int,
    ) -> List[Dict[str, float]]:
        assert psycopg is not None and sql is not None  # nosec - guarded by caller

        session = get_timescale_session(account_id)
        try:
            with psycopg.connect(session.dsn, autocommit=True) as conn:
                self._ensure_schema(conn, session.account_schema)
                query = sql.SQL(
                    """
                    SELECT filled_qty::float8, impact_bps::float8
                    FROM {}.{}
                    WHERE symbol = %(symbol)s
                    ORDER BY recorded_at DESC
                    LIMIT %(limit)s
                """
                ).format(
                    sql.Identifier(session.account_schema),
                    sql.Identifier("oms_fill_impact"),
                )
                params = {"symbol": symbol, "limit": limit}
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    rows = cursor.fetchall()
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning(
                "Failed to query impact analytics for %s: %s. Falling back to in-memory data.",
                account_id,
                exc,
            )
            rows = [
                (fill.filled_qty, fill.impact_bps)
                for fill in self._memory.get((account_id, symbol), [])
            ]

        return self._build_curve(rows, bucket_count=bucket_count)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _ensure_schema(self, conn: "psycopg.Connection[Any]", schema: str) -> None:
        if schema in self._initialized_accounts:
            return

        assert psycopg is not None and sql is not None  # nosec - guarded by caller

        with self._lock:
            if schema in self._initialized_accounts:
                return

            create_schema = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}" ).format(
                sql.Identifier(schema)
            )
            create_table = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    recorded_at TIMESTAMPTZ NOT NULL,
                    account_id TEXT NOT NULL,
                    client_order_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    filled_qty NUMERIC NOT NULL,
                    avg_price NUMERIC NOT NULL,
                    pre_trade_mid NUMERIC NOT NULL,
                    impact_bps DOUBLE PRECISION NOT NULL,
                    simulated BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY (recorded_at, account_id, symbol, client_order_id)
                )
                """
            ).format(
                sql.Identifier(schema),
                sql.Identifier("oms_fill_impact"),
            )
            add_simulated_column = sql.SQL(
                """
                ALTER TABLE {}.{}
                ADD COLUMN IF NOT EXISTS simulated BOOLEAN NOT NULL DEFAULT FALSE
                """
            ).format(
                sql.Identifier(schema),
                sql.Identifier("oms_fill_impact"),
            )
            create_index = sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS {} ON {}.{} (symbol, recorded_at DESC)
                """
            ).format(
                sql.Identifier(f"{schema}_impact_symbol_recorded_at"),
                sql.Identifier(schema),
                sql.Identifier("oms_fill_impact"),
            )
            with conn.cursor() as cursor:
                cursor.execute(create_schema)
                cursor.execute(create_table)
                cursor.execute(add_simulated_column)
                cursor.execute(create_index)

            self._initialized_accounts.add(schema)

    @staticmethod
    def _build_curve(
        rows: Iterable[Tuple[Decimal | float, Decimal | float]],
        *,
        bucket_count: int,
    ) -> List[Dict[str, float]]:
        def _to_decimal(value: Decimal | float | None) -> Decimal:
            if value is None:
                raise ValueError("Cannot convert None to Decimal")
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))

        data = [
            (_to_decimal(row[0]), _to_decimal(row[1]))
            for row in rows
            if row[0] is not None and row[1] is not None
        ]
        if not data:
            return []

        ordered = sorted(data, key=lambda item: abs(item[0]))
        buckets = max(1, min(bucket_count, len(ordered)))
        chunk_size = max(1, math.ceil(len(ordered) / buckets))

        points: List[Dict[str, float]] = []
        for index in range(0, len(ordered), chunk_size):
            chunk = ordered[index : index + chunk_size]
            if not chunk:
                continue
            total_chunk = Decimal(len(chunk))
            total_size = sum((abs(entry[0]) for entry in chunk), Decimal("0"))
            total_impact = sum((entry[1] for entry in chunk), Decimal("0"))
            avg_size = total_size / total_chunk
            avg_impact = total_impact / total_chunk
            points.append({"size": float(avg_size), "impact_bps": float(avg_impact)})

        return points

    def _append_memory(self, fill: ImpactFill) -> None:
        key = (fill.account_id, fill.symbol)
        with self._lock:
            self._memory[key].append(fill)


impact_store = ImpactAnalyticsStore()


__all__ = ["ImpactAnalyticsStore", "ImpactFill", "impact_store"]

