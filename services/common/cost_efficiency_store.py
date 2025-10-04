"""Repository for persisting cost efficiency metrics in Timescale/Postgres.

The repository exposes a very small interface that allows callers to
upsert and retrieve infrastructure cost metrics keyed by ``account_id``.
Implementations default to using the shared Timescale/Postgres cluster via
``psycopg`` but transparently fall back to an in-memory store when the
driver or connection string are unavailable.  This keeps unit tests fast
while still exercising the same code paths that production will use.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Optional, Protocol

try:  # pragma: no cover - psycopg may be absent in lightweight test envs
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - allow tests to run without psycopg
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cost_efficiency_metrics (
    account_id TEXT PRIMARY KEY,
    infra_cost DOUBLE PRECISION NOT NULL,
    recent_pnl DOUBLE PRECISION NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    stored_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_UPSERT_SQL = """
INSERT INTO cost_efficiency_metrics (account_id, infra_cost, recent_pnl, observed_at, stored_at)
VALUES (%(account_id)s, %(infra_cost)s, %(recent_pnl)s, %(observed_at)s, %(stored_at)s)
ON CONFLICT (account_id) DO UPDATE
SET infra_cost = EXCLUDED.infra_cost,
    recent_pnl = EXCLUDED.recent_pnl,
    observed_at = EXCLUDED.observed_at,
    stored_at = EXCLUDED.stored_at
RETURNING account_id, infra_cost, recent_pnl, observed_at, stored_at
"""

_SELECT_SQL = """
SELECT account_id, infra_cost, recent_pnl, observed_at, stored_at
FROM cost_efficiency_metrics
WHERE account_id = %(account_id)s
"""

_DELETE_ONE_SQL = "DELETE FROM cost_efficiency_metrics WHERE account_id = %(account_id)s"
_DELETE_ALL_SQL = "DELETE FROM cost_efficiency_metrics"


def _normalize_timestamp(value: datetime) -> datetime:
    """Ensure ``value`` is timezone aware in UTC."""

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class CostEfficiencyRecord:
    """Persistent representation of cost efficiency metrics."""

    account_id: str
    infra_cost: float
    recent_pnl: float
    observed_at: datetime
    stored_at: datetime


class CostEfficiencyStore(Protocol):
    """Protocol describing store behaviours required by the service."""

    def fetch(self, account_id: str) -> Optional[CostEfficiencyRecord]:
        """Return the latest metrics for ``account_id`` if present."""

    def upsert(
        self, account_id: str, *, infra_cost: float, recent_pnl: float, observed_at: datetime
    ) -> CostEfficiencyRecord:
        """Persist metrics for ``account_id`` returning the stored record."""

    def clear(self, account_id: Optional[str] = None) -> None:
        """Remove stored metrics either for ``account_id`` or entirely."""


class InMemoryCostEfficiencyStore:
    """Thread-safe in-memory store used for tests and fallbacks."""

    def __init__(self) -> None:
        self._records: dict[str, CostEfficiencyRecord] = {}
        self._lock = RLock()

    def fetch(self, account_id: str) -> Optional[CostEfficiencyRecord]:
        with self._lock:
            return self._records.get(account_id)

    def upsert(
        self, account_id: str, *, infra_cost: float, recent_pnl: float, observed_at: datetime
    ) -> CostEfficiencyRecord:
        stored_at = datetime.now(timezone.utc)
        record = CostEfficiencyRecord(
            account_id=account_id,
            infra_cost=float(infra_cost),
            recent_pnl=float(recent_pnl),
            observed_at=_normalize_timestamp(observed_at),
            stored_at=stored_at,
        )
        with self._lock:
            self._records[account_id] = record
        return record

    def clear(self, account_id: Optional[str] = None) -> None:
        with self._lock:
            if account_id is None:
                self._records.clear()
            else:
                self._records.pop(account_id, None)


class TimescaleCostEfficiencyStore:
    """Repository backed by the shared Timescale/Postgres cluster."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn or os.getenv("COST_EFFICIENCY_DSN") or os.getenv("TIMESCALE_DSN")
        self._fallback = InMemoryCostEfficiencyStore()

    def _driver_available(self) -> bool:
        return bool(psycopg) and bool(self._dsn)

    def _execute(self, sql: str, params: Optional[dict[str, object]] = None) -> list[dict[str, object]]:
        if not self._driver_available():
            return []

        assert psycopg is not None  # Satisfy type checkers
        try:
            with psycopg.connect(self._dsn, autocommit=True) as conn:  # type: ignore[arg-type]
                with conn.cursor(row_factory=dict_row) as cur:  # type: ignore[misc]
                    cur.execute(_CREATE_TABLE_SQL)
                    cur.execute(sql, params or {})
                    if cur.description is None:
                        return []
                    return list(cur.fetchall())
        except Exception:  # pragma: no cover - depends on external service
            logger.warning("Falling back to in-memory cost efficiency store", exc_info=True)
            return []

    def fetch(self, account_id: str) -> Optional[CostEfficiencyRecord]:
        rows = self._execute(_SELECT_SQL, {"account_id": account_id})
        if rows:
            row = rows[0]
            return CostEfficiencyRecord(
                account_id=str(row["account_id"]),
                infra_cost=float(row["infra_cost"]),
                recent_pnl=float(row["recent_pnl"]),
                observed_at=_normalize_timestamp(row["observed_at"]),
                stored_at=_normalize_timestamp(row["stored_at"]),
            )
        # fall back to in-memory record
        return self._fallback.fetch(account_id)

    def upsert(
        self, account_id: str, *, infra_cost: float, recent_pnl: float, observed_at: datetime
    ) -> CostEfficiencyRecord:
        stored_at = datetime.now(timezone.utc)
        params = {
            "account_id": account_id,
            "infra_cost": float(infra_cost),
            "recent_pnl": float(recent_pnl),
            "observed_at": _normalize_timestamp(observed_at),
            "stored_at": stored_at,
        }
        rows = self._execute(_UPSERT_SQL, params)
        if rows:
            row = rows[0]
            record = CostEfficiencyRecord(
                account_id=str(row["account_id"]),
                infra_cost=float(row["infra_cost"]),
                recent_pnl=float(row["recent_pnl"]),
                observed_at=_normalize_timestamp(row["observed_at"]),
                stored_at=_normalize_timestamp(row["stored_at"]),
            )
        else:
            record = self._fallback.upsert(
                account_id,
                infra_cost=float(infra_cost),
                recent_pnl=float(recent_pnl),
                observed_at=_normalize_timestamp(observed_at),
            )
        # Keep fallback updated so reads succeed if the DB becomes unavailable
        self._fallback.upsert(
            account_id,
            infra_cost=record.infra_cost,
            recent_pnl=record.recent_pnl,
            observed_at=record.observed_at,
        )
        return CostEfficiencyRecord(
            account_id=record.account_id,
            infra_cost=record.infra_cost,
            recent_pnl=record.recent_pnl,
            observed_at=record.observed_at,
            stored_at=_normalize_timestamp(record.stored_at),
        )

    def clear(self, account_id: Optional[str] = None) -> None:
        if account_id is None:
            self._execute(_DELETE_ALL_SQL)
        else:
            self._execute(_DELETE_ONE_SQL, {"account_id": account_id})
        self._fallback.clear(account_id)


_STORE: CostEfficiencyStore = TimescaleCostEfficiencyStore()


def configure_cost_efficiency_store(store: CostEfficiencyStore) -> None:
    """Override the global store implementation (useful for tests)."""

    global _STORE
    _STORE = store


def get_cost_efficiency_store() -> CostEfficiencyStore:
    """Return the configured store implementation."""

    return _STORE


__all__ = [
    "CostEfficiencyRecord",
    "CostEfficiencyStore",
    "InMemoryCostEfficiencyStore",
    "TimescaleCostEfficiencyStore",
    "configure_cost_efficiency_store",
    "get_cost_efficiency_store",
]

