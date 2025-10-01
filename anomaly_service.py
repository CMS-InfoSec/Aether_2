"""FastAPI service responsible for monitoring trade fills for anomalies.

The service periodically scans the ``fills`` hypertable inside the
account-specific TimescaleDB schema and looks for a handful of risk signals:

* **Price outliers** – fill prices that deviate materially from the recent
  median for the instrument.
* **Excessive rejections** – orders that are repeatedly rejected relative to
  filled orders within the observation window.
* **Fee spikes** – fees that are disproportionally large compared to the
  notional of the executed trade.

When an anomaly is detected it is persisted to the ``fill_anomalies`` table,
Alertmanager notifications are triggered, and (optionally) the account's kill
switch is engaged to halt trading.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import statistics
from collections import defaultdict
from contextlib import closing, suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence

import psycopg2
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import Response
from pydantic import BaseModel, Field
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

from services.alert_manager import (
    RiskEvent,
    alert_fee_spike,
    get_alert_manager_instance,
    setup_alerting,
)
from services.common.adapters import TimescaleAdapter
from services.common.config import get_timescale_session


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


ACCOUNT_ID = os.getenv("AETHER_ACCOUNT_ID", "default")
TIMESCALE = get_timescale_session(ACCOUNT_ID)

LOOKBACK_MINUTES = int(os.getenv("ANOMALY_LOOKBACK_MINUTES", "15"))
SCAN_INTERVAL_SECONDS = int(os.getenv("ANOMALY_SCAN_INTERVAL_SECONDS", "300"))
PRICE_DEVIATION_THRESHOLD_PCT = float(os.getenv("ANOMALY_PRICE_DEVIATION_PCT", "5.0"))
FEE_SPIKE_THRESHOLD_BPS = float(os.getenv("ANOMALY_FEE_THRESHOLD_BPS", "75.0"))
REJECTION_THRESHOLD = int(os.getenv("ANOMALY_REJECTION_THRESHOLD", "5"))
BLOCK_ON_SEVERITIES = {
    severity.strip().lower()
    for severity in os.getenv("ANOMALY_BLOCK_SEVERITIES", "high,critical").split(",")
    if severity.strip()
}
BLOCK_ENABLED = os.getenv("ANOMALY_BLOCK_ENABLED", "true").lower() in {"true", "1", "yes"}
BLOCK_ACTOR = os.getenv("ANOMALY_BLOCK_ACTOR", "anomaly-service")


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------


ANOMALY_SCANS_TOTAL = Counter(
    "anomaly_scans_total",
    "Total number of anomaly scans executed.",
)

ANOMALY_EVENTS_TOTAL = Counter(
    "anomaly_events_total",
    "Number of detected anomalies grouped by type and severity.",
    ("anomaly_type", "severity"),
)

ANOMALY_LAST_SCAN_TS = Gauge(
    "anomaly_last_scan_timestamp_seconds",
    "Unix timestamp of the most recent anomaly scan.",
)

ANOMALY_BLOCKED_ACCOUNTS = Gauge(
    "anomaly_blocked_accounts",
    "Number of accounts currently blocked by the anomaly service.",
)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def _connect() -> psycopg2.extensions.connection:
    """Create a TimescaleDB connection scoped to the account schema."""

    conn = psycopg2.connect(TIMESCALE.dsn)
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(TIMESCALE.account_schema)
            )
        )
    return conn


def _ensure_tables() -> None:
    """Create the ``fill_anomalies`` table if it does not yet exist."""

    create_stmt = """
    CREATE TABLE IF NOT EXISTS fill_anomalies (
        anomaly_id BIGSERIAL PRIMARY KEY,
        fill_id UUID,
        order_id UUID,
        account_id TEXT NOT NULL,
        instrument TEXT NOT NULL,
        anomaly_type TEXT NOT NULL,
        severity TEXT NOT NULL,
        description TEXT NOT NULL,
        detected_at TIMESTAMPTZ NOT NULL,
        metadata JSONB DEFAULT '{}'::jsonb,
        fingerprint TEXT UNIQUE
    )
    """

    with closing(_connect()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_stmt)


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FillRecord:
    fill_id: Optional[str]
    order_id: Optional[str]
    account_id: str
    instrument: str
    price: float
    size: float
    fee: Optional[float]
    fill_time: datetime


@dataclass(frozen=True)
class RejectionStats:
    account_id: str
    instrument: str
    rejections: int
    fills: int
    last_order_ts: Optional[datetime]


@dataclass(frozen=True)
class DetectedAnomaly:
    account_id: str
    instrument: str
    anomaly_type: str
    severity: str
    description: str
    metadata: MutableMapping[str, Any] = field(default_factory=dict)
    fill_id: Optional[str] = None
    order_id: Optional[str] = None

    @property
    def fingerprint(self) -> str:
        payload = json.dumps(
            {
                "account_id": self.account_id,
                "instrument": self.instrument,
                "type": self.anomaly_type,
                "severity": self.severity,
                "description": self.description,
                "fill_id": self.fill_id,
                "order_id": self.order_id,
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Anomaly monitor
# ---------------------------------------------------------------------------


class AnomalyMonitor:
    """Encapsulates the anomaly detection workflow and state."""

    def __init__(
        self,
        *,
        lookback: timedelta,
        price_threshold_pct: float,
        fee_threshold_bps: float,
        rejection_threshold: int,
        interval_seconds: int,
    ) -> None:
        self.lookback = lookback
        self.price_threshold_pct = price_threshold_pct
        self.fee_threshold_bps = fee_threshold_bps
        self.rejection_threshold = rejection_threshold
        self.interval_seconds = interval_seconds
        self.last_run: Optional[datetime] = None
        self.blocked_accounts: set[str] = set()
        self._stop_event = asyncio.Event()
        self._scan_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def run_periodic(self) -> None:
        """Continuously execute anomaly scans until cancelled."""

        while not self._stop_event.is_set():
            try:
                await self.run_scan_async()
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Anomaly scan failed")

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval_seconds)
            except asyncio.TimeoutError:
                continue

    async def run_scan_async(self) -> None:
        """Execute a scan in a thread-safe manner from the asyncio loop."""

        async with self._scan_lock:
            await asyncio.to_thread(self.scan_once)

    def stop(self) -> None:
        """Signal the monitor to stop periodic execution."""

        self._stop_event.set()

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------
    def scan_once(self) -> None:
        """Run a single anomaly detection pass."""

        observation_start = datetime.now(timezone.utc) - self.lookback
        now = datetime.now(timezone.utc)
        logger.debug("Starting anomaly scan lookback=%s", self.lookback)

        with closing(_connect()) as conn:
            fills = self._fetch_recent_fills(conn, observation_start)
            rejection_stats = self._fetch_rejection_stats(conn, observation_start)

            anomalies: List[DetectedAnomaly] = []
            anomalies.extend(self._detect_price_outliers(fills))
            anomalies.extend(self._detect_fee_spikes(fills))
            anomalies.extend(self._detect_rejection_spikes(rejection_stats))

            inserted = self._persist_anomalies(conn, anomalies, now)

            if inserted:
                self._emit_alerts(inserted)
                engaged = self._engage_kill_switch(inserted)
                if engaged:
                    self._mark_blocked(conn, inserted, now)

        self.last_run = now
        ANOMALY_SCANS_TOTAL.inc()
        ANOMALY_LAST_SCAN_TS.set(now.timestamp())
        ANOMALY_BLOCKED_ACCOUNTS.set(len(self.blocked_accounts))

    def fetch_recent_anomalies(self, limit: int = 100) -> List[Mapping[str, Any]]:
        """Return the most recent anomaly rows for API display."""

        query = """
        SELECT
            anomaly_id,
            detected_at,
            account_id,
            instrument,
            anomaly_type,
            severity,
            description,
            metadata,
            fill_id::text AS fill_id,
            order_id::text AS order_id
        FROM fill_anomalies
        ORDER BY detected_at DESC
        LIMIT %(limit)s
        """

        with closing(_connect()) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, {"limit": limit})
                results = cursor.fetchall()
        return results

    # ------------------------------------------------------------------
    # Data collection helpers
    # ------------------------------------------------------------------
    def _fetch_recent_fills(
        self, conn: psycopg2.extensions.connection, start: datetime
    ) -> List[FillRecord]:
        query = """
        SELECT
            f.fill_id::text AS fill_id,
            f.order_id::text AS order_id,
            o.account_id::text AS account_id,
            o.market AS instrument,
            f.price::float AS price,
            f.size::float AS size,
            f.fee::float AS fee,
            f.fill_time AS fill_time
        FROM fills AS f
        JOIN orders AS o ON o.order_id = f.order_id
        WHERE f.fill_time >= %(start)s
        """

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, {"start": start})
            rows = cursor.fetchall()

        fills: List[FillRecord] = []
        for row in rows:
            price = float(row.get("price") or 0.0)
            size = float(row.get("size") or 0.0)
            fee = row.get("fee")
            fee_value = float(fee) if fee is not None else None
            fills.append(
                FillRecord(
                    fill_id=row.get("fill_id"),
                    order_id=row.get("order_id"),
                    account_id=str(row.get("account_id")),
                    instrument=str(row.get("instrument")),
                    price=price,
                    size=size,
                    fee=fee_value,
                    fill_time=row["fill_time"],
                )
            )
        return fills

    def _fetch_rejection_stats(
        self, conn: psycopg2.extensions.connection, start: datetime
    ) -> List[RejectionStats]:
        query = """
        SELECT
            o.account_id::text AS account_id,
            o.market AS instrument,
            COUNT(*) FILTER (WHERE lower(o.status) = 'rejected') AS rejections,
            COUNT(*) FILTER (
                WHERE lower(o.status) IN ('filled', 'partially_filled', 'executed')
            ) AS fills,
            MAX(o.submitted_at) AS last_order_ts
        FROM orders AS o
        WHERE o.submitted_at >= %(start)s
        GROUP BY 1, 2
        """

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, {"start": start})
            rows = cursor.fetchall()

        stats: List[RejectionStats] = []
        for row in rows:
            stats.append(
                RejectionStats(
                    account_id=str(row.get("account_id")),
                    instrument=str(row.get("instrument")),
                    rejections=int(row.get("rejections") or 0),
                    fills=int(row.get("fills") or 0),
                    last_order_ts=row.get("last_order_ts"),
                )
            )
        return stats

    # ------------------------------------------------------------------
    # Detection logic
    # ------------------------------------------------------------------
    def _detect_price_outliers(self, fills: Sequence[FillRecord]) -> List[DetectedAnomaly]:
        grouped: Dict[tuple[str, str], List[FillRecord]] = defaultdict(list)
        for fill in fills:
            grouped[(fill.account_id, fill.instrument)].append(fill)

        anomalies: List[DetectedAnomaly] = []
        for (account_id, instrument), records in grouped.items():
            if len(records) < 3:
                continue

            prices = [fill.price for fill in records if fill.price and not math.isnan(fill.price)]
            if len(prices) < 3:
                continue

            median_price = statistics.median(prices)
            if median_price == 0:
                continue

            for fill in records:
                deviation_pct = abs(fill.price - median_price) / abs(median_price) * 100.0
                if deviation_pct < self.price_threshold_pct:
                    continue

                severity = "warning"
                if deviation_pct >= self.price_threshold_pct * 2:
                    severity = "critical"
                elif deviation_pct >= self.price_threshold_pct * 1.5:
                    severity = "high"

                metadata: MutableMapping[str, Any] = {
                    "fill_price": fill.price,
                    "median_price": median_price,
                    "percent_deviation": deviation_pct,
                    "observation_count": len(records),
                }
                anomalies.append(
                    DetectedAnomaly(
                        account_id=account_id,
                        instrument=instrument,
                        anomaly_type="price_outlier",
                        severity=severity,
                        description=(
                            f"Fill price deviated {deviation_pct:.2f}% from the "
                            f"median for {instrument}"
                        ),
                        metadata=metadata,
                        fill_id=fill.fill_id,
                        order_id=fill.order_id,
                    )
                )
        return anomalies

    def _detect_fee_spikes(self, fills: Sequence[FillRecord]) -> List[DetectedAnomaly]:
        anomalies: List[DetectedAnomaly] = []
        for fill in fills:
            if fill.fee is None:
                continue

            notional = abs(fill.price * fill.size)
            if notional <= 0:
                continue

            fee_bps = abs(fill.fee) / notional * 10_000.0
            if fee_bps < self.fee_threshold_bps:
                continue

            severity = "warning"
            if fee_bps >= self.fee_threshold_bps * 2:
                severity = "critical"
            elif fee_bps >= self.fee_threshold_bps * 1.5:
                severity = "high"

            metadata: MutableMapping[str, Any] = {
                "fee": fill.fee,
                "notional": notional,
                "fee_bps": fee_bps,
            }

            anomalies.append(
                DetectedAnomaly(
                    account_id=fill.account_id,
                    instrument=fill.instrument,
                    anomaly_type="fee_spike",
                    severity=severity,
                    description=(
                        f"Observed fee of {fee_bps:.2f} bps on fill {fill.fill_id or 'unknown'}"
                    ),
                    metadata=metadata,
                    fill_id=fill.fill_id,
                    order_id=fill.order_id,
                )
            )
        return anomalies

    def _detect_rejection_spikes(
        self, stats: Sequence[RejectionStats]
    ) -> List[DetectedAnomaly]:
        anomalies: List[DetectedAnomaly] = []
        for record in stats:
            if record.rejections < self.rejection_threshold:
                continue

            ratio = record.rejections / max(record.fills, 1)
            severity = "warning"
            if ratio >= 3 or record.rejections >= self.rejection_threshold * 3:
                severity = "critical"
            elif ratio >= 2 or record.rejections >= self.rejection_threshold * 2:
                severity = "high"

            metadata: MutableMapping[str, Any] = {
                "rejections": record.rejections,
                "fills": record.fills,
                "ratio": ratio,
            }

            if record.last_order_ts:
                metadata["last_order_ts"] = record.last_order_ts.isoformat()

            anomalies.append(
                DetectedAnomaly(
                    account_id=record.account_id,
                    instrument=record.instrument,
                    anomaly_type="rejection_spike",
                    severity=severity,
                    description=(
                        f"Recorded {record.rejections} rejections vs {record.fills} fills for "
                        f"{record.instrument}"
                    ),
                    metadata=metadata,
                )
            )
        return anomalies

    # ------------------------------------------------------------------
    # Persistence and side effects
    # ------------------------------------------------------------------
    def _persist_anomalies(
        self,
        conn: psycopg2.extensions.connection,
        anomalies: Sequence[DetectedAnomaly],
        detected_at: datetime,
    ) -> List[DetectedAnomaly]:
        if not anomalies:
            return []

        insert_sql = """
        INSERT INTO fill_anomalies (
            fill_id,
            order_id,
            account_id,
            instrument,
            anomaly_type,
            severity,
            description,
            detected_at,
            metadata,
            fingerprint
        )
        VALUES (
            %(fill_id)s,
            %(order_id)s,
            %(account_id)s,
            %(instrument)s,
            %(anomaly_type)s,
            %(severity)s,
            %(description)s,
            %(detected_at)s,
            %(metadata)s::jsonb,
            %(fingerprint)s
        )
        ON CONFLICT (fingerprint) DO NOTHING
        RETURNING anomaly_id
        """

        inserted: List[DetectedAnomaly] = []
        with conn.cursor() as cursor:
            for anomaly in anomalies:
                payload = {
                    "fill_id": anomaly.fill_id,
                    "order_id": anomaly.order_id,
                    "account_id": anomaly.account_id,
                    "instrument": anomaly.instrument,
                    "anomaly_type": anomaly.anomaly_type,
                    "severity": anomaly.severity,
                    "description": anomaly.description,
                    "detected_at": detected_at,
                    "metadata": json.dumps(
                        {
                            **anomaly.metadata,
                            "block_requested": BLOCK_ENABLED
                            and anomaly.severity.lower() in BLOCK_ON_SEVERITIES,
                        }
                    ),
                    "fingerprint": anomaly.fingerprint,
                }
                cursor.execute(insert_sql, payload)
                if cursor.fetchone():
                    inserted.append(anomaly)
                    ANOMALY_EVENTS_TOTAL.labels(
                        anomaly.anomaly_type, anomaly.severity.lower()
                    ).inc()
        return inserted

    def _emit_alerts(self, anomalies: Sequence[DetectedAnomaly]) -> None:
        manager = get_alert_manager_instance()
        if not manager:
            return

        for anomaly in anomalies:
            severity = anomaly.severity.lower()
            if anomaly.anomaly_type == "fee_spike":
                fee_bps = anomaly.metadata.get("fee_bps")
                if fee_bps is None:
                    continue
                alert_fee_spike(anomaly.account_id, float(fee_bps) / 100.0)
                continue

            labels = {
                "account_id": anomaly.account_id,
                "instrument": anomaly.instrument,
                "anomaly_type": anomaly.anomaly_type,
            }
            manager.handle_risk_event(
                RiskEvent(
                    event_type=f"anomaly_{anomaly.anomaly_type}",
                    severity=severity,
                    description=anomaly.description,
                    labels=labels,
                )
            )

    def _engage_kill_switch(
        self, anomalies: Sequence[DetectedAnomaly]
    ) -> List[DetectedAnomaly]:
        if not BLOCK_ENABLED:
            return []

        engaged: List[DetectedAnomaly] = []
        for anomaly in anomalies:
            severity = anomaly.severity.lower()
            if severity not in BLOCK_ON_SEVERITIES:
                continue

            account_id = anomaly.account_id
            if account_id in self.blocked_accounts:
                continue

            try:
                adapter = TimescaleAdapter(account_id=account_id)
                adapter.set_kill_switch(
                    engaged=True,
                    reason=f"Anomaly detected: {anomaly.anomaly_type}",
                    actor=BLOCK_ACTOR,
                )
                self.blocked_accounts.add(account_id)
                engaged.append(anomaly)
                logger.warning(
                    "Engaged kill switch for %s due to %s", account_id, anomaly.anomaly_type
                )
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Failed to engage kill switch for %s", account_id)
        return engaged

    def _mark_blocked(
        self,
        conn: psycopg2.extensions.connection,
        anomalies: Sequence[DetectedAnomaly],
        blocked_at: datetime,
    ) -> None:
        if not anomalies:
            return

        update_sql = """
        UPDATE fill_anomalies
        SET metadata = metadata || %(update)s::jsonb
        WHERE fingerprint = %(fingerprint)s
        """

        update_payload = json.dumps(
            {"kill_switch_engaged": True, "blocked_at": blocked_at.isoformat()}
        )

        with conn.cursor() as cursor:
            for anomaly in anomalies:
                cursor.execute(
                    update_sql,
                    {
                        "update": update_payload,
                        "fingerprint": anomaly.fingerprint,
                    },
                )


# ---------------------------------------------------------------------------
# API models
# ---------------------------------------------------------------------------


class AnomalyRecord(BaseModel):
    anomaly_id: int
    detected_at: datetime
    account_id: str
    instrument: str
    anomaly_type: str
    severity: str
    description: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    fill_id: Optional[str] = None
    order_id: Optional[str] = None


class AnomalyStatus(BaseModel):
    last_scan: Optional[datetime]
    lookback_minutes: int
    interval_seconds: int
    blocked_accounts: List[str]
    anomalies: List[AnomalyRecord]


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


app = FastAPI(title="Anomaly Monitoring Service", version="1.0.0")
setup_alerting(app)


@app.on_event("startup")
async def _startup() -> None:
    _ensure_tables()
    monitor = AnomalyMonitor(
        lookback=timedelta(minutes=LOOKBACK_MINUTES),
        price_threshold_pct=PRICE_DEVIATION_THRESHOLD_PCT,
        fee_threshold_bps=FEE_SPIKE_THRESHOLD_BPS,
        rejection_threshold=REJECTION_THRESHOLD,
        interval_seconds=max(SCAN_INTERVAL_SECONDS, 1),
    )
    app.state.monitor = monitor
    if SCAN_INTERVAL_SECONDS > 0:
        app.state.monitor_task = asyncio.create_task(monitor.run_periodic())
    else:
        app.state.monitor_task = None


@app.on_event("shutdown")
async def _shutdown() -> None:
    monitor: AnomalyMonitor | None = getattr(app.state, "monitor", None)
    task: asyncio.Task | None = getattr(app.state, "monitor_task", None)

    if monitor:
        monitor.stop()

    if task:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@app.get("/metrics")
async def metrics_endpoint() -> Response:  # pragma: no cover - simple I/O
    payload = generate_latest()
    return Response(payload, media_type=CONTENT_TYPE_LATEST)


@app.get("/health", tags=["health"])
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def ready() -> Dict[str, Any]:
    monitor: AnomalyMonitor | None = getattr(app.state, "monitor", None)
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Anomaly monitor not initialised",
        )
    return {"status": "ready", "last_scan": monitor.last_run.isoformat() if monitor.last_run else None}


@app.get("/anomaly/status", response_model=AnomalyStatus)
async def anomaly_status() -> AnomalyStatus:
    monitor: AnomalyMonitor | None = getattr(app.state, "monitor", None)
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Anomaly monitor not available",
        )

    try:
        rows = monitor.fetch_recent_anomalies()
    except Exception as exc:  # pragma: no cover - DB errors
        logger.exception("Failed to load anomaly status")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to load anomaly status",
        ) from exc

    records: List[AnomalyRecord] = []
    for row in rows:
        metadata = row.get("metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        elif metadata is None:
            metadata = {}

        records.append(
            AnomalyRecord(
                anomaly_id=int(row["anomaly_id"]),
                detected_at=row["detected_at"],
                account_id=str(row["account_id"]),
                instrument=str(row["instrument"]),
                anomaly_type=str(row["anomaly_type"]),
                severity=str(row["severity"]),
                description=str(row["description"]),
                metadata=metadata,
                fill_id=row.get("fill_id"),
                order_id=row.get("order_id"),
            )
        )

    blocked_accounts = sorted(monitor.blocked_accounts)

    return AnomalyStatus(
        last_scan=monitor.last_run,
        lookback_minutes=int(monitor.lookback.total_seconds() // 60),
        interval_seconds=monitor.interval_seconds,
        blocked_accounts=blocked_accounts,
        anomalies=records,
    )


__all__ = [
    "app",
    "AnomalyMonitor",
    "AnomalyRecord",
    "AnomalyStatus",
]

