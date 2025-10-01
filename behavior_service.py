"""Behavior monitoring service that surfaces trading anomalies.

The service exposes two endpoints:

* ``POST /behavior/scan`` – triggers an on-demand scan for a particular
  account.  The scan looks for trading behaviour irregularities using
  rolling z-scores to highlight sharp deviations from recent history.
* ``GET /behavior/status`` – returns the most recent incidents recorded
  for an account.

Detected anomalies are persisted to the ``behavior_log`` table and severe
incidents trigger an alert through Alertmanager.
"""

from __future__ import annotations

import logging
import math
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field
from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from services.alert_manager import RiskEvent, get_alert_manager_instance
from services.common.adapters import TimescaleAdapter


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


DATABASE_URL = os.getenv("BEHAVIOR_DATABASE_URL", "sqlite:///./behavior.db")


def _engine_options(url: str) -> Dict[str, Any]:
    options: Dict[str, Any] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


ENGINE: Engine = create_engine(DATABASE_URL, **_engine_options(DATABASE_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)
Base = declarative_base()


class BehaviorLog(Base):
    """SQLAlchemy model backing the ``behavior_log`` table."""

    __tablename__ = "behavior_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, nullable=False, index=True)
    anomaly_type = Column(String, nullable=False)
    details_json = Column(JSON, nullable=False, default=dict)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)


Base.metadata.create_all(bind=ENGINE)


class ScanRequest(BaseModel):
    account_id: str = Field(..., min_length=1, description="Account identifier to scan")
    lookback_minutes: int = Field(
        60,
        ge=5,
        le=480,
        description="Observation window in minutes used to compute rolling statistics.",
    )


class BehaviorIncident(BaseModel):
    account_id: str
    anomaly_type: str
    severity: str
    ts: datetime
    details: Dict[str, Any]

    class Config:
        json_encoders = {datetime: lambda value: value.isoformat()}


class ScanResponse(BaseModel):
    incidents: List[BehaviorIncident]


class StatusResponse(BaseModel):
    account_id: str
    incidents: List[BehaviorIncident]


@dataclass
class RollingStatistic:
    bucket_start: datetime
    bucket_width_minutes: int
    count: int
    rolling_mean: float
    rolling_std: float
    z_score: float
    window: int
    sample_size: int


@dataclass
class DetectorConfig:
    trading_bucket_minutes: int = 1
    trading_window: int = 5
    trading_threshold: float = 2.5
    trading_critical_threshold: float = 4.0

    kraken_bucket_minutes: int = 1
    kraken_window: int = 5
    kraken_threshold: float = 2.5
    kraken_critical_threshold: float = 4.0

    mode_bucket_minutes: int = 5
    mode_window: int = 4
    mode_threshold: float = 2.0
    mode_critical_threshold: float = 3.5


@dataclass
class BehaviorDetector:
    """Encapsulates heuristics for behaviour anomaly detection."""

    adapter_factory: type[TimescaleAdapter] = TimescaleAdapter
    config: DetectorConfig = DetectorConfig()

    def scan_account(self, account_id: str, lookback_minutes: int) -> List[BehaviorIncident]:
        adapter = self.adapter_factory(account_id=account_id)
        observation_start = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)

        events = adapter.events()
        telemetry = adapter.telemetry()

        ack_events = _within_window(events.get("acks", []), "recorded_at", observation_start)
        oms_events = _within_window(events.get("events", []), "timestamp", observation_start)
        telemetry_events = _within_window(telemetry, "timestamp", observation_start)

        incidents: List[BehaviorIncident] = []
        incidents.extend(self._detect_trading_frequency_spikes(account_id, ack_events))
        incidents.extend(self._detect_kraken_spam(account_id, ack_events, oms_events))
        incidents.extend(self._detect_mode_flipping(account_id, telemetry_events))
        return incidents

    # ------------------------------------------------------------------
    # Detection helpers
    # ------------------------------------------------------------------
    def _detect_trading_frequency_spikes(
        self, account_id: str, ack_events: Sequence[Mapping[str, Any]]
    ) -> List[BehaviorIncident]:
        timestamps = [
            _ensure_timezone(entry["recorded_at"]).astimezone(timezone.utc)
            for entry in ack_events
            if isinstance(entry.get("recorded_at"), datetime)
        ]

        if len(timestamps) < self.config.trading_window + 1:
            return []

        stats = self._rolling_z_scores(
            timestamps,
            bucket_minutes=self.config.trading_bucket_minutes,
            window=self.config.trading_window,
        )

        return [
            _incident_from_stat(
                account_id=account_id,
                anomaly_type="trading_frequency_spike",
                stat=stat,
                threshold=self.config.trading_threshold,
                critical_threshold=self.config.trading_critical_threshold,
                source="oms.acks",
            )
            for stat in stats
            if stat.z_score >= self.config.trading_threshold
        ]

    def _detect_kraken_spam(
        self,
        account_id: str,
        ack_events: Sequence[Mapping[str, Any]],
        oms_events: Sequence[Mapping[str, Any]],
    ) -> List[BehaviorIncident]:
        """Detect Kraken related spikes based on ack metadata and OMS events."""

        kraken_ack_timestamps: List[datetime] = []
        for entry in ack_events:
            recorded_at = entry.get("recorded_at")
            if not isinstance(recorded_at, datetime):
                continue
            transport = str(entry.get("transport", "")).lower()
            flags = str(entry.get("flags", "")).lower()
            if "kraken" in flags or transport in {"rest", "http", "kraken"}:
                kraken_ack_timestamps.append(_ensure_timezone(recorded_at).astimezone(timezone.utc))

        for entry in oms_events:
            ts = entry.get("timestamp") or entry.get("ts")
            if not isinstance(ts, datetime):
                continue
            event_type = str(entry.get("event_type") or entry.get("type") or "").lower()
            if "kraken" in event_type:
                kraken_ack_timestamps.append(_ensure_timezone(ts).astimezone(timezone.utc))

        if len(kraken_ack_timestamps) < self.config.kraken_window + 1:
            return []

        stats = self._rolling_z_scores(
            kraken_ack_timestamps,
            bucket_minutes=self.config.kraken_bucket_minutes,
            window=self.config.kraken_window,
        )

        return [
            _incident_from_stat(
                account_id=account_id,
                anomaly_type="kraken_spam",
                stat=stat,
                threshold=self.config.kraken_threshold,
                critical_threshold=self.config.kraken_critical_threshold,
                source="kraken.activity",
            )
            for stat in stats
            if stat.z_score >= self.config.kraken_threshold
        ]

    def _detect_mode_flipping(
        self, account_id: str, telemetry_events: Sequence[Mapping[str, Any]]
    ) -> List[BehaviorIncident]:
        mode_events: List[Tuple[datetime, str]] = []
        for entry in sorted(
            telemetry_events,
            key=lambda item: _ensure_timezone(item.get("timestamp")) or datetime.now(timezone.utc),
        ):
            ts_raw = entry.get("timestamp")
            ts = _ensure_timezone(ts_raw)
            if ts is None:
                continue
            payload = entry.get("payload") or {}
            mode = _extract_mode(payload)
            if mode is None:
                continue
            mode_events.append((ts.astimezone(timezone.utc), mode))

        if len(mode_events) < 2:
            return []

        flips: List[datetime] = []
        previous_mode = mode_events[0][1]
        for ts, mode in mode_events[1:]:
            if mode != previous_mode:
                flips.append(ts)
                previous_mode = mode

        if len(flips) < self.config.mode_window + 1:
            return []

        stats = self._rolling_z_scores(
            flips,
            bucket_minutes=self.config.mode_bucket_minutes,
            window=self.config.mode_window,
        )

        return [
            _incident_from_stat(
                account_id=account_id,
                anomaly_type="mode_flipping",
                stat=stat,
                threshold=self.config.mode_threshold,
                critical_threshold=self.config.mode_critical_threshold,
                source="telemetry.modes",
            )
            for stat in stats
            if stat.z_score >= self.config.mode_threshold
        ]

    # ------------------------------------------------------------------
    # Rolling statistics helpers
    # ------------------------------------------------------------------
    def _rolling_z_scores(
        self,
        timestamps: Sequence[datetime],
        *,
        bucket_minutes: int,
        window: int,
    ) -> List[RollingStatistic]:
        buckets = _bucket_counts(timestamps, bucket_minutes=bucket_minutes)
        if not buckets:
            return []

        counts = [count for _, count in buckets]
        stats: List[RollingStatistic] = []
        for index, (bucket_start, count) in enumerate(buckets):
            history = counts[max(0, index - window) : index]
            sample_size = len(history)
            if sample_size == 0:
                continue

            mean = sum(history) / sample_size
            variance = 0.0
            if sample_size > 1:
                variance = sum((value - mean) ** 2 for value in history) / (sample_size - 1)
            std_dev = math.sqrt(variance)
            if std_dev < 1e-6:
                std_dev = max(abs(mean), 1.0)

            z_score = (count - mean) / std_dev

            stats.append(
                RollingStatistic(
                    bucket_start=bucket_start,
                    bucket_width_minutes=bucket_minutes,
                    count=count,
                    rolling_mean=mean,
                    rolling_std=std_dev,
                    z_score=z_score,
                    window=window,
                    sample_size=sample_size,
                )
            )

        return stats


# ----------------------------------------------------------------------
# FastAPI wiring
# ----------------------------------------------------------------------


app = FastAPI(title="Behavior Monitoring Service", version="1.0.0")
detector = BehaviorDetector()


def _serialize_details(details: Mapping[str, Any]) -> Dict[str, Any]:
    serialized: Dict[str, Any] = {}
    for key, value in details.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif isinstance(value, (list, tuple)):
            serialized[key] = [
                item.isoformat() if isinstance(item, datetime) else item for item in value
            ]
        else:
            serialized[key] = value
    return serialized


def _persist_incident(session: Session, incident: BehaviorIncident) -> None:
    payload = {**incident.details, "severity": incident.severity}
    record = BehaviorLog(
        account_id=incident.account_id,
        anomaly_type=incident.anomaly_type,
        details_json=_serialize_details(payload),
        ts=incident.ts,
    )
    session.add(record)


def _emit_alert(incident: BehaviorIncident) -> None:
    if incident.severity != "critical":
        return
    manager = get_alert_manager_instance()
    if not manager:
        return
    description = (
        f"Behavioral anomaly {incident.anomaly_type} detected for {incident.account_id} "
        f"(z-score={incident.details.get('z_score')})."
    )
    event = RiskEvent(
        event_type=f"behavior.{incident.anomaly_type}",
        severity="critical",
        description=description,
        labels={
            "account_id": incident.account_id,
            "source": str(incident.details.get("source", "behavior")),
        },
    )
    manager.handle_risk_event(event)


@app.post("/behavior/scan", response_model=ScanResponse)
def scan_behavior(request: ScanRequest) -> ScanResponse:
    incidents = detector.scan_account(request.account_id, request.lookback_minutes)
    if not incidents:
        return ScanResponse(incidents=[])

    with SessionLocal() as session:
        for incident in incidents:
            _persist_incident(session, incident)
        session.commit()

    for incident in incidents:
        _emit_alert(incident)

    return ScanResponse(incidents=incidents)


@app.get("/behavior/status", response_model=StatusResponse)
def behavior_status(
    account_id: str = Query(..., min_length=1),
    limit: int = Query(50, ge=1, le=500),
) -> StatusResponse:
    with SessionLocal() as session:
        rows: List[BehaviorLog] = (
            session.query(BehaviorLog)
            .filter(BehaviorLog.account_id == account_id)
            .order_by(BehaviorLog.ts.desc())
            .limit(limit)
            .all()
        )

    incidents: List[BehaviorIncident] = []
    for row in rows:
        details = dict(row.details_json or {})
        severity = str(details.pop("severity", "info"))
        incidents.append(
            BehaviorIncident(
                account_id=row.account_id,
                anomaly_type=row.anomaly_type,
                severity=severity,
                ts=_ensure_timezone(row.ts) or datetime.now(timezone.utc),
                details=details,
            )
        )

    incidents.sort(key=lambda item: item.ts)
    return StatusResponse(account_id=account_id, incidents=incidents)


# ----------------------------------------------------------------------
# Utility helpers
# ----------------------------------------------------------------------


def _within_window(
    entries: Iterable[Mapping[str, Any]],
    timestamp_key: str,
    observation_start: datetime,
) -> List[Mapping[str, Any]]:
    scoped: List[Mapping[str, Any]] = []
    for entry in entries:
        ts = entry.get(timestamp_key)
        if not isinstance(ts, datetime):
            continue
        ts = _ensure_timezone(ts)
        if ts is None:
            continue
        if ts >= observation_start:
            scoped.append(entry)
    return scoped


def _ensure_timezone(value: Any) -> Optional[datetime]:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _bucket_counts(
    timestamps: Sequence[datetime], *, bucket_minutes: int
) -> List[Tuple[datetime, int]]:
    counter: Counter[datetime] = Counter()
    bucket_seconds = max(bucket_minutes, 1) * 60
    for ts in timestamps:
        ts_utc = ts.astimezone(timezone.utc)
        epoch_seconds = int(ts_utc.timestamp() // bucket_seconds) * bucket_seconds
        bucket_start = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        counter[bucket_start] += 1
    return sorted(counter.items(), key=lambda item: item[0])


def _extract_mode(payload: Mapping[str, Any]) -> Optional[str]:
    candidates = (
        payload.get("mode"),
        payload.get("model_mode"),
        payload.get("decision_mode"),
        payload.get("regime"),
    )
    for candidate in candidates:
        if candidate:
            return str(candidate)

    state = payload.get("state")
    if isinstance(state, Mapping):
        for key in ("mode", "regime", "model_mode"):
            value = state.get(key)
            if value:
                return str(value)

    metadata = payload.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("mode", "regime"):
            value = metadata.get(key)
            if value:
                return str(value)

    return None


def _incident_from_stat(
    *,
    account_id: str,
    anomaly_type: str,
    stat: RollingStatistic,
    threshold: float,
    critical_threshold: float,
    source: str,
) -> BehaviorIncident:
    severity = "critical" if stat.z_score >= critical_threshold else "high"
    details = {
        "bucket_start": stat.bucket_start.isoformat(),
        "bucket_width_minutes": stat.bucket_width_minutes,
        "count": stat.count,
        "rolling_mean": round(stat.rolling_mean, 4),
        "rolling_std": round(stat.rolling_std, 4),
        "z_score": round(stat.z_score, 4),
        "window": stat.window,
        "sample_size": stat.sample_size,
        "threshold": threshold,
        "critical_threshold": critical_threshold,
        "source": source,
    }
    return BehaviorIncident(
        account_id=account_id,
        anomaly_type=anomaly_type,
        severity=severity,
        ts=stat.bucket_start + timedelta(minutes=stat.bucket_width_minutes),
        details=details,
    )


__all__ = [
    "app",
    "BehaviorDetector",
    "BehaviorIncident",
    "ScanRequest",
    "ScanResponse",
    "StatusResponse",
]

