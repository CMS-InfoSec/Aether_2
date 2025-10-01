"""FastAPI service that performs near real-time anomaly detection for trading accounts.

The service performs ad-hoc scans when requested via the ``/anomaly/scan`` endpoint
and surfaces the current status for an account via ``/anomaly/status``.  Detected
incidents are stored in the ``anomaly_log`` table and immediately trigger
Alertmanager notifications as well as kill switch activation for the impacted
account.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from services.alert_manager import RiskEvent, get_alert_manager_instance
from services.common.adapters import TimescaleAdapter


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


DATABASE_URL = os.getenv("ANOMALY_DATABASE_URL", "sqlite:///./anomaly.db")


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


class AnomalyLog(Base):
    """SQLAlchemy ORM model backing ``anomaly_log``."""

    __tablename__ = "anomaly_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, nullable=False, index=True)
    anomaly_type = Column(String, nullable=False)
    details_json = Column(JSON, nullable=False, default=dict)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)


Base.metadata.create_all(bind=ENGINE)


class ScanRequest(BaseModel):
    """Request payload for triggering a scan."""

    account_id: str = Field(..., min_length=1, description="Account identifier to scan")
    lookback_minutes: int = Field(
        15, ge=1, le=180, description="Observation lookback window in minutes"
    )


class Incident(BaseModel):
    """Response payload describing an anomaly incident."""

    account_id: str
    anomaly_type: str
    details: Dict[str, Any]
    ts: datetime


class ScanResponse(BaseModel):
    incidents: List[Incident]
    blocked: bool


class StatusResponse(BaseModel):
    account_id: str
    blocked: bool
    incidents: List[Incident]


@dataclass
class FillSnapshot:
    price: float
    mid_price: Optional[float]
    fee: Optional[float]
    quantity: Optional[float]
    recorded_at: datetime
    metadata: Dict[str, Any]


class AnomalyDetector:
    """Encapsulates anomaly detection heuristics."""

    price_deviation_threshold: float = 0.05
    fee_spike_multiplier: float = 2.0
    rejection_threshold: int = 3
    auth_failure_threshold: int = 3
    volume_spike_multiplier: float = 3.0

    def __init__(self, adapter_factory: type[TimescaleAdapter] = TimescaleAdapter) -> None:
        self._adapter_factory = adapter_factory

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scan_account(self, account_id: str, lookback_minutes: int) -> List[Incident]:
        """Scan a single account for anomalies."""

        now = datetime.now(timezone.utc)
        observation_start = now - timedelta(minutes=lookback_minutes)

        adapter = self._adapter_factory(account_id=account_id)
        events = adapter.events()

        fills = self._extract_fills(events.get("fills", []), observation_start)
        ack_events = events.get("acks", [])
        telemetry = events.get("events", [])

        incidents: List[Incident] = []
        incidents.extend(self._detect_price_outliers(account_id, fills))
        incidents.extend(self._detect_fee_spikes(account_id, fills))
        incidents.extend(
            self._detect_rejection_spikes(account_id, ack_events, observation_start)
        )
        incidents.extend(
            self._detect_api_abuse(account_id, telemetry, observation_start, fills)
        )

        return incidents

    # ------------------------------------------------------------------
    # Detection helpers
    # ------------------------------------------------------------------
    def _extract_fills(
        self, raw_fills: Iterable[Dict[str, Any]], observation_start: datetime
    ) -> List[FillSnapshot]:
        snapshots: List[FillSnapshot] = []
        for entry in raw_fills:
            recorded_at = entry.get("recorded_at")
            if not isinstance(recorded_at, datetime):
                continue
            if recorded_at < observation_start:
                continue

            payload = entry.get("payload") or {}
            price = self._to_float(payload.get("price"))
            if price is None:
                continue

            mid_price = self._to_float(payload.get("mid_price"))
            if mid_price is None:
                book = payload.get("book_snapshot") or {}
                mid_price = self._to_float(book.get("mid_price"))

            fee = self._to_float(payload.get("fee"))
            quantity = self._to_float(
                payload.get("quantity")
                if payload.get("quantity") is not None
                else payload.get("size")
            )

            snapshots.append(
                FillSnapshot(
                    price=float(price),
                    mid_price=mid_price,
                    fee=fee,
                    quantity=quantity,
                    recorded_at=recorded_at,
                    metadata={k: v for k, v in payload.items() if k not in {"price", "fee"}},
                )
            )
        return snapshots

    def _detect_price_outliers(
        self, account_id: str, fills: List[FillSnapshot]
    ) -> List[Incident]:
        incidents: List[Incident] = []
        for fill in fills:
            if not fill.mid_price or fill.mid_price <= 0:
                continue
            deviation = abs(fill.price - fill.mid_price) / fill.mid_price
            if deviation >= self.price_deviation_threshold:
                incidents.append(
                    Incident(
                        account_id=account_id,
                        anomaly_type="price_outlier",
                        details={
                            "fill_price": fill.price,
                            "mid_price": fill.mid_price,
                            "deviation_pct": round(deviation * 100, 4),
                        },
                        ts=fill.recorded_at,
                    )
                )
        return incidents

    def _detect_fee_spikes(
        self, account_id: str, fills: List[FillSnapshot]
    ) -> List[Incident]:
        incidents: List[Incident] = []
        if not fills:
            return incidents

        effective_rates: List[float] = []
        for fill in fills:
            if fill.fee is None:
                continue
            notional = self._estimate_notional(fill)
            if notional is None or notional <= 0:
                continue
            effective_rates.append(fill.fee / notional)

        if not effective_rates:
            return incidents

        baseline = median(effective_rates)
        if baseline <= 0:
            positive_rates = [rate for rate in effective_rates if rate > 0]
            baseline = min(positive_rates) if positive_rates else 0.0

        for fill in fills:
            if fill.fee is None:
                continue
            notional = self._estimate_notional(fill)
            if notional is None or notional <= 0:
                continue
            rate = fill.fee / notional
            if baseline > 0 and rate >= baseline * self.fee_spike_multiplier:
                incidents.append(
                    Incident(
                        account_id=account_id,
                        anomaly_type="fee_spike",
                        details={
                            "fee": fill.fee,
                            "notional": notional,
                            "effective_rate": rate,
                            "baseline_rate": baseline,
                        },
                        ts=fill.recorded_at,
                    )
                )
        return incidents

    def _detect_rejection_spikes(
        self,
        account_id: str,
        ack_events: Iterable[Dict[str, Any]],
        observation_start: datetime,
    ) -> List[Incident]:
        rejections = 0
        total = 0
        for entry in ack_events:
            recorded_at = entry.get("recorded_at")
            if not isinstance(recorded_at, datetime) or recorded_at < observation_start:
                continue
            payload = entry.get("payload") or {}
            status = str(payload.get("status") or payload.get("state") or "").lower()
            if not status:
                continue
            total += 1
            if "reject" in status:
                rejections += 1

        incidents: List[Incident] = []
        if rejections >= self.rejection_threshold and rejections >= max(total, 1) / 2:
            incidents.append(
                Incident(
                    account_id=account_id,
                    anomaly_type="rejection_spike",
                    details={
                        "rejections": rejections,
                        "total": total,
                        "rejection_ratio": rejections / max(total, 1),
                    },
                    ts=datetime.now(timezone.utc),
                )
            )
        return incidents

    def _detect_api_abuse(
        self,
        account_id: str,
        telemetry: Iterable[Dict[str, Any]],
        observation_start: datetime,
        fills: List[FillSnapshot],
    ) -> List[Incident]:
        auth_failures = 0
        incidents: List[Incident] = []
        volume_spike = self._detect_volume_spike(fills)

        for entry in telemetry:
            timestamp = entry.get("timestamp") or entry.get("recorded_at")
            if not isinstance(timestamp, datetime) or timestamp < observation_start:
                continue
            event_type = str(entry.get("event_type") or entry.get("type") or "").lower()
            payload = entry.get("payload") or {}
            description = str(payload.get("status") or payload.get("error") or "").lower()

            if "auth" in event_type and "fail" in event_type:
                auth_failures += 1
            elif "auth" in description and "fail" in description:
                auth_failures += 1

        if auth_failures >= self.auth_failure_threshold:
            incidents.append(
                Incident(
                    account_id=account_id,
                    anomaly_type="api_auth_failure",
                    details={"failures": auth_failures},
                    ts=datetime.now(timezone.utc),
                )
            )

        if volume_spike is not None:
            details, ts = volume_spike
            incidents.append(
                Incident(
                    account_id=account_id,
                    anomaly_type="volume_spike",
                    details=details,
                    ts=ts,
                )
            )

        return incidents

    def _detect_volume_spike(
        self, fills: List[FillSnapshot]
    ) -> Optional[tuple[Dict[str, Any], datetime]]:
        if len(fills) < 2:
            return None

        totals: List[float] = []
        for fill in fills:
            notional = self._estimate_notional(fill)
            if notional is None:
                continue
            totals.append(notional)

        if len(totals) < 2:
            return None

        baseline = median(totals[:-1]) if len(totals) > 2 else totals[0]
        latest = totals[-1]
        if baseline <= 0:
            return None

        if latest >= baseline * self.volume_spike_multiplier:
            ts = fills[-1].recorded_at
            return (
                {
                    "latest_notional": latest,
                    "baseline_notional": baseline,
                    "multiplier": latest / baseline,
                    "timestamp": ts.isoformat(),
                },
                ts,
            )
        return None

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _estimate_notional(fill: FillSnapshot) -> Optional[float]:
        if fill.quantity is not None:
            return abs(fill.price * fill.quantity)
        notional = fill.metadata.get("notional") if isinstance(fill.metadata, dict) else None
        return float(notional) if notional is not None else None


def median(values: Iterable[float]) -> float:
    sorted_values = sorted(values)
    if not sorted_values:
        return 0.0
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        return sorted_values[mid]
    return (sorted_values[mid - 1] + sorted_values[mid]) / 2.0


class IncidentRepository:
    """Persistence layer for anomaly incidents."""

    def __init__(self, session_factory: sessionmaker = SessionLocal) -> None:
        self._session_factory = session_factory

    def log_incident(self, incident: Incident) -> None:
        record = AnomalyLog(
            account_id=incident.account_id,
            anomaly_type=incident.anomaly_type,
            details_json=incident.details,
            ts=incident.ts,
        )
        with self._session_factory() as session:
            session: Session
            session.add(record)
            session.commit()

    def recent_incidents(self, account_id: str, limit: int = 20) -> List[Incident]:
        with self._session_factory() as session:
            session: Session
            query = (
                session.query(AnomalyLog)
                .filter(AnomalyLog.account_id == account_id)
                .order_by(AnomalyLog.ts.desc())
                .limit(limit)
            )
            return [
                Incident(
                    account_id=row.account_id,
                    anomaly_type=row.anomaly_type,
                    details=dict(row.details_json or {}),
                    ts=row.ts,
                )
                for row in query.all()
            ]


class ResponseFactory:
    """Coordinates detection, persistence, and mitigation actions."""

    def __init__(
        self,
        detector: Optional[AnomalyDetector] = None,
        repository: Optional[IncidentRepository] = None,
    ) -> None:
        self.detector = detector or AnomalyDetector()
        self.repository = repository or IncidentRepository()

    def scan(self, request: ScanRequest) -> ScanResponse:
        account_id = request.account_id.strip().lower()
        if not account_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Account identifier must not be empty.",
            )

        incidents = self.detector.scan_account(account_id, request.lookback_minutes)
        blocked = False
        if incidents:
            for incident in incidents:
                self.repository.log_incident(incident)
                self._raise_alert(incident)
                self._block_account(incident)
            blocked = True

        if blocked:
            logger.warning("Blocked trading for %s due to anomalies", account_id)
        else:
            logger.info("Scan completed with no anomalies for %s", account_id)

        blocked_state = self._is_blocked(account_id)
        return ScanResponse(incidents=incidents, blocked=blocked_state)

    def status(self, account_id: str) -> StatusResponse:
        normalized = account_id.strip().lower()
        if not normalized:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Account identifier must not be empty.",
            )

        incidents = self.repository.recent_incidents(normalized)
        blocked = self._is_blocked(normalized)
        return StatusResponse(account_id=normalized, blocked=blocked, incidents=incidents)

    # ------------------------------------------------------------------
    # Side effects
    # ------------------------------------------------------------------
    def _raise_alert(self, incident: Incident) -> None:
        manager = get_alert_manager_instance()
        if manager is None:
            logger.info("Alertmanager not configured; skipping alert for %s", incident.anomaly_type)
            return

        event = RiskEvent(
            event_type=f"anomaly.{incident.anomaly_type}",
            severity="high",
            description=json.dumps(incident.details),
            labels={"account_id": incident.account_id},
        )
        manager.handle_risk_event(event)

    def _block_account(self, incident: Incident) -> None:
        adapter = TimescaleAdapter(account_id=incident.account_id)
        adapter.set_kill_switch(
            engaged=True,
            reason=f"Anomaly detected: {incident.anomaly_type}",
            actor="anomaly-service",
        )

    def _is_blocked(self, account_id: str) -> bool:
        adapter = TimescaleAdapter(account_id=account_id)
        config = adapter.load_risk_config()
        return bool(config.get("kill_switch"))


app = FastAPI(title="Anomaly Detection Service")
_coordinator = ResponseFactory()


@app.get("/anomaly/status", response_model=StatusResponse)
def get_status(account_id: str = Query(..., min_length=1)) -> StatusResponse:
    """Return current anomaly status for the requested account."""

    return _coordinator.status(account_id)


@app.post("/anomaly/scan", response_model=ScanResponse)
def post_scan(request: ScanRequest) -> ScanResponse:
    """Run anomaly detection for the specified account."""

    return _coordinator.scan(request)
