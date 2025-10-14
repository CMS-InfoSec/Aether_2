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
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, TypeVar, cast

try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import Depends, FastAPI, HTTPException, Query, status
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[misc]
        Depends,
        FastAPI,
        HTTPException,
        Query,
        status,
    )
from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from services.alert_manager import RiskEvent, get_alert_manager_instance
from services.common.adapters import TimescaleAdapter
from services.common.security import require_admin_account
from shared.account_scope import account_id_column
from shared.postgres import normalize_sqlalchemy_dsn
from shared.pydantic_compat import BaseModel, Field


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


_DATABASE_URL_ENV_VAR = "ANOMALY_DATABASE_URL"


def _allow_sqlite_fallback() -> bool:
    """Return whether sqlite URLs are permitted (pytest contexts only)."""

    return "pytest" in sys.modules


def _database_url() -> str:
    """Return the configured PostgreSQL/Timescale connection string."""

    raw = os.getenv(_DATABASE_URL_ENV_VAR) or os.getenv("TIMESCALE_DSN")
    if raw is None:
        raise RuntimeError(
            "ANOMALY_DATABASE_URL or TIMESCALE_DSN must be set to a PostgreSQL/Timescale DSN"
        )

    candidate = raw.strip()
    if not candidate:
        raise RuntimeError("Anomaly service database DSN cannot be empty once configured.")

    allow_sqlite = _allow_sqlite_fallback()
    normalized: str = normalize_sqlalchemy_dsn(
        candidate,
        allow_sqlite=allow_sqlite,
        label="Anomaly service database DSN",
    )
    return normalized


DATABASE_URL = _database_url()


ENGINE: Engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
SessionLocal: sessionmaker[Session] = sessionmaker(
    bind=ENGINE, autoflush=False, expire_on_commit=False, future=True
)


if TYPE_CHECKING:

    class Base:  # pragma: no cover - typing stub for declarative base
        metadata: Any
        registry: Any

else:  # pragma: no cover - runtime declarative base when SQLAlchemy is available
    Base = declarative_base()
    Base.__doc__ = "Typed declarative base for the anomaly service ORM models."


class AnomalyLog(Base):
    """SQLAlchemy ORM model backing ``anomaly_log``."""

    __tablename__ = "anomaly_log"

    if TYPE_CHECKING:  # pragma: no cover - typing-only constructor metadata
        def __init__(
            self,
            *,
            account_id: str,
            anomaly_type: str,
            details_json: Dict[str, Any],
            ts: datetime,
        ) -> None:
            ...

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = account_id_column(index=True)
    anomaly_type = Column(String, nullable=False)
    details_json = Column(JSON, nullable=False, default=dict)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)


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


def _normalize_account(value: str) -> str:
    return value.strip().lower()


def _ensure_account_access(requested: str, authenticated: str) -> str:
    normalized_request = _normalize_account(requested)
    normalized_auth = _normalize_account(authenticated)
    if not normalized_request:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Account identifier must not be empty.",
        )
    if normalized_request != normalized_auth:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authenticated account does not match requested account.",
        )
    return normalized_request


def _with_normalized_account(request: ScanRequest, account_id: str) -> ScanRequest:
    """Return a copy of *request* with ``account_id`` normalised to *account_id*."""

    if hasattr(request, "model_dump"):
        payload = request.model_dump()
    else:
        payload = request.dict()
    payload["account_id"] = account_id
    return ScanRequest(**payload)


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


SessionFactory = Callable[[], Session]


class IncidentRepository:
    """Persistence layer for anomaly incidents."""

    def __init__(self, session_factory: SessionFactory = SessionLocal) -> None:
        self._session_factory = session_factory

    def log_incident(self, incident: Incident) -> None:
        record = AnomalyLog(
            account_id=incident.account_id,
            anomaly_type=incident.anomaly_type,
            details_json=incident.details,
            ts=incident.ts,
        )
        with self._session_factory() as session:
            session.add(record)
            session.commit()

    def recent_incidents(self, account_id: str, limit: int = 20) -> List[Incident]:
        with self._session_factory() as session:
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
        account_id = _normalize_account(request.account_id)
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
        normalized = _normalize_account(account_id)
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

RouteFn = TypeVar("RouteFn", bound=Callable[..., Any])


def _app_get(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``FastAPI.get`` to satisfy strict type checks."""

    return cast(Callable[[RouteFn], RouteFn], app.get(*args, **kwargs))


def _app_post(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``FastAPI.post`` to satisfy strict type checks."""

    return cast(Callable[[RouteFn], RouteFn], app.post(*args, **kwargs))


@_app_get("/anomaly/status", response_model=StatusResponse)
def get_status(
    account_id: str = Query(..., min_length=1),
    caller_account: str = Depends(require_admin_account),
) -> StatusResponse:
    """Return current anomaly status for the requested account."""

    normalized = _ensure_account_access(account_id, caller_account)
    return _coordinator.status(normalized)


@_app_post("/anomaly/scan", response_model=ScanResponse)
def post_scan(
    request: ScanRequest,
    caller_account: str = Depends(require_admin_account),
) -> ScanResponse:
    """Run anomaly detection for the specified account."""

    normalized = _ensure_account_access(request.account_id, caller_account)
    normalized_request = _with_normalized_account(request, normalized)
    return _coordinator.scan(normalized_request)
