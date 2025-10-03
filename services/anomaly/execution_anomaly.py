"""FastAPI service for monitoring execution quality anomalies.

The service ingests execution telemetry samples and computes rolling
z-scores over a configurable window for the following metrics:

* Order rejection rate
* Order cancel rate
* Partial fill ratio
* Slippage deviation in basis points (realized minus model expectation)

When any metric breaches the configured z-score threshold for a
configured number of consecutive samples an incident is recorded in the
``execution_anomaly_log`` table.  Optionally, the service can trigger
safe mode on critical incidents.
"""

from __future__ import annotations

import logging
import math
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Deque, Dict, Iterable, List, Optional, Tuple

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field, validator
from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from metrics import setup_metrics
from services.common.security import require_admin_account


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


DATABASE_URL = os.getenv(
    "EXECUTION_ANOMALY_DATABASE_URL", "sqlite:///./execution_anomaly.db"
)
WINDOW_SIZE = int(os.getenv("EXECUTION_ANOMALY_WINDOW", "60"))
Z_THRESHOLD = float(os.getenv("EXECUTION_ANOMALY_Z_THRESHOLD", "3.0"))
REQUIRED_CONSECUTIVE = int(os.getenv("EXECUTION_ANOMALY_CONSECUTIVE", "3"))
SAFE_MODE_ENABLED = os.getenv("EXECUTION_ANOMALY_SAFE_MODE_ENABLED", "false").lower() in {
    "1",
    "true",
    "yes",
}
SAFE_MODE_URL = os.getenv("EXECUTION_ANOMALY_SAFE_MODE_URL", "http://localhost:8000")
SAFE_MODE_ENDPOINT = os.getenv(
    "EXECUTION_ANOMALY_SAFE_MODE_ENDPOINT", "/safe_mode/enter"
)
SAFE_MODE_TIMEOUT = float(os.getenv("EXECUTION_ANOMALY_SAFE_MODE_TIMEOUT", "2.0"))


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------


def _engine_options(url: str) -> Dict[str, object]:
    options: Dict[str, object] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


ENGINE: Engine = create_engine(DATABASE_URL, **_engine_options(DATABASE_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)
Base = declarative_base()


class ExecutionAnomalyLog(Base):
    """ORM model backing the ``execution_anomaly_log`` table."""

    __tablename__ = "execution_anomaly_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, nullable=False, index=True)
    symbol = Column(String, nullable=False, index=True)
    metrics_json = Column(JSON, nullable=False)
    severity = Column(String, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)


Base.metadata.create_all(bind=ENGINE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class RollingStats:
    """Maintain a rolling window of values and compute statistics."""

    def __init__(self, maxlen: int) -> None:
        self._values: Deque[float] = deque(maxlen=maxlen)

    def push(self, value: float) -> None:
        self._values.append(float(value))

    def mean(self) -> float:
        if not self._values:
            return 0.0
        return float(sum(self._values) / len(self._values))

    def std(self) -> float:
        n = len(self._values)
        if n < 2:
            return 0.0
        mean = self.mean()
        variance = sum((value - mean) ** 2 for value in self._values) / (n - 1)
        return math.sqrt(variance)

    def zscore(self, value: float) -> float:
        std_dev = self.std()
        if std_dev == 0.0:
            return 0.0
        mean = self.mean()
        return (value - mean) / std_dev

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._values)


@dataclass
class MetricFlag:
    """Represents a metric breaching the anomaly threshold."""

    account_id: str
    symbol: str
    metric: str
    value: float
    zscore: float
    mean: float
    std_dev: float
    consecutive: int
    timestamp: datetime

    def to_dict(self) -> Dict[str, object]:
        return {
            "account_id": self.account_id,
            "symbol": self.symbol,
            "metric": self.metric,
            "value": self.value,
            "zscore": self.zscore,
            "mean": self.mean,
            "std_dev": self.std_dev,
            "consecutive": self.consecutive,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class MetricState:
    stats: RollingStats
    consecutive: int = 0
    flagged: bool = False
    last_flag: Optional[MetricFlag] = None


class ExecutionAnomalyMonitor:
    """Track rolling execution metrics and surface anomalies."""

    def __init__(
        self,
        *,
        window_size: int = WINDOW_SIZE,
        z_threshold: float = Z_THRESHOLD,
        required_consecutive: int = REQUIRED_CONSECUTIVE,
    ) -> None:
        self._window_size = max(1, window_size)
        self._z_threshold = max(0.0, z_threshold)
        self._required_consecutive = max(1, required_consecutive)
        self._states: Dict[str, Dict[Tuple[str, str], MetricState]] = defaultdict(dict)
        self._lock = Lock()

    def _state_for(self, metric: str, key: Tuple[str, str]) -> MetricState:
        metric_states = self._states[metric]
        state = metric_states.get(key)
        if state is None:
            state = MetricState(stats=RollingStats(self._window_size))
            metric_states[key] = state
        return state

    def observe(self, sample: "ExecutionSample") -> List[MetricFlag]:
        metrics = self._extract_metrics(sample)
        incidents: List[MetricFlag] = []

        with self._lock:
            for metric_name, value in metrics.items():
                key = (sample.account_id, sample.symbol)
                state = self._state_for(metric_name, key)
                state.stats.push(value)

                if len(state.stats) < self._required_consecutive:
                    state.consecutive = 0
                    state.flagged = False
                    state.last_flag = None
                    continue

                zscore = state.stats.zscore(value)
                mean = state.stats.mean()
                std_dev = state.stats.std()

                if abs(zscore) > self._z_threshold:
                    state.consecutive += 1
                    flag = MetricFlag(
                        account_id=sample.account_id,
                        symbol=sample.symbol,
                        metric=metric_name,
                        value=value,
                        zscore=zscore,
                        mean=mean,
                        std_dev=std_dev,
                        consecutive=state.consecutive,
                        timestamp=sample.timestamp,
                    )
                    state.last_flag = flag
                    if state.consecutive >= self._required_consecutive:
                        if not state.flagged:
                            incidents.append(flag)
                        state.flagged = True
                else:
                    state.consecutive = 0
                    state.flagged = False
                    state.last_flag = None
        return incidents

    def current_flags(self) -> List[MetricFlag]:
        with self._lock:
            flags: List[MetricFlag] = []
            for metric_states in self._states.values():
                for state in metric_states.values():
                    if state.flagged and state.last_flag is not None:
                        flags.append(state.last_flag)
            return sorted(
                flags,
                key=lambda flag: (flag.account_id, flag.symbol, flag.metric),
            )

    @staticmethod
    def _extract_metrics(sample: "ExecutionSample") -> Dict[str, float]:
        slippage_deviation = sample.realized_slippage_bps - sample.model_slippage_bps
        return {
            "rejection_rate": sample.rejection_rate,
            "cancel_rate": sample.cancel_rate,
            "partial_fill_ratio": sample.partial_fill_ratio,
            "slippage_deviation_bps": slippage_deviation,
        }


monitor = ExecutionAnomalyMonitor()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class ExecutionSample(BaseModel):
    account_id: str = Field(..., min_length=1, description="Trading account identifier")
    symbol: str = Field(..., min_length=1, description="Instrument symbol")
    timestamp: datetime = Field(default_factory=_utcnow)
    rejection_rate: float = Field(..., ge=0.0, le=1.0)
    cancel_rate: float = Field(..., ge=0.0, le=1.0)
    partial_fill_ratio: float = Field(..., ge=0.0, le=1.0)
    realized_slippage_bps: float = Field(..., description="Realized slippage in bps")
    model_slippage_bps: float = Field(
        0.0, description="Model-expected slippage in bps used as baseline"
    )

    @validator("timestamp", pre=True)
    def _ensure_timezone(cls, value: datetime) -> datetime:  # noqa: N805
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        raise ValueError("timestamp must be a datetime")


class IngestResponse(BaseModel):
    triggered_incidents: List[Dict[str, object]]
    safe_mode_triggered: bool


class SummaryItem(BaseModel):
    account_id: str
    symbol: str
    incident_count: int
    last_incident: Optional[datetime]


class StatusResponse(BaseModel):
    flags: List[Dict[str, object]]
    last_24h: List[SummaryItem]


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


app = FastAPI(title="Execution Anomaly Service")
setup_metrics(app, service_name="execution-anomaly")


def _log_incidents(incidents: Iterable[MetricFlag]) -> None:
    if not incidents:
        return
    with SessionLocal() as session:
        for incident in incidents:
            entry = ExecutionAnomalyLog(
                account_id=incident.account_id,
                symbol=incident.symbol,
                metrics_json=incident.to_dict(),
                severity="critical",
                ts=incident.timestamp,
            )
            session.add(entry)
        session.commit()


def _trigger_safe_mode() -> None:
    if not SAFE_MODE_ENABLED:
        return
    url = f"{SAFE_MODE_URL.rstrip('/')}{SAFE_MODE_ENDPOINT}"
    try:
        response = httpx.post(
            url,
            json={"reason": "execution_anomaly"},
            timeout=SAFE_MODE_TIMEOUT,
        )
        response.raise_for_status()
        logger.warning("Triggered safe mode due to execution anomaly")
    except httpx.HTTPError as exc:  # pragma: no cover - network failures are logged
        logger.error("Failed to trigger safe mode: %s", exc)


@app.post("/anomaly/execution/sample", response_model=IngestResponse)
def ingest_sample(
    sample: ExecutionSample,
    actor: str = Depends(require_admin_account),
) -> IngestResponse:
    if sample.account_id.strip().lower() != actor.strip().lower():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authenticated account is not authorized for the requested account.",
        )
    incidents = monitor.observe(sample)
    if incidents:
        _log_incidents(incidents)
        _trigger_safe_mode()
    return IngestResponse(
        triggered_incidents=[incident.to_dict() for incident in incidents],
        safe_mode_triggered=bool(incidents and SAFE_MODE_ENABLED),
    )


def _recent_summary(session: Session) -> List[SummaryItem]:
    cutoff = _utcnow() - timedelta(hours=24)
    rows = (
        session.query(ExecutionAnomalyLog)
        .filter(ExecutionAnomalyLog.ts >= cutoff)
        .all()
    )
    summary: Dict[Tuple[str, str], SummaryItem] = {}
    for row in rows:
        key = (row.account_id, row.symbol)
        item = summary.get(key)
        if item is None:
            item = SummaryItem(
                account_id=row.account_id,
                symbol=row.symbol,
                incident_count=0,
                last_incident=row.ts,
            )
            summary[key] = item
        item.incident_count += 1
        if item.last_incident is None or (row.ts and row.ts > item.last_incident):
            item.last_incident = row.ts
    return sorted(
        summary.values(),
        key=lambda item: (item.account_id, item.symbol),
    )


@app.get("/anomaly/execution/status", response_model=StatusResponse)
def execution_status(
    account_id: Optional[str] = Query(None, min_length=1),
    actor: str = Depends(require_admin_account),
) -> StatusResponse:
    scope_account = account_id or actor
    if scope_account.strip().lower() != actor.strip().lower():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authenticated account is not authorized for the requested account.",
        )

    try:
        flags = [
            flag.to_dict()
            for flag in monitor.current_flags()
            if flag.account_id.strip().lower() == scope_account.strip().lower()
        ]
        with SessionLocal() as session:
            summary = [
                item
                for item in _recent_summary(session)
                if item.account_id.strip().lower() == scope_account.strip().lower()
            ]
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.exception("Failed to fetch execution anomaly status")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    return StatusResponse(flags=flags, last_24h=summary)


__all__ = [
    "app",
    "ExecutionAnomalyLog",
    "ExecutionAnomalyMonitor",
    "ExecutionSample",
    "StatusResponse",
]
