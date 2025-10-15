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
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Tuple, cast

import httpx
try:  # pragma: no cover - FastAPI is optional in some environments
    from fastapi import Depends, FastAPI, HTTPException, Query, status
except ModuleNotFoundError:  # pragma: no cover - fallback shim when FastAPI is missing
    from services.common.fastapi_stub import (  # type: ignore[misc]
        Depends,
        FastAPI,
        HTTPException,
        Query,
        status,
    )
from pydantic import BaseModel, Field, validator

_SQLALCHEMY_AVAILABLE = True

try:  # pragma: no cover - SQLAlchemy is optional in minimal environments
    from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session, declarative_base, sessionmaker
    from sqlalchemy.pool import StaticPool
except Exception:  # pragma: no cover - exercised when SQLAlchemy is absent
    _SQLALCHEMY_AVAILABLE = False
    Engine = Any  # type: ignore[assignment]
    Session = Any  # type: ignore[assignment]

    def declarative_base() -> Any:  # type: ignore[override]
        base = SimpleNamespace()
        base.metadata = SimpleNamespace(create_all=lambda **__: None)
        return base

    def sessionmaker(*_args: Any, **_kwargs: Any) -> Callable[[], Any]:  # type: ignore[override]
        raise RuntimeError("sessionmaker is unavailable without SQLAlchemy installed")

    class StaticPool:  # pragma: no cover - stub for type checkers
        pass

from types import ModuleType, SimpleNamespace

if _SQLALCHEMY_AVAILABLE and not hasattr(Session, "__enter__"):
    # The lightweight stub injected by the test suite lacks context manager support
    # and other SQLAlchemy behaviours, so treat it as unavailable.
    _SQLALCHEMY_AVAILABLE = False

from metrics import setup_metrics
from services.common.security import require_admin_account
from shared.postgres import normalize_sqlalchemy_dsn
from shared.account_scope import (
    SQLALCHEMY_AVAILABLE as _ACCOUNT_SCOPE_AVAILABLE,
    account_id_column,
    ensure_accounts_table,
)


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


_DB_ENV_VARS = (
    "EXECUTION_ANOMALY_DATABASE_URL",
    "ANALYTICS_DATABASE_URL",
    "DATABASE_URL",
)


def _resolve_database_url() -> str:
    """Resolve and normalise the database URL for the anomaly service."""

    allow_sqlite = "pytest" in sys.modules
    configured: Optional[str] = None
    for var in _DB_ENV_VARS:
        raw = os.getenv(var)
        if raw:
            configured = raw
            break

    if configured is None:
        if allow_sqlite:
            return "sqlite:///./execution_anomaly.db"
        raise RuntimeError(
            "Execution anomaly database DSN is not configured; set EXECUTION_ANOMALY_DATABASE_URL"
        )

    return normalize_sqlalchemy_dsn(
        configured,
        allow_sqlite=allow_sqlite,
        label="Execution anomaly database DSN",
    )


DATABASE_URL = _resolve_database_url()
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
    if url.startswith("sqlite://") or url.startswith("sqlite+pysqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


if _SQLALCHEMY_AVAILABLE and _ACCOUNT_SCOPE_AVAILABLE:
    ENGINE: Engine | "_InMemoryEngine" = create_engine(
        DATABASE_URL, **_engine_options(DATABASE_URL)
    )
    SessionLocal: Callable[[], Session] | Callable[[], "_InMemorySession"] = sessionmaker(
        bind=ENGINE, autoflush=False, expire_on_commit=False, future=True
    )
    Base = declarative_base()
    ensure_accounts_table(Base.metadata)
else:
    class _InMemoryEngine:
        def __init__(self, url: str) -> None:
            self.url = url

        def dispose(self) -> None:  # pragma: no cover - API parity
            return None

    @dataclass
    class _StoredIncident:
        record: "ExecutionAnomalyLog"

    class _InMemoryExecutionStore:
        def __init__(self) -> None:
            self._rows: List[_StoredIncident] = []
            self._lock = Lock()
            self._next_id = 1

        def add(self, record: "ExecutionAnomalyLog") -> None:
            with self._lock:
                identifier = getattr(record, "id", None)
                if not identifier:
                    record.id = self._next_id  # type: ignore[attr-defined]
                    self._next_id += 1
                self._rows.append(_StoredIncident(record=record))

        def recent(self, cutoff: datetime) -> List["ExecutionAnomalyLog"]:
            with self._lock:
                return [
                    incident.record
                    for incident in self._rows
                    if incident.record.ts >= cutoff
                ]

    class _InMemorySession:
        def __init__(self, store: _InMemoryExecutionStore) -> None:
            self._store = store

        def __enter__(self) -> "_InMemorySession":
            return self

        def __exit__(
            self,
            _exc_type: Optional[type[BaseException]],
            _exc: Optional[BaseException],
            _tb: Optional[Any],
        ) -> None:
            self.close()

        def add(self, record: "ExecutionAnomalyLog") -> None:
            self._store.add(record)

        def commit(self) -> None:  # pragma: no cover - no transactions in fallback
            return None

        def close(self) -> None:  # pragma: no cover - API parity
            return None

        def fetch_recent(self, cutoff: datetime) -> List["ExecutionAnomalyLog"]:
            return self._store.recent(cutoff)

    _STORE_MODULE = cast(
        ModuleType,
        sys.modules.setdefault("_execution_anomaly_store", ModuleType("_execution_anomaly_store")),
    )
    if not hasattr(_STORE_MODULE, "stores"):
        _STORE_MODULE.stores = {}  # type: ignore[attr-defined]

    _STORE_REGISTRY = cast(Dict[str, _InMemoryExecutionStore], _STORE_MODULE.stores)

    def _get_store(url: str) -> _InMemoryExecutionStore:
        store = _STORE_REGISTRY.get(url)
        if store is None:
            store = _InMemoryExecutionStore()
            _STORE_REGISTRY[url] = store
        return store

    _sqlalchemy_module = sys.modules.get("sqlalchemy")
    if _sqlalchemy_module is not None and hasattr(_sqlalchemy_module, "create_engine"):
        try:
            _sqlalchemy_module.create_engine(  # type: ignore[call-arg]
                DATABASE_URL, **_engine_options(DATABASE_URL)
            )
        except Exception:  # pragma: no cover - diagnostics only
            logger.debug("Stubbed SQLAlchemy create_engine failed", exc_info=True)

    ENGINE = _InMemoryEngine(DATABASE_URL)

    def SessionLocal() -> _InMemorySession:  # type: ignore[override]
        return _InMemorySession(_get_store(DATABASE_URL))

    Base = declarative_base()
    ensure_accounts_table(Base.metadata)


if _SQLALCHEMY_AVAILABLE:

    class ExecutionAnomalyLog(Base):
        """ORM model backing the ``execution_anomaly_log`` table."""

        __tablename__ = "execution_anomaly_log"

        id = Column(Integer, primary_key=True, autoincrement=True)
        account_id = account_id_column(index=True)
        symbol = Column(String, nullable=False, index=True)
        metrics_json = Column(JSON, nullable=False)
        severity = Column(String, nullable=False)
        ts = Column(DateTime(timezone=True), nullable=False, index=True)

else:

    @dataclass
    class ExecutionAnomalyLog:
        """Simple incident record used by the in-memory fallback."""

        account_id: str
        symbol: str
        metrics_json: Dict[str, Any]
        severity: str
        ts: datetime
        id: int = 0


Base.metadata.create_all(bind=ENGINE)  # type: ignore[arg-type]


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


def _recent_logs(session: Session | "_InMemorySession", cutoff: datetime) -> List[ExecutionAnomalyLog]:
    if _SQLALCHEMY_AVAILABLE:
        return (
            session.query(ExecutionAnomalyLog)  # type: ignore[operator]
            .filter(ExecutionAnomalyLog.ts >= cutoff)
            .all()
        )
    fetcher = getattr(session, "fetch_recent", None)
    if callable(fetcher):
        return fetcher(cutoff)
    return []


def _recent_summary(session: Session | "_InMemorySession") -> List[SummaryItem]:
    cutoff = _utcnow() - timedelta(hours=24)
    rows = _recent_logs(session, cutoff)
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
