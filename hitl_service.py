"""FastAPI service exposing human-in-the-loop (HITL) trade approvals.

The service receives trade intents and evaluates them against a configurable
risk profile.  High-risk trades – defined as large notional sizes or trades
executed with low model confidence – are placed into a review queue awaiting a
human director's decision.  If no decision is recorded within the configured
SLA the trade is automatically cancelled to prevent stale intents from being
executed inadvertently.

Endpoints
---------
``POST /hitl/review``
    Evaluate a proposed trade and, if required, enqueue it for manual review.
``GET /hitl/pending``
    Retrieve the list of trade intents still awaiting a decision.
``POST /hitl/approve``
    Record a director's decision and update the trade's lifecycle state.

The queue persists in a managed PostgreSQL/Timescale database shared by all HITL
replicas and asynchronous tasks enforce the approval SLA.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional

try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import Depends, FastAPI, HTTPException
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[misc]
        Depends,
        FastAPI,
        HTTPException,
    )
from pydantic import BaseModel, Field, PositiveFloat

_SQLALCHEMY_AVAILABLE = True

try:  # pragma: no cover - optional dependency in production
    from sqlalchemy import Column, DateTime, String, Text, create_engine, select
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session, declarative_base, sessionmaker
except Exception:  # pragma: no cover - exercised in lightweight environments
    _SQLALCHEMY_AVAILABLE = False
    Column = DateTime = String = Text = None  # type: ignore[assignment]
    create_engine = None  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]
    Session = Any  # type: ignore[assignment]

    def declarative_base() -> Any:  # type: ignore[override]
        class _Base:
            metadata = type("_Meta", (), {"create_all": staticmethod(lambda **_: None)})()

        return _Base()

    def sessionmaker(**_: object) -> Any:  # type: ignore[override]
        raise RuntimeError("SQLAlchemy is unavailable in this environment")

from services.common.security import require_admin_account
from shared.account_scope import SQLALCHEMY_AVAILABLE as _ACCOUNT_SCOPE_AVAILABLE, account_id_column

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------


DATABASE_URL_ENV = "HITL_DATABASE_URL"
_SQLITE_FALLBACK_FLAG = "HITL_ALLOW_SQLITE_FOR_TESTS"


def _is_pytest_active() -> bool:
    return "pytest" in sys.modules


def _database_url() -> str:
    url = os.getenv(DATABASE_URL_ENV) or os.getenv("TIMESCALE_DSN")
    if not url:
        if not _SQLALCHEMY_AVAILABLE or _is_pytest_active():
            return "memory://hitl"
        raise RuntimeError(
            "HITL service requires a PostgreSQL/Timescale DSN via "
            f"{DATABASE_URL_ENV} or TIMESCALE_DSN",
        )

    normalized = url.strip()
    lowered = normalized.lower()

    if lowered.startswith("postgresql://"):
        normalized = "postgresql+psycopg://" + normalized.split("://", 1)[1]
        lowered = normalized.lower()

    allowed_prefixes = (
        "postgresql+psycopg://",
        "postgresql+psycopg2://",
        "timescaledb://",
        "timescaledb+psycopg://",
        "timescaledb+psycopg2://",
    )

    if lowered.startswith(allowed_prefixes):
        return normalized

    if lowered.startswith("sqlite"):
        if os.getenv(_SQLITE_FALLBACK_FLAG) == "1" or _is_pytest_active():
            logger.warning(
                "Allowing SQLite HITL database URL '%s' for tests (flag %s).",
                normalized,
                _SQLITE_FALLBACK_FLAG,
            )
            return normalized
        raise RuntimeError(
            f"SQLite HITL database URLs are only permitted when {_SQLITE_FALLBACK_FLAG}=1",
        )

    if normalized.startswith("memory://"):
        return normalized

    raise RuntimeError(
        f"HITL service requires a PostgreSQL/Timescale DSN via {DATABASE_URL_ENV} or TIMESCALE_DSN",
    )


@dataclass
class QueueEntry:
    intent_id: str
    account_id: str
    trade_json: str
    status: str
    ts: datetime


class _QueueStore:
    def upsert_pending(self, *, intent_id: str, account_id: str, trade_json: str, ts: datetime) -> None:
        raise NotImplementedError

    def list_pending(self) -> List[QueueEntry]:
        raise NotImplementedError

    def get(self, intent_id: str) -> Optional[QueueEntry]:
        raise NotImplementedError

    def update_status(
        self,
        intent_id: str,
        *,
        status: str,
        ts: datetime,
        allow_from: Iterable[str] | None = None,
    ) -> Optional[QueueEntry]:
        raise NotImplementedError


if _SQLALCHEMY_AVAILABLE and _ACCOUNT_SCOPE_AVAILABLE:

    Base = declarative_base()

    class HitlQueueEntry(Base):
        """SQLAlchemy model backing the human review queue."""

        __tablename__ = "hitl_queue"

        intent_id = Column(String, primary_key=True)
        account_id = account_id_column()
        trade_json = Column(Text, nullable=False)
        status = Column(String, nullable=False, default="pending")
        ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    def _engine_options(url: str) -> Dict[str, Any]:
        options: Dict[str, Any] = {"future": True, "pool_pre_ping": True}
        if url.startswith("sqlite"):
            options.setdefault("connect_args", {})["check_same_thread"] = False
        return options

    class _SqlAlchemyQueueStore(_QueueStore):
        def __init__(self, session_factory: Callable[[], Session]) -> None:
            self._session_factory = session_factory

        @contextmanager
        def _session(self) -> Iterator[Session]:
            session = self._session_factory()
            try:
                yield session
                commit = getattr(session, "commit", None)
                if callable(commit):
                    commit()
            except Exception:  # pragma: no cover - defensive cleanup
                rollback = getattr(session, "rollback", None)
                if callable(rollback):
                    rollback()
                raise
            finally:
                close = getattr(session, "close", None)
                if callable(close):
                    close()

        @staticmethod
        def _convert(entry: HitlQueueEntry) -> QueueEntry:
            return QueueEntry(
                intent_id=entry.intent_id,
                account_id=entry.account_id,
                trade_json=entry.trade_json,
                status=entry.status,
                ts=entry.ts,
            )

        def upsert_pending(self, *, intent_id: str, account_id: str, trade_json: str, ts: datetime) -> None:
            with self._session() as session:
                record = session.get(HitlQueueEntry, intent_id)
                if record is None:
                    record = HitlQueueEntry(
                        intent_id=intent_id,
                        account_id=account_id,
                        trade_json=trade_json,
                        status="pending",
                        ts=ts,
                    )
                    session.add(record)
                else:
                    record.account_id = account_id
                    record.trade_json = trade_json
                    record.status = "pending"
                    record.ts = ts

        def list_pending(self) -> List[QueueEntry]:
            with self._session() as session:
                query = getattr(session, "query", None)
                if callable(query):
                    rows: List[HitlQueueEntry] = (
                        query(HitlQueueEntry)
                        .filter(HitlQueueEntry.status == "pending")
                        .order_by(HitlQueueEntry.ts.asc())
                        .all()
                    )
                else:
                    statement = (
                        select(HitlQueueEntry)
                        .where(HitlQueueEntry.status == "pending")
                        .order_by(HitlQueueEntry.ts)
                    )
                    result = session.execute(statement)
                    stream = getattr(result, "scalars", None)
                    if callable(stream):
                        stream_result = stream()
                        if hasattr(stream_result, "all"):
                            rows = list(stream_result.all())
                        else:
                            try:
                                rows = list(stream_result)
                            except TypeError:
                                rows = []
                    elif hasattr(result, "all"):
                        rows = list(result.all())
                    else:
                        try:
                            rows = list(result)
                        except TypeError:
                            rows = []
                return [self._convert(row) for row in rows]

        def get(self, intent_id: str) -> Optional[QueueEntry]:
            with self._session() as session:
                record = session.get(HitlQueueEntry, intent_id)
                return self._convert(record) if record is not None else None

        def update_status(
            self,
            intent_id: str,
            *,
            status: str,
            ts: datetime,
            allow_from: Iterable[str] | None = None,
        ) -> Optional[QueueEntry]:
            with self._session() as session:
                record = session.get(HitlQueueEntry, intent_id)
                if record is None:
                    return None
                if allow_from is not None and record.status not in set(allow_from):
                    return None
                record.status = status
                record.ts = ts
                session.flush()
                return self._convert(record)

else:
    Base = None


class _InMemoryQueueStore(_QueueStore):
    def __init__(self) -> None:
        self._records: Dict[str, QueueEntry] = {}

    def reset(self) -> None:
        self._records.clear()

    def upsert_pending(self, *, intent_id: str, account_id: str, trade_json: str, ts: datetime) -> None:
        self._records[intent_id] = QueueEntry(
            intent_id=intent_id,
            account_id=account_id,
            trade_json=trade_json,
            status="pending",
            ts=ts,
        )

    def list_pending(self) -> List[QueueEntry]:
        return sorted(
            (entry for entry in self._records.values() if entry.status == "pending"),
            key=lambda entry: entry.ts,
        )

    def get(self, intent_id: str) -> Optional[QueueEntry]:
        entry = self._records.get(intent_id)
        return replace(entry) if entry is not None else None

    def update_status(
        self,
        intent_id: str,
        *,
        status: str,
        ts: datetime,
        allow_from: Iterable[str] | None = None,
    ) -> Optional[QueueEntry]:
        entry = self._records.get(intent_id)
        if entry is None:
            return None
        if allow_from is not None and entry.status not in set(allow_from):
            return None
        entry.status = status
        entry.ts = ts
        return replace(entry)


_IN_MEMORY_STORES: Dict[str, _InMemoryQueueStore] = {}


def _get_in_memory_store(url: str) -> _InMemoryQueueStore:
    store = _IN_MEMORY_STORES.get(url)
    if store is None:
        store = _InMemoryQueueStore()
        _IN_MEMORY_STORES[url] = store
    return store


class _InMemoryEngine:
    def __init__(self, url: str) -> None:
        self.url = url
        self.store = _get_in_memory_store(url)

    def dispose(self) -> None:  # pragma: no cover - API parity
        self.store.reset()


def _initialise_sqlalchemy_backend(url: str) -> tuple[Optional[Engine], Optional[Callable[[], Session]], Optional[_QueueStore]]:
    if not _SQLALCHEMY_AVAILABLE or url.startswith("memory://"):
        return None, None, None

    try:
        engine = create_engine(url, **_engine_options(url))  # type: ignore[arg-type]
        session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
        probe_session = session_factory()
        try:
            has_query = callable(getattr(probe_session, "query", None))
            has_get = callable(getattr(probe_session, "get", None))
            has_scalars = callable(getattr(probe_session, "scalars", None))
            has_execute = callable(getattr(probe_session, "execute", None))

            if not has_get and not has_scalars and not has_execute:
                raise RuntimeError("SQLAlchemy session missing query/get helpers")

            # Lightweight SQLAlchemy shims used in insecure-default environments expose the
            # ORM API surface area but do not actually persist state.  Probe the identity map
            # by inserting a synthetic queue entry and ensuring it can be retrieved via
            # ``Session.get`` before committing to the SQLAlchemy backend.
            probe_intent = "__hitl_sqlalchemy_probe__"
            can_persist = True
            add = getattr(probe_session, "add", None)
            flush = getattr(probe_session, "flush", None)
            get = getattr(probe_session, "get", None)
            rollback = getattr(probe_session, "rollback", None)
            if callable(add) and callable(get):
                entry = HitlQueueEntry(
                    intent_id=probe_intent,
                    account_id="probe",
                    trade_json="{}",
                    status="pending",
                    ts=datetime.now(timezone.utc),
                )
                try:
                    add(entry)
                    if callable(flush):
                        flush()
                    can_persist = get(HitlQueueEntry, probe_intent) is not None
                finally:
                    if callable(rollback):
                        rollback()

            if not can_persist:
                raise RuntimeError("SQLAlchemy session is unable to persist HITL queue entries")
        finally:
            close = getattr(probe_session, "close", None)
            if callable(close):
                close()

        Base.metadata.create_all(bind=engine)  # type: ignore[call-arg]
        return engine, session_factory, _SqlAlchemyQueueStore(session_factory)
    except Exception:  # pragma: no cover - diagnostic
        logger.warning(
            "Failed to initialise SQLAlchemy backend for HITL queue; using in-memory store instead.",
            exc_info=True,
        )
        return None, None, None


_DB_URL = _database_url()

engine, session_factory, queue_store = _initialise_sqlalchemy_backend(_DB_URL)

if engine is not None and session_factory is not None and queue_store is not None:
    ENGINE = engine
    SessionLocal = session_factory
    _QUEUE_STORE = queue_store
else:
    if engine is not None:
        try:
            engine.dispose()
        except Exception:  # pragma: no cover - defensive cleanup
            logger.debug("Failed to dispose partially initialised HITL engine", exc_info=True)
    if not _SQLALCHEMY_AVAILABLE:
        logger.warning("SQLAlchemy is unavailable; using in-memory HITL queue store")
    ENGINE = _InMemoryEngine(_DB_URL)
    SessionLocal = None  # type: ignore[assignment]
    _QUEUE_STORE = _get_in_memory_store(_DB_URL)


# ---------------------------------------------------------------------------
# Configuration and domain models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HitlConfig:
    """Thresholds driving the classification of high-risk trades."""

    notional_threshold: float = float(os.getenv("HITL_NOTIONAL_THRESHOLD", "500000"))
    confidence_threshold: float = float(os.getenv("HITL_CONFIDENCE_THRESHOLD", "0.6"))
    approval_timeout_seconds: int = int(os.getenv("HITL_APPROVAL_TIMEOUT", "180"))

    def __post_init__(self) -> None:
        if self.notional_threshold <= 0:
            raise ValueError("HITL notional threshold must be positive")
        if not (0.0 <= self.confidence_threshold <= 1.0):
            raise ValueError("Model confidence threshold must be between 0 and 1")
        if self.approval_timeout_seconds < 0:
            raise ValueError("Approval timeout must be non-negative")


class TradeDetails(BaseModel):
    """Canonical representation of a trade intent for risk review."""

    symbol: str = Field(..., min_length=1)
    side: str = Field(..., min_length=3)
    notional: PositiveFloat
    model_confidence: float = Field(..., ge=0.0, le=1.0)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ReviewRequest(BaseModel):
    account_id: str = Field(..., min_length=1)
    intent_id: str = Field(..., min_length=1)
    trade_details: TradeDetails


class ReviewResponse(BaseModel):
    intent_id: str
    requires_approval: bool
    status: str
    message: str


class PendingTrade(BaseModel):
    intent_id: str
    account_id: str
    trade_details: Dict[str, Any]
    status: str
    submitted_at: datetime
    expires_at: Optional[datetime]


class ApprovalRequest(BaseModel):
    intent_id: str = Field(..., min_length=1)
    approved: bool
    reviewer: Optional[str] = None


class ApprovalResponse(BaseModel):
    intent_id: str
    status: str
    message: str


# ---------------------------------------------------------------------------
# Service implementation
# ---------------------------------------------------------------------------


def _normalize_account(account: str) -> str:
    return account.strip().lower()


class HitlService:
    """Encapsulates trade evaluation logic and queue management."""

    def __init__(self, config: HitlConfig) -> None:
        self.config = config
        self._timeout_tasks: Dict[str, asyncio.Task[None]] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------
    # Risk evaluation helpers
    # ------------------------------
    def is_high_risk(self, trade: TradeDetails) -> bool:
        return trade.notional >= self.config.notional_threshold or trade.model_confidence <= self.config.confidence_threshold

    def _persist_entry(self, *, intent_id: str, account_id: str, trade: TradeDetails) -> None:
        payload = trade.model_dump()
        now = datetime.now(timezone.utc)
        _QUEUE_STORE.upsert_pending(
            intent_id=intent_id,
            account_id=account_id,
            trade_json=json.dumps(payload),
            ts=now,
        )

    def _schedule_timeout(self, intent_id: str, *, delay: Optional[float] = None) -> None:
        timeout = self.config.approval_timeout_seconds if delay is None else delay
        if timeout <= 0:
            return

        existing = self._timeout_tasks.get(intent_id)
        if existing and not existing.done():
            existing.cancel()

        loop = asyncio.get_running_loop()
        self._loop = loop
        task = loop.create_task(self._expire_after_timeout(intent_id, timeout))
        self._timeout_tasks[intent_id] = task

    def _mark_expired(self, intent_id: str, *, reason: str) -> None:
        updated = _QUEUE_STORE.update_status(
            intent_id,
            status="expired",
            ts=datetime.now(timezone.utc),
            allow_from=("pending",),
        )
        if updated is not None:
            logger.info("HITL intent %s expired %s", intent_id, reason)

    async def _expire_after_timeout(self, intent_id: str, timeout: float) -> None:
        try:
            await asyncio.sleep(timeout)
            self._mark_expired(intent_id, reason="without approval")
        except asyncio.CancelledError:  # pragma: no cover - cooperative cancellation
            pass
        finally:
            self._timeout_tasks.pop(intent_id, None)

    def _cancel_timeout(self, intent_id: str) -> None:
        task = self._timeout_tasks.pop(intent_id, None)
        if task and not task.done():
            loop = self._loop
            if loop and loop.is_running():
                loop.call_soon_threadsafe(task.cancel)
            else:  # pragma: no cover - defensive fallback
                task.cancel()

    # ------------------------------
    # Public API
    # ------------------------------
    async def review_trade(self, request: ReviewRequest, *, actor: str) -> ReviewResponse:
        if _normalize_account(request.account_id) != _normalize_account(actor):
            raise HTTPException(
                status_code=403,
                detail="Account mismatch between authenticated session and payload.",
            )

        trade = request.trade_details
        if not self.is_high_risk(trade):
            logger.info(
                "Intent %s auto-approved (notional=%.2f, confidence=%.2f)",
                request.intent_id,
                trade.notional,
                trade.model_confidence,
            )
            return ReviewResponse(
                intent_id=request.intent_id,
                requires_approval=False,
                status="auto-approved",
                message="Trade intent falls within automated risk thresholds.",
            )

        self._persist_entry(intent_id=request.intent_id, account_id=request.account_id, trade=trade)
        if self.config.approval_timeout_seconds > 0:
            self._schedule_timeout(request.intent_id)

        logger.info(
            "Intent %s flagged for human review (notional=%.2f, confidence=%.2f)",
            request.intent_id,
            trade.notional,
            trade.model_confidence,
        )
        return ReviewResponse(
            intent_id=request.intent_id,
            requires_approval=True,
            status="pending",
            message="Trade requires director approval before execution.",
        )

    def pending_trades(self, *, account_id: str) -> List[PendingTrade]:
        timeout = self.config.approval_timeout_seconds
        entries: List[PendingTrade] = []
        for record in _QUEUE_STORE.list_pending():
            if _normalize_account(record.account_id) != _normalize_account(account_id):
                continue
            submitted_at = record.ts
            expires_at = submitted_at + timedelta(seconds=timeout) if timeout > 0 else None
            entries.append(
                PendingTrade(
                    intent_id=record.intent_id,
                    account_id=record.account_id,
                    trade_details=json.loads(record.trade_json),
                    status=record.status,
                    submitted_at=submitted_at,
                    expires_at=expires_at,
                )
            )
        return entries

    async def restore_pending_timeouts(self) -> None:
        timeout = self.config.approval_timeout_seconds
        if timeout == 0:
            return

        pending = _QUEUE_STORE.list_pending()

        now = datetime.now(timezone.utc)
        for record in pending:
            intent_id = record.intent_id
            submitted_at = record.ts
            elapsed = (now - submitted_at).total_seconds()
            remaining = timeout - elapsed
            if remaining <= 0:
                self._mark_expired(
                    intent_id,
                    reason="after exceeding the SLA while the service was offline",
                )
            else:
                self._schedule_timeout(intent_id, delay=remaining)

    def record_decision(self, request: ApprovalRequest, *, actor: str) -> ApprovalResponse:
        entry = _QUEUE_STORE.get(request.intent_id)
        if entry is None:
            raise HTTPException(status_code=404, detail="Intent not found")

        if entry.status == "expired":
            raise HTTPException(
                status_code=409,
                detail="Intent expired and was cancelled automatically.",
            )

        if _normalize_account(entry.account_id) != _normalize_account(actor):
            raise HTTPException(
                status_code=403,
                detail="Account mismatch between authenticated session and intent owner.",
            )

        if entry.status != "pending":
            return ApprovalResponse(
                intent_id=request.intent_id,
                status=entry.status,
                message=f"Intent already {entry.status}.",
            )

        _QUEUE_STORE.update_status(
            request.intent_id,
            status="approved" if request.approved else "denied",
            ts=datetime.now(timezone.utc),
            allow_from=("pending",),
        )
        self._cancel_timeout(request.intent_id)
        action = "approved" if request.approved else "denied"
        logger.info("Intent %s %s by reviewer %s", request.intent_id, action, request.reviewer or "unknown")
        return ApprovalResponse(
            intent_id=request.intent_id,
            status=action,
            message=f"Intent {action} successfully recorded.",
        )


# ---------------------------------------------------------------------------
# FastAPI wiring
# ---------------------------------------------------------------------------


config = HitlConfig()
service = HitlService(config)
app = FastAPI(title="Aether HITL Service")


@app.post("/hitl/review", response_model=ReviewResponse)
async def review_trade(
    request: ReviewRequest, actor: str = Depends(require_admin_account)
) -> ReviewResponse:
    return await service.review_trade(request, actor=actor)


@app.get("/hitl/pending", response_model=List[PendingTrade])
def get_pending_trades(actor: str = Depends(require_admin_account)) -> List[PendingTrade]:
    return service.pending_trades(account_id=actor)


@app.post("/hitl/approve", response_model=ApprovalResponse)
def approve_trade(
    request: ApprovalRequest, actor: str = Depends(require_admin_account)
) -> ApprovalResponse:
    return service.record_decision(request, actor=actor)


@app.on_event("startup")
async def _reschedule_pending_timeouts() -> None:
    await service.restore_pending_timeouts()


__all__ = [
    "app",
    "HitlService",
    "HitlConfig",
    "ReviewRequest",
    "ReviewResponse",
    "PendingTrade",
    "ApprovalRequest",
    "ApprovalResponse",
]
