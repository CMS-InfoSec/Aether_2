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
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, PositiveFloat
from sqlalchemy import Column, DateTime, String, Text, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------


Base = declarative_base()


class HitlQueueEntry(Base):
    """SQLAlchemy model backing the human review queue."""

    __tablename__ = "hitl_queue"

    intent_id = Column(String, primary_key=True)
    account_id = Column(String, nullable=False)
    trade_json = Column(Text, nullable=False)
    status = Column(String, nullable=False, default="pending")
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


DATABASE_URL_ENV = "HITL_DATABASE_URL"


def _database_url() -> str:
    url = os.getenv(DATABASE_URL_ENV) or os.getenv("TIMESCALE_DSN")
    if not url:
        raise RuntimeError(
            "HITL service requires a PostgreSQL/Timescale DSN via "
            f"{DATABASE_URL_ENV} or TIMESCALE_DSN",
        )

    normalized = url.strip()
    if normalized.startswith("postgresql://"):
        normalized = normalized.replace("postgresql://", "postgresql+psycopg://", 1)
    elif normalized.startswith("postgresql+psycopg://") or normalized.startswith("postgresql+psycopg2://"):
        pass
    else:
        raise RuntimeError(
            "HITL service requires a PostgreSQL/Timescale DSN via "
            f"{DATABASE_URL_ENV} or TIMESCALE_DSN",
        )

    return normalized


def _engine_options(_url: str) -> Dict[str, Any]:
    return {"future": True, "pool_pre_ping": True}


_DB_URL = _database_url()
ENGINE: Engine = create_engine(_DB_URL, **_engine_options(_DB_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)
Base.metadata.create_all(bind=ENGINE)


@contextmanager
def db_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:  # pragma: no cover - defensive cleanup
        session.rollback()
        raise
    finally:
        session.close()


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
        with db_session() as session:
            entry = session.get(HitlQueueEntry, intent_id)
            if entry is None:
                entry = HitlQueueEntry(
                    intent_id=intent_id,
                    account_id=account_id,
                    trade_json=json.dumps(payload),
                    status="pending",
                    ts=now,
                )
                session.add(entry)
            else:
                entry.account_id = account_id
                entry.trade_json = json.dumps(payload)
                entry.status = "pending"
                entry.ts = now

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
        with db_session() as session:
            entry = session.get(HitlQueueEntry, intent_id)
            if entry and entry.status == "pending":
                entry.status = "expired"
                entry.ts = datetime.now(timezone.utc)
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
    async def review_trade(self, request: ReviewRequest) -> ReviewResponse:
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

    def pending_trades(self) -> List[PendingTrade]:
        timeout = self.config.approval_timeout_seconds
        entries: List[PendingTrade] = []
        with db_session() as session:
            rows: Iterable[HitlQueueEntry] = session.query(HitlQueueEntry).filter(HitlQueueEntry.status == "pending").order_by(HitlQueueEntry.ts.asc())
            for row in rows:
                submitted_at = row.ts
                expires_at = submitted_at + timedelta(seconds=timeout) if timeout > 0 else None
                entries.append(
                    PendingTrade(
                        intent_id=row.intent_id,
                        account_id=row.account_id,
                        trade_details=json.loads(row.trade_json),
                        status=row.status,
                        submitted_at=submitted_at,
                        expires_at=expires_at,
                    )
                )
        return entries

    async def restore_pending_timeouts(self) -> None:
        timeout = self.config.approval_timeout_seconds
        if timeout == 0:
            return

        with db_session() as session:
            pending: List[tuple[str, datetime]] = [
                (row.intent_id, row.ts)
                for row in session.query(HitlQueueEntry)
                .filter(HitlQueueEntry.status == "pending")
                .all()
            ]

        now = datetime.now(timezone.utc)
        for intent_id, submitted_at in pending:
            elapsed = (now - submitted_at).total_seconds()
            remaining = timeout - elapsed
            if remaining <= 0:
                self._mark_expired(
                    intent_id,
                    reason="after exceeding the SLA while the service was offline",
                )
            else:
                self._schedule_timeout(intent_id, delay=remaining)

    def record_decision(self, request: ApprovalRequest) -> ApprovalResponse:
        with db_session() as session:
            entry = session.get(HitlQueueEntry, request.intent_id)
            if entry is None:
                raise HTTPException(status_code=404, detail="Intent not found")

            if entry.status == "expired":
                raise HTTPException(
                    status_code=409,
                    detail="Intent expired and was cancelled automatically.",
                )

            if entry.status != "pending":
                return ApprovalResponse(
                    intent_id=request.intent_id,
                    status=entry.status,
                    message=f"Intent already {entry.status}.",
                )

            entry.status = "approved" if request.approved else "denied"
            entry.ts = datetime.now(timezone.utc)

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
async def review_trade(request: ReviewRequest) -> ReviewResponse:
    return await service.review_trade(request)


@app.get("/hitl/pending", response_model=List[PendingTrade])
def get_pending_trades() -> List[PendingTrade]:
    return service.pending_trades()


@app.post("/hitl/approve", response_model=ApprovalResponse)
def approve_trade(request: ApprovalRequest) -> ApprovalResponse:
    return service.record_decision(request)


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
