"""FastAPI service for managing manual trade overrides."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from enum import Enum
from typing import Iterable, List, Optional

from fastapi import Depends, FastAPI, Header, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from services.common.security import require_admin_account

try:  # pragma: no cover - import guarded for optional dependency
    from common.utils.audit_logger import hash_ip, log_audit
except Exception:  # pragma: no cover - degrade gracefully if audit logger unavailable
    log_audit = None  # type: ignore[assignment]

    def hash_ip(_: Optional[str]) -> Optional[str]:  # type: ignore[override]
        return None


LOGGER = logging.getLogger("override_service")


Base = declarative_base()


class OverrideDecision(str, Enum):
    APPROVE = "approve"
    REJECT = "reject"


def _database_url() -> str:
    return os.getenv("OVERRIDE_DATABASE_URL", "sqlite:///./override.db")


def _engine() -> Engine:
    url = _database_url()
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, future=True, connect_args=connect_args)


ENGINE = _engine()
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)


class OverrideLogEntry(Base):
    __tablename__ = "override_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    intent_id = Column(String, nullable=False, index=True)
    account_id = Column(String, nullable=False, index=True)
    actor = Column(String, nullable=False)
    decision = Column(String, nullable=False)
    reason = Column(Text, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), index=True)


Base.metadata.create_all(bind=ENGINE)


def get_session() -> Iterable[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:  # pragma: no cover - defensive cleanup
        session.rollback()
        raise
    finally:
        session.close()


def _normalize_account(account_id: Optional[str]) -> str:
    if not account_id:
        return "unknown"
    normalized = account_id.strip().lower()
    return normalized or "unknown"


async def get_actor(x_actor: Optional[str] = Header(None), x_user: Optional[str] = Header(None)) -> str:
    for value in (x_actor, x_user, os.getenv("OVERRIDE_ACTOR")):
        if value:
            actor = value.strip()
            if actor:
                return actor
    return "unknown"


class OverrideRequest(BaseModel):
    intent_id: str = Field(..., min_length=1, description="Unique identifier of the trade intent")
    decision: OverrideDecision = Field(..., description="Director decision applied to the trade")
    reason: str = Field(..., min_length=1, description="Explanation for the override decision")


class OverrideRecord(BaseModel):
    intent_id: str
    account_id: str
    actor: str
    decision: OverrideDecision
    reason: str
    ts: datetime

    model_config = {"from_attributes": True}


class OverrideHistoryResponse(BaseModel):
    overrides: List[OverrideRecord]


app = FastAPI(title="Override Service", version="1.0.0")


def _latest_entry(intent_id: str, session: Session) -> Optional[OverrideLogEntry]:
    stmt = (
        select(OverrideLogEntry)
        .where(OverrideLogEntry.intent_id == intent_id)
        .order_by(OverrideLogEntry.ts.desc())
        .limit(1)
    )
    result = session.execute(stmt).scalars().first()
    return result


def latest_override(intent_id: str) -> Optional[OverrideRecord]:
    session = SessionLocal()
    try:
        entry = _latest_entry(intent_id, session)
        if entry is None:
            return None
        session.expunge(entry)
        return OverrideRecord.model_validate(entry)
    finally:
        session.close()


@app.post("/override/trade", response_model=OverrideRecord, status_code=status.HTTP_201_CREATED)
def record_override(
    payload: OverrideRequest,
    request: Request,
    session: Session = Depends(get_session),
    admin_account: str = Depends(require_admin_account),
    account_id: Optional[str] = Header(None, alias="X-Account-ID"),
) -> OverrideRecord:
    normalized_account = _normalize_account(account_id or admin_account)
    entry = OverrideLogEntry(
        intent_id=payload.intent_id,
        account_id=normalized_account,
        actor=admin_account,
        decision=payload.decision.value,
        reason=payload.reason,
        ts=datetime.now(timezone.utc),
    )
    session.add(entry)
    session.flush()

    if log_audit is not None:
        try:
            ip_hash = hash_ip(request.client.host if request.client else None)
            log_audit(
                actor=admin_account,
                action="override.human_decision",
                entity=payload.intent_id,
                before={},
                after={
                    "decision": payload.decision.value,
                    "reason": payload.reason,
                    "account_id": normalized_account,
                    "source": "human decision",
                },
                ip_hash=ip_hash,
            )
        except Exception:  # pragma: no cover - audit logging best effort
            LOGGER.exception("Failed to record audit log for override %s", payload.intent_id)

    session.refresh(entry)
    return OverrideRecord.model_validate(entry)


@app.get("/override/history", response_model=OverrideHistoryResponse)
def override_history(
    account_id: Optional[str] = Query(None, description="Filter overrides for a specific account"),
    session: Session = Depends(get_session),
    _: str = Depends(require_admin_account),
) -> OverrideHistoryResponse:
    stmt = select(OverrideLogEntry).order_by(OverrideLogEntry.ts.desc())
    if account_id:
        stmt = stmt.where(OverrideLogEntry.account_id == _normalize_account(account_id))
    entries = session.execute(stmt).scalars().all()
    records = [OverrideRecord.model_validate(entry) for entry in entries]
    return OverrideHistoryResponse(overrides=records)


__all__ = [
    "app",
    "latest_override",
    "OverrideDecision",
    "OverrideRecord",
]

