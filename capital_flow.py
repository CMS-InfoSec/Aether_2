"""FastAPI service exposing capital flow management endpoints.

This module provides lightweight APIs for recording deposits and withdrawals
against trading accounts while maintaining a running NAV baseline per account.
The data is persisted in a SQLite database by default (configurable via the
``CAPITAL_FLOW_DATABASE_URL`` environment variable), making it suitable for
local development and unit testing while remaining portable to production
deployments backed by PostgreSQL.
"""

from __future__ import annotations

import enum
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Generator, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field, PositiveFloat
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------


DEFAULT_DATABASE_URL = "sqlite:///./capital_flows.db"


def _create_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine with sensible defaults for SQLite."""

    options: dict[str, object] = {"future": True}
    if database_url.startswith("sqlite"):
        connect_args = {"check_same_thread": False}
        options["connect_args"] = connect_args
        if ":memory:" in database_url:
            options["poolclass"] = StaticPool
    return create_engine(database_url, **options)


DATABASE_URL = os.getenv("CAPITAL_FLOW_DATABASE_URL", DEFAULT_DATABASE_URL)
ENGINE = _create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

Base = declarative_base()


class CapitalFlowType(str, enum.Enum):
    """Supported capital flow actions."""

    DEPOSIT = "deposit"
    WITHDRAW = "withdraw"


class CapitalFlowRecord(Base):
    """ORM model for persisted capital flows."""

    __tablename__ = "capital_flows"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, nullable=False, index=True)
    type = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class NavBaselineRecord(Base):
    """Per-account NAV baseline state."""

    __tablename__ = "nav_baselines"

    account_id = Column(String, primary_key=True)
    currency = Column(String, nullable=False)
    baseline = Column(Float, nullable=False, default=0.0)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


Base.metadata.create_all(bind=ENGINE)


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------


def get_session() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session for request-scoped use."""

    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# API schemas
# ---------------------------------------------------------------------------


class CapitalFlowRequest(BaseModel):
    account_id: str = Field(..., min_length=1, description="Unique account identifier")
    amount: PositiveFloat = Field(..., description="Absolute amount of the flow in account currency")
    currency: str = Field(..., min_length=1, description="ISO currency code for the flow")


class CapitalFlowResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    account_id: str
    type: CapitalFlowType
    amount: float
    currency: str
    ts: datetime
    nav_baseline: float = Field(..., description="NAV baseline after applying the flow")


class FlowHistoryResponse(BaseModel):
    flows: list[CapitalFlowResponse]


@dataclass(slots=True)
class _FlowApplicationResult:
    flow: CapitalFlowRecord
    baseline: NavBaselineRecord


# ---------------------------------------------------------------------------
# Business logic
# ---------------------------------------------------------------------------


def _ensure_currency_consistency(baseline: NavBaselineRecord, currency: str) -> None:
    if baseline.currency != currency:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Currency mismatch between requested flow and NAV baseline: "
                f"baseline={baseline.currency}, flow={currency}"
            ),
        )


def _apply_flow(
    session: Session, payload: CapitalFlowRequest, flow_type: CapitalFlowType
) -> _FlowApplicationResult:
    timestamp = _utcnow()
    baseline = session.get(NavBaselineRecord, payload.account_id)

    if baseline is None:
        if flow_type is CapitalFlowType.WITHDRAW:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot withdraw against an unknown NAV baseline",
            )
        baseline = NavBaselineRecord(
            account_id=payload.account_id,
            currency=payload.currency,
            baseline=0.0,
            updated_at=timestamp,
        )
        session.add(baseline)
        session.flush()
    else:
        _ensure_currency_consistency(baseline, payload.currency)

    delta = float(payload.amount)
    if flow_type is CapitalFlowType.WITHDRAW:
        new_baseline = baseline.baseline - delta
        if new_baseline < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Withdrawal amount exceeds available NAV baseline",
            )
    else:
        new_baseline = baseline.baseline + delta

    baseline.baseline = new_baseline
    baseline.currency = payload.currency
    baseline.updated_at = timestamp

    flow = CapitalFlowRecord(
        account_id=payload.account_id,
        type=flow_type.value,
        amount=delta,
        currency=payload.currency,
        ts=timestamp,
    )
    session.add(flow)
    session.flush()

    return _FlowApplicationResult(flow=flow, baseline=baseline)


def _record_flow(
    session: Session, payload: CapitalFlowRequest, flow_type: CapitalFlowType
) -> CapitalFlowResponse:
    try:
        result = _apply_flow(session, payload, flow_type)
    except HTTPException:
        session.rollback()
        raise
    except Exception:  # pragma: no cover - defensive catch to ensure rollback
        session.rollback()
        raise
    else:
        session.commit()
        return CapitalFlowResponse(
            id=result.flow.id,
            account_id=result.flow.account_id,
            type=flow_type,
            amount=result.flow.amount,
            currency=result.flow.currency,
            ts=result.flow.ts,
            nav_baseline=result.baseline.baseline,
        )


def _serialize_flow(record: CapitalFlowRecord, baseline_lookup: dict[str, float]) -> CapitalFlowResponse:
    nav_baseline = baseline_lookup.get(record.account_id, 0.0)
    return CapitalFlowResponse(
        id=record.id,
        account_id=record.account_id,
        type=CapitalFlowType(record.type),
        amount=record.amount,
        currency=record.currency,
        ts=record.ts,
        nav_baseline=nav_baseline,
    )


# ---------------------------------------------------------------------------
# FastAPI wiring
# ---------------------------------------------------------------------------


app = FastAPI(title="Capital Flow Service", version="1.0.0")


@app.post(
    "/finance/deposit",
    response_model=CapitalFlowResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Record a capital deposit",
)
def record_deposit(
    payload: CapitalFlowRequest, session: Session = Depends(get_session)
) -> CapitalFlowResponse:
    """Persist a deposit and update the NAV baseline."""

    return _record_flow(session, payload, CapitalFlowType.DEPOSIT)


@app.post(
    "/finance/withdraw",
    response_model=CapitalFlowResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Record a capital withdrawal",
)
def record_withdrawal(
    payload: CapitalFlowRequest, session: Session = Depends(get_session)
) -> CapitalFlowResponse:
    """Persist a withdrawal and update the NAV baseline."""

    return _record_flow(session, payload, CapitalFlowType.WITHDRAW)


@app.get(
    "/finance/flows",
    response_model=FlowHistoryResponse,
    summary="List capital flows for an account",
)
def list_flows(
    account_id: Optional[str] = Query(None, description="Filter flows to a specific account"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    session: Session = Depends(get_session),
) -> FlowHistoryResponse:
    """Return recent capital flows with their resulting NAV baselines."""

    stmt = select(CapitalFlowRecord).order_by(CapitalFlowRecord.ts.desc()).limit(limit)
    if account_id:
        stmt = stmt.where(CapitalFlowRecord.account_id == account_id)

    records = list(session.execute(stmt).scalars())
    account_ids = {record.account_id for record in records}
    baseline_lookup: dict[str, float] = {}
    if account_ids:
        baseline_stmt = select(NavBaselineRecord).where(NavBaselineRecord.account_id.in_(account_ids))
        for baseline_record in session.execute(baseline_stmt).scalars():
            baseline_lookup[baseline_record.account_id] = baseline_record.baseline

    flows = [_serialize_flow(record, baseline_lookup) for record in records]
    return FlowHistoryResponse(flows=flows)


@app.get("/health", include_in_schema=False)
def healthcheck() -> dict[str, str]:
    """Lightweight readiness probe."""

    return {"status": "ok"}
