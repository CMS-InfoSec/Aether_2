"""Capital allocator service orchestrating firm-wide NAV distribution."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, ConfigDict, model_validator
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


Base = declarative_base()


DEFAULT_DATABASE_URL = "sqlite:///./allocator.db"


def _database_url() -> str:
    return (
        os.getenv("CAPITAL_ALLOCATOR_DB_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )


def _engine_options(url: str) -> Dict[str, object]:
    options: Dict[str, object] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url == "sqlite:///:memory:" or url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


_DB_URL = _database_url()
ENGINE: Engine = create_engine(_DB_URL, **_engine_options(_DB_URL))
SessionLocal = sessionmaker(bind=ENGINE, expire_on_commit=False, autoflush=False, future=True)


class CapitalAllocation(Base):
    """Historical record of allocation decisions for an account."""

    __tablename__ = "capital_allocations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, nullable=False, index=True)
    pct = Column(Float, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)


Base.metadata.create_all(bind=ENGINE)


@dataclass(slots=True)
class AccountNavSnapshot:
    account_id: str
    nav: float
    drawdown: float
    drawdown_limit: Optional[float]
    timestamp: Optional[datetime]


@dataclass(slots=True)
class AllocationDecision:
    account_id: str
    nav: float
    drawdown_ratio: float
    drawdown_limit: float
    requested_pct: float
    final_pct: float
    throttled: bool


class AllocationRequest(BaseModel):
    allocations: Dict[str, float] = Field(
        default_factory=dict,
        description="Requested allocation percentages keyed by account identifier.",
    )

    @model_validator(mode="after")
    def validate_totals(cls, model: "AllocationRequest") -> "AllocationRequest":
        if not model.allocations:
            raise ValueError("allocations payload cannot be empty")
        total = sum(float(value) for value in model.allocations.values())
        if total <= 0:
            raise ValueError("allocation percentages must sum to a positive value")
        return model


class AllocatedAccount(BaseModel):
    account_id: str
    nav: float = Field(0.0, ge=0.0)
    allocation_pct: float = Field(0.0, ge=0.0)
    allocated_nav: float = Field(0.0, ge=0.0)
    drawdown_ratio: float = Field(0.0, ge=0.0)
    drawdown_limit: float = Field(0.0, ge=0.0)
    throttled: bool = False

    model_config = ConfigDict(extra="ignore")


class AllocationResponse(BaseModel):
    timestamp: datetime
    total_nav: float
    requested_total_pct: float
    allocated_total_pct: float
    unallocated_pct: float
    accounts: List[AllocatedAccount]


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        LOGGER.warning("Invalid float value for %s: %s", key, raw)
        return default


def _parse_timestamp(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text_value = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text_value)
        except ValueError:
            return None
    return None


def _load_latest_navs(session: Session) -> Dict[str, AccountNavSnapshot]:
    query = text(
        """
        SELECT
            pc.account_id,
            COALESCE(pc.nav, pc.net_asset_value, pc.equity, pc.ending_balance, pc.balance, 0.0) AS nav,
            COALESCE(pc.drawdown, pc.realized_drawdown, pc.drawdown_value, pc.max_drawdown, 0.0) AS drawdown,
            COALESCE(pc.drawdown_limit, pc.max_drawdown_limit, pc.drawdown_cap, pc.drawdown_threshold) AS drawdown_limit,
            COALESCE(pc.curve_ts, pc.valuation_ts, pc.ts, pc.created_at) AS event_ts
        FROM pnl_curves pc
        INNER JOIN (
            SELECT
                account_id,
                MAX(COALESCE(curve_ts, valuation_ts, ts, created_at)) AS latest_ts
            FROM pnl_curves
            GROUP BY account_id
        ) latest ON latest.account_id = pc.account_id
               AND COALESCE(pc.curve_ts, pc.valuation_ts, pc.ts, pc.created_at) = latest.latest_ts
        """
    )

    snapshots: Dict[str, AccountNavSnapshot] = {}
    try:
        results = session.execute(query)
    except SQLAlchemyError as exc:
        LOGGER.error("Failed to query pnl_curves: %s", exc)
        raise HTTPException(status_code=503, detail="Unable to load NAV snapshots") from exc

    for row in results:
        account_id = str(row["account_id"])
        nav = float(row["nav"] or 0.0)
        drawdown = float(row["drawdown"] or 0.0)
        drawdown_limit = row["drawdown_limit"]
        limit_value = float(drawdown_limit) if drawdown_limit not in (None, "") else None
        snapshots[account_id] = AccountNavSnapshot(
            account_id=account_id,
            nav=max(nav, 0.0),
            drawdown=max(drawdown, 0.0),
            drawdown_limit=limit_value,
            timestamp=_parse_timestamp(row["event_ts"]),
        )
    return snapshots


def _latest_allocation_snapshot(session: Session) -> Dict[str, float]:
    query = text(
        """
        SELECT ca.account_id, ca.pct
        FROM capital_allocations ca
        INNER JOIN (
            SELECT account_id, MAX(ts) AS latest_ts
            FROM capital_allocations
            GROUP BY account_id
        ) latest
        ON ca.account_id = latest.account_id AND ca.ts = latest.latest_ts
        """
    )
    allocations: Dict[str, float] = {}
    try:
        for row in session.execute(query):
            allocations[str(row["account_id"])] = float(row["pct"] or 0.0)
    except SQLAlchemyError as exc:
        LOGGER.error("Failed to query capital allocation history: %s", exc)
        raise HTTPException(status_code=503, detail="Unable to load allocation history") from exc
    return allocations


def _effective_drawdown_limit(snapshot: AccountNavSnapshot, default_ratio: float) -> Optional[float]:
    if snapshot.drawdown_limit and snapshot.drawdown_limit > 0:
        return snapshot.drawdown_limit
    if snapshot.nav > 0 and default_ratio > 0:
        return snapshot.nav * default_ratio
    return None


def _apply_allocation_rules(
    navs: Mapping[str, AccountNavSnapshot],
    requested: Mapping[str, float],
) -> Tuple[Dict[str, AllocationDecision], float, float]:
    threshold = _env_float("ALLOCATOR_DRAWDOWN_THRESHOLD", 0.85)
    throttle_floor = _env_float("ALLOCATOR_MIN_THROTTLE_PCT", 0.0)
    default_limit_ratio = _env_float("ALLOCATOR_DEFAULT_DRAWDOWN_LIMIT_PCT", 0.10)

    if threshold <= 0:
        threshold = 0.85
    if throttle_floor < 0:
        throttle_floor = 0.0

    decisions: Dict[str, AllocationDecision] = {}
    accounts: Iterable[str] = set(navs.keys()) | set(requested.keys())
    freed_total = 0.0
    requested_total = 0.0

    for account_id in accounts:
        snapshot = navs.get(
            account_id,
            AccountNavSnapshot(account_id=account_id, nav=0.0, drawdown=0.0, drawdown_limit=None, timestamp=None),
        )
        desired = float(requested.get(account_id, 0.0) or 0.0)
        desired = max(desired, 0.0)
        requested_total += desired

        limit_value = _effective_drawdown_limit(snapshot, default_limit_ratio)
        drawdown = max(snapshot.drawdown, 0.0)
        ratio = drawdown / limit_value if limit_value and limit_value > 0 else 0.0
        throttled = bool(snapshot.nav > 0 and ratio >= threshold)

        final_pct = desired
        if throttled and desired > throttle_floor:
            final_pct = throttle_floor
            freed_total += desired - final_pct

        decisions[account_id] = AllocationDecision(
            account_id=account_id,
            nav=max(snapshot.nav, 0.0),
            drawdown_ratio=ratio,
            drawdown_limit=float(limit_value) if limit_value else 0.0,
            requested_pct=desired,
            final_pct=final_pct,
            throttled=throttled,
        )

    eligible: List[AllocationDecision] = [entry for entry in decisions.values() if not entry.throttled]
    if freed_total > 0 and eligible:
        weight = sum(entry.nav for entry in eligible)
        if weight <= 0:
            redistribution = freed_total / len(eligible)
            for entry in eligible:
                entry.final_pct += redistribution
        else:
            for entry in eligible:
                share = freed_total * (entry.nav / weight)
                entry.final_pct += share

    allocated_total = sum(entry.final_pct for entry in decisions.values())
    if (
        requested_total > 0
        and allocated_total > 0
        and freed_total <= 1e-9
    ):
        scale = requested_total / allocated_total
        for entry in decisions.values():
            entry.final_pct *= scale
        allocated_total = requested_total

    unallocated = max(requested_total - allocated_total, 0.0)
    return decisions, requested_total, unallocated


def _build_response(decisions: Mapping[str, AllocationDecision], requested_total: float, unallocated_pct: float) -> AllocationResponse:
    timestamp = datetime.now(timezone.utc)
    total_nav = sum(entry.nav for entry in decisions.values())
    accounts: List[AllocatedAccount] = []
    allocated_total = 0.0
    allocation_base = total_nav if total_nav > 0 else 1.0
    for entry in sorted(decisions.values(), key=lambda item: item.account_id):
        allocated_total += entry.final_pct
        accounts.append(
            AllocatedAccount(
                account_id=entry.account_id,
                nav=entry.nav,
                allocation_pct=entry.final_pct,
                allocated_nav=entry.final_pct * allocation_base,
                drawdown_ratio=entry.drawdown_ratio,
                drawdown_limit=entry.drawdown_limit,
                throttled=entry.throttled,
            )
        )

    return AllocationResponse(
        timestamp=timestamp,
        total_nav=total_nav,
        requested_total_pct=requested_total,
        allocated_total_pct=allocated_total,
        unallocated_pct=unallocated_pct,
        accounts=accounts,
    )


app = FastAPI(title="Capital Allocator Service", version="1.0.0")


@app.get("/allocator/status", response_model=AllocationResponse)
def allocator_status() -> AllocationResponse:
    with SessionLocal() as session:
        navs = _load_latest_navs(session)
        if not navs:
            raise HTTPException(status_code=404, detail="No NAV data available")
        latest_allocations = _latest_allocation_snapshot(session)

    if not latest_allocations:
        latest_allocations = {account_id: 0.0 for account_id in navs.keys()}

    decisions, requested_total, unallocated = _apply_allocation_rules(navs, latest_allocations)
    return _build_response(decisions, requested_total, unallocated)


@app.post("/allocator/rebalance", response_model=AllocationResponse)
def rebalance_allocation(request: AllocationRequest) -> AllocationResponse:
    with SessionLocal() as session:
        navs = _load_latest_navs(session)
        if not navs:
            raise HTTPException(status_code=404, detail="No NAV data available")

        decisions, requested_total, unallocated = _apply_allocation_rules(navs, request.allocations)
        timestamp = datetime.now(timezone.utc)

        try:
            for decision in decisions.values():
                record = CapitalAllocation(
                    account_id=decision.account_id,
                    pct=decision.final_pct,
                    ts=timestamp,
                )
                session.add(record)
            session.commit()
        except SQLAlchemyError as exc:
            session.rollback()
            LOGGER.error("Failed to persist capital allocation decisions: %s", exc)
            raise HTTPException(status_code=503, detail="Unable to persist allocation decisions") from exc

    return _build_response(decisions, requested_total, unallocated)


__all__ = ["app", "SessionLocal", "ENGINE", "CapitalAllocation"]

