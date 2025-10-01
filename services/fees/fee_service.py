"""FastAPI service exposing the venue fee schedule and volume metrics."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterator, List, Sequence, Tuple

from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from services.common.security import require_admin_account
from services.fees.fee_optimizer import FeeOptimizer
from services.fees.models import AccountFill, AccountVolume30d, Base, FeeTier


DEFAULT_DATABASE_URL = "sqlite:///./fees.db"
POLICY_SERVICE_URL = os.getenv("POLICY_SERVICE_URL", "http://policy-service")
POLICY_VOLUME_SIGNAL_PATH = os.getenv(
    "POLICY_VOLUME_SIGNAL_PATH", "/policy/opportunistic-volume"
)
POLICY_VOLUME_SIGNAL_TIMEOUT = float(os.getenv("POLICY_VOLUME_SIGNAL_TIMEOUT", "0.5"))
NEXT_TIER_ALERT_THRESHOLD = Decimal("0.95")


def _database_url() -> str:
    url = os.getenv("TIMESCALE_DSN") or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


ENGINE: Engine = create_engine(_database_url(), future=True)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)


app = FastAPI(title="Fee Schedule Service")


logger = logging.getLogger(__name__)


DEFAULT_KRAKEN_SCHEDULE: Sequence[dict[str, Decimal | str]] = (
    {"tier_id": "tier_0", "threshold": Decimal("0"), "maker": Decimal("16"), "taker": Decimal("26")},
    {"tier_id": "tier_1", "threshold": Decimal("10000"), "maker": Decimal("14"), "taker": Decimal("24")},
    {"tier_id": "tier_2", "threshold": Decimal("50000"), "maker": Decimal("12"), "taker": Decimal("22")},
    {"tier_id": "tier_3", "threshold": Decimal("100000"), "maker": Decimal("10"), "taker": Decimal("20")},
    {"tier_id": "tier_4", "threshold": Decimal("250000"), "maker": Decimal("8"), "taker": Decimal("18")},
    {"tier_id": "tier_5", "threshold": Decimal("500000"), "maker": Decimal("6"), "taker": Decimal("16")},
    {"tier_id": "tier_6", "threshold": Decimal("1000000"), "maker": Decimal("4"), "taker": Decimal("14")},
    {"tier_id": "tier_7", "threshold": Decimal("2500000"), "maker": Decimal("2"), "taker": Decimal("12")},
    {"tier_id": "tier_8", "threshold": Decimal("5000000"), "maker": Decimal("0"), "taker": Decimal("10")},
    {"tier_id": "tier_9", "threshold": Decimal("10000000"), "maker": Decimal("0"), "taker": Decimal("8")},
    {"tier_id": "tier_10", "threshold": Decimal("25000000"), "maker": Decimal("0"), "taker": Decimal("6")},
    {"tier_id": "tier_11", "threshold": Decimal("100000000"), "maker": Decimal("0"), "taker": Decimal("4")},
)


def _seed_schedule(session: Session) -> None:
    existing = session.execute(select(func.count()).select_from(FeeTier)).scalar_one()
    if existing:
        return

    effective_from = datetime(2020, 1, 1, tzinfo=timezone.utc)
    for tier in DEFAULT_KRAKEN_SCHEDULE:
        session.add(
            FeeTier(
                tier_id=tier["tier_id"],
                notional_threshold_usd=tier["threshold"],
                maker_bps=tier["maker"],
                taker_bps=tier["taker"],
                effective_from=effective_from,
            )
        )
    session.commit()


@app.on_event("startup")
def _on_startup() -> None:
    Base.metadata.create_all(bind=ENGINE)
    with SessionLocal() as session:
        _seed_schedule(session)


def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


class EffectiveFeeResponse(BaseModel):
    bps: float = Field(..., description="Fee rate expressed in basis points")
    usd: float = Field(..., description="Fee amount in USD")
    tier_id: str = Field(..., description="Identifier of the matched fee tier")
    basis_ts: datetime = Field(..., description="Timestamp of the volume basis for the tier decision")


class FeeTierSchema(BaseModel):
    tier_id: str
    notional_threshold_usd: float
    maker_bps: float
    taker_bps: float
    effective_from: datetime


class Volume30dResponse(BaseModel):
    notional_usd_30d: float
    updated_at: datetime


class AccountSummaryResponse(BaseModel):
    account_id: str = Field(..., description="Account identifier")
    tier: str = Field(..., description="Matched Kraken fee tier")
    volume_usd: float = Field(..., ge=0.0, description="Rolling 30-day USD volume")
    effective_fee_bps: float = Field(..., ge=0.0, description="Realized effective fee in basis points")
    basis_ts: datetime = Field(..., description="Timestamp anchoring the rolling window")


class NextTierStatusResponse(BaseModel):
    current_tier: str = Field(..., description="Identifier of the active fee tier")
    next_tier: str | None = Field(
        None, description="Identifier of the next fee tier if one exists"
    )
    progress_pct: float = Field(
        ..., description="Progress towards the next tier expressed as a percentage"
    )


def _fee_amount(notional: Decimal, bps: Decimal) -> Decimal:
    raw_fee = (notional * bps) / Decimal("10000")
    return raw_fee.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


def _to_decimal(value: Decimal | float | int | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


optimizer = FeeOptimizer(
    alert_threshold=NEXT_TIER_ALERT_THRESHOLD,
    policy_service_url=POLICY_SERVICE_URL,
    policy_path=POLICY_VOLUME_SIGNAL_PATH,
    policy_timeout=POLICY_VOLUME_SIGNAL_TIMEOUT,
)


def _ordered_tiers(session: Session) -> list[FeeTier]:
    """Return the configured fee tiers ordered by threshold."""

    return optimizer.ordered_tiers(session)


def _rolling_window(as_of: datetime | None = None) -> Tuple[datetime, datetime]:
    now = as_of or datetime.now(timezone.utc)
    start = now - timedelta(days=30)
    return start, now


def _rolling_volume(
    session: Session, account_id: str, as_of: datetime | None = None
) -> Tuple[Decimal, datetime]:
    window_start, window_end = _rolling_window(as_of)
    stmt = (
        select(
            func.coalesce(func.sum(AccountFill.notional_usd), Decimal("0")),
            func.max(AccountFill.fill_ts),
        )
        .where(AccountFill.account_id == account_id)
        .where(AccountFill.fill_ts >= window_start)
        .where(AccountFill.fill_ts <= window_end)
    )
    total_notional, basis_ts = session.execute(stmt).one()
    volume = _to_decimal(total_notional)
    basis = basis_ts or window_end
    return volume, basis


def _realized_fee_bps(session: Session, account_id: str, as_of: datetime | None = None) -> Decimal:
    window_start, window_end = _rolling_window(as_of)
    stmt = (
        select(
            func.coalesce(func.sum(AccountFill.notional_usd), Decimal("0")),
            func.coalesce(func.sum(AccountFill.actual_fee_usd), Decimal("0")),
            func.coalesce(func.sum(AccountFill.estimated_fee_usd), Decimal("0")),
        )
        .where(AccountFill.account_id == account_id)
        .where(AccountFill.fill_ts >= window_start)
        .where(AccountFill.fill_ts <= window_end)
    )
    total_notional, actual_fee_total, estimated_fee_total = session.execute(stmt).one()
    notional = _to_decimal(total_notional)
    if notional <= 0:
        return Decimal("0")
    actual_total = _to_decimal(actual_fee_total)
    if actual_total > 0:
        return (actual_total / notional) * Decimal("10000")
    estimated_total = _to_decimal(estimated_fee_total)
    if estimated_total > 0:
        return (estimated_total / notional) * Decimal("10000")
    return Decimal("0")


@app.get("/fees/effective", response_model=EffectiveFeeResponse)
def get_effective_fee(
    pair: str = Query(..., description="Trading pair symbol", min_length=3, max_length=32),
    liquidity: str = Query(..., description="Requested liquidity side", pattern=r"(?i)^(maker|taker)$"),
    notional: float = Query(..., gt=0.0, description="Order notional in USD"),
    session: Session = Depends(get_session),
    account_id: str = Depends(require_admin_account),
) -> EffectiveFeeResponse:
    del pair  # the current schedule is global and does not vary by pair

    normalized_liquidity = liquidity.lower()
    tiers = _ordered_tiers(session)
    if not tiers:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fee schedule is not configured")

    rolling_volume, basis_ts = _rolling_volume(session, account_id)

    try:
        tier = optimizer.determine_tier(tiers, rolling_volume)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    bps_value = Decimal(tier.maker_bps if normalized_liquidity == "maker" else tier.taker_bps)
    notional_decimal = Decimal(str(notional))
    fee_usd = _fee_amount(notional_decimal, bps_value)

    return EffectiveFeeResponse(
        bps=float(bps_value),
        usd=float(fee_usd),
        tier_id=tier.tier_id,
        basis_ts=basis_ts,
    )


@app.get("/fees/tiers", response_model=List[FeeTierSchema])
def get_fee_tiers(
    session: Session = Depends(get_session),
    _: str = Depends(require_admin_account),
) -> List[FeeTierSchema]:
    tiers = _ordered_tiers(session)
    if not tiers:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fee schedule is not configured")

    return [
        FeeTierSchema(
            tier_id=tier.tier_id,
            notional_threshold_usd=float(tier.notional_threshold_usd or 0),
            maker_bps=float(tier.maker_bps),
            taker_bps=float(tier.taker_bps),
            effective_from=tier.effective_from,
        )
        for tier in tiers
    ]


@app.get("/fees/volume30d", response_model=Volume30dResponse)
def get_volume_30d(
    session: Session = Depends(get_session),
    account_id: str = Depends(require_admin_account),
) -> Volume30dResponse:
    volume, basis_ts = _rolling_volume(session, account_id)
    return Volume30dResponse(
        notional_usd_30d=float(volume),
        updated_at=basis_ts,
    )


@app.get("/fees/account_summary", response_model=AccountSummaryResponse)
def get_account_summary(
    account_id: str = Query(..., min_length=1, max_length=64, description="Account identifier"),
    session: Session = Depends(get_session),
    _: str = Depends(require_admin_account),
) -> AccountSummaryResponse:
    tiers = _ordered_tiers(session)
    if not tiers:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fee schedule is not configured")

    volume, basis_ts = _rolling_volume(session, account_id)
    try:
        tier = optimizer.determine_tier(tiers, volume)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    effective_fee = _realized_fee_bps(session, account_id, basis_ts)
    return AccountSummaryResponse(
        account_id=account_id,
        tier=tier.tier_id,
        volume_usd=float(volume),
        effective_fee_bps=float(effective_fee),
        basis_ts=basis_ts,
    )


@app.get("/fees/next_tier_status", response_model=NextTierStatusResponse)
def get_next_tier_status(
    session: Session = Depends(get_session),
    account_id: str = Depends(require_admin_account),
) -> NextTierStatusResponse:
    try:
        status = optimizer.status_for_account(session, account_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return NextTierStatusResponse(
        current_tier=status.current_tier.tier_id,
        next_tier=status.next_tier.tier_id if status.next_tier is not None else None,
        progress_pct=float(status.progress_pct),
    )


def update_account_volume_30d(
    session: Session,
    account_id: str,
    fill_notional_usd: float,
    fill_time: datetime | None = None,
    liquidity: str | None = None,
    estimated_fee_bps: float | None = None,
    actual_fee_usd: float | None = None,
) -> AccountVolume30d:
    """Update the rolling 30-day volume using a newly observed fill."""

    timestamp = fill_time or datetime.now(timezone.utc)
    notional_delta = _to_decimal(fill_notional_usd)

    if notional_delta < 0:
        raise ValueError("fill_notional_usd must be non-negative")

    estimated_bps = Decimal(str(estimated_fee_bps)) if estimated_fee_bps is not None else None
    estimated_fee_usd = (
        _fee_amount(notional_delta, estimated_bps) if estimated_bps is not None else None
    )
    actual_fee = Decimal(str(actual_fee_usd)) if actual_fee_usd is not None else None

    session.add(
        AccountFill(
            account_id=account_id,
            liquidity=liquidity,
            notional_usd=notional_delta,
            estimated_fee_bps=estimated_bps,
            estimated_fee_usd=estimated_fee_usd,
            actual_fee_usd=actual_fee,
            fill_ts=timestamp,
        )
    )
    session.flush()

    prune_before = timestamp - timedelta(days=60)
    session.execute(
        AccountFill.__table__
        .delete()
        .where(AccountFill.__table__.c.account_id == account_id)
        .where(AccountFill.__table__.c.fill_ts < prune_before)
    )

    rolling_volume, basis_ts = _rolling_volume(session, account_id, timestamp)

    record = session.get(AccountVolume30d, account_id)
    if record is None:
        record = AccountVolume30d(
            account_id=account_id,
            notional_usd_30d=rolling_volume,
            updated_at=basis_ts,
        )
        session.add(record)
    else:
        record.notional_usd_30d = rolling_volume
        record.updated_at = basis_ts

    session.commit()
    session.refresh(record)

    if actual_fee is not None and estimated_fee_usd is not None and notional_delta > 0:
        actual_bps = (actual_fee / notional_delta) * Decimal("10000") if actual_fee > 0 else Decimal("0")
        estimated_bps_value = estimated_bps if estimated_bps is not None else Decimal("0")
        discrepancy = actual_fee - estimated_fee_usd
        logger.info(
            "fee_reconciliation",
            extra={
                "account_id": account_id,
                "liquidity": liquidity,
                "notional_usd": float(notional_delta),
                "estimated_fee_bps": float(estimated_bps_value),
                "actual_fee_bps": float(actual_bps),
                "estimated_fee_usd": float(estimated_fee_usd),
                "actual_fee_usd": float(actual_fee),
                "discrepancy_bps": float(
                    actual_bps - estimated_bps_value if notional_delta > 0 else Decimal("0")
                ),
                "discrepancy_usd": float(discrepancy),
            },
        )

    optimizer.monitor_account(
        session,
        account_id,
        Decimal(record.notional_usd_30d or 0),
        record.updated_at or timestamp,
    )
    return record


__all__ = ["app", "update_account_volume_30d"]

