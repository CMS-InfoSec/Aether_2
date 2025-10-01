"""FastAPI service exposing the venue fee schedule and volume metrics."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterator, List, Sequence

from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from services.fees.models import AccountVolume30d, Base, FeeTier


DEFAULT_DATABASE_URL = "sqlite:///./fees.db"


def _database_url() -> str:
    url = os.getenv("TIMESCALE_DSN") or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


ENGINE: Engine = create_engine(_database_url(), future=True)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)


app = FastAPI(title="Fee Schedule Service")


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


def _ordered_tiers(session: Session) -> List[FeeTier]:
    stmt = select(FeeTier).order_by(FeeTier.notional_threshold_usd.asc())
    return session.execute(stmt).scalars().all()


def _determine_tier(tiers: Sequence[FeeTier], volume: Decimal) -> FeeTier:
    if not tiers:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fee schedule is empty")

    ordered = sorted(tiers, key=lambda tier: Decimal(tier.notional_threshold_usd or 0))
    match = ordered[0]
    for tier in ordered:
        threshold = Decimal(tier.notional_threshold_usd or 0)
        if volume >= threshold:
            match = tier
        else:
            break
    return match


def _fee_amount(notional: Decimal, bps: Decimal) -> Decimal:
    raw_fee = (notional * bps) / Decimal("10000")
    return raw_fee.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


@app.get("/fees/effective", response_model=EffectiveFeeResponse)
def get_effective_fee(
    account_id: str = Query(..., description="Unique account identifier"),
    pair: str = Query(..., description="Trading pair symbol", min_length=3, max_length=32),
    liquidity: str = Query(..., description="Requested liquidity side", pattern=r"(?i)^(maker|taker)$"),
    notional: float = Query(..., gt=0.0, description="Order notional in USD"),
    session: Session = Depends(get_session),
) -> EffectiveFeeResponse:
    del pair  # the current schedule is global and does not vary by pair

    normalized_liquidity = liquidity.lower()
    tiers = _ordered_tiers(session)
    if not tiers:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fee schedule is not configured")

    volume_row = session.get(AccountVolume30d, account_id)
    if volume_row is None:
        basis_ts = datetime.now(timezone.utc)
        rolling_volume = Decimal("0")
    else:
        basis_ts = volume_row.updated_at or datetime.now(timezone.utc)
        rolling_volume = Decimal(volume_row.notional_usd_30d or 0)

    tier = _determine_tier(tiers, rolling_volume)
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
def get_fee_tiers(session: Session = Depends(get_session)) -> List[FeeTierSchema]:
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
    account_id: str = Query(..., description="Unique account identifier"),
    session: Session = Depends(get_session),
) -> Volume30dResponse:
    record = session.get(AccountVolume30d, account_id)
    if record is None:
        return Volume30dResponse(
            notional_usd_30d=0.0,
            updated_at=datetime.now(timezone.utc),
        )

    return Volume30dResponse(
        notional_usd_30d=float(record.notional_usd_30d or 0),
        updated_at=record.updated_at,
    )


def update_account_volume_30d(
    session: Session,
    account_id: str,
    fill_notional_usd: float,
    fill_time: datetime | None = None,
) -> AccountVolume30d:
    """Update the rolling 30-day volume using a newly observed fill."""

    timestamp = fill_time or datetime.now(timezone.utc)
    notional_delta = Decimal(str(fill_notional_usd))

    record = session.get(AccountVolume30d, account_id)
    if record is None:
        record = AccountVolume30d(
            account_id=account_id,
            notional_usd_30d=notional_delta,
            updated_at=timestamp,
        )
        session.add(record)
    else:
        if record.updated_at is None or timestamp - record.updated_at >= timedelta(days=30):
            new_total = notional_delta
        else:
            new_total = Decimal(record.notional_usd_30d or 0) + notional_delta
        record.notional_usd_30d = new_total
        record.updated_at = timestamp

    session.commit()
    session.refresh(record)
    return record


__all__ = ["app", "update_account_volume_30d"]

