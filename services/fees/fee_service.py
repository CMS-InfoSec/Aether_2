"""FastAPI service exposing Kraken-aligned fee schedule endpoints."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
from typing import Dict, Iterable, Iterator, List, Sequence

from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field, validator
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Numeric,
    String,
    UniqueConstraint,
    create_engine,
    func,
    select,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker


app = FastAPI(title="Fee Schedule Service")


# ---------------------------------------------------------------------------
# Database configuration
# ---------------------------------------------------------------------------


def _database_url() -> str:
    """Return the SQLAlchemy connection URL for TimescaleDB."""

    url = os.getenv("TIMESCALE_DSN") or os.getenv(
        "DATABASE_URL", "postgresql+psycopg2://timescale:password@localhost:5432/telemetry"
    )
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


ENGINE: Engine = create_engine(_database_url(), pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, autocommit=False, expire_on_commit=False)
Base = declarative_base()


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------


class FeeTierModel(Base):
    """ORM representation of the Kraken-style fee tier schedule."""

    __tablename__ = "fee_tiers"

    id = Column(BigInteger, primary_key=True)
    pair = Column(String(32), nullable=False, index=True)
    tier_id = Column(String(32), nullable=False)
    volume_threshold_usd = Column(Numeric(18, 2), nullable=False)
    maker_bps = Column(Numeric(9, 4), nullable=False)
    taker_bps = Column(Numeric(9, 4), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("pair", "tier_id", name="uq_fee_tiers_pair_tier"),
    )


class AccountFeeVolume(Base):
    """Timescale-backed rolling 30-day USD volume for each account/pair."""

    __tablename__ = "account_fee_volume"

    id = Column(BigInteger, primary_key=True)
    account_id = Column(String(64), nullable=False, index=True)
    pair = Column(String(32), nullable=False, index=True)
    event_time = Column(DateTime(timezone=True), nullable=False, index=True)
    notional_usd = Column(Numeric(18, 2), nullable=False)


# ---------------------------------------------------------------------------
# Static Kraken fee schedule used to seed the database
# ---------------------------------------------------------------------------


KRAKEN_FEE_SCHEDULE: Sequence[Dict[str, Decimal]] = tuple(
    {
        "tier_id": tier_id,
        "volume_threshold": Decimal(str(volume_threshold)),
        "maker_bps": Decimal(str(maker_bps)),
        "taker_bps": Decimal(str(taker_bps)),
    }
    for tier_id, volume_threshold, maker_bps, taker_bps in (
        ("Tier 0", 0, 16, 26),
        ("Tier 1", 10_000, 14, 24),
        ("Tier 2", 50_000, 12, 22),
        ("Tier 3", 100_000, 10, 20),
        ("Tier 4", 250_000, 8, 18),
        ("Tier 5", 500_000, 6, 16),
        ("Tier 6", 1_000_000, 4, 14),
        ("Tier 7", 2_500_000, 2, 12),
        ("Tier 8", 5_000_000, 0, 10),
        ("Tier 9", 10_000_000, 0, 8),
        ("Tier 10", 25_000_000, 0, 6),
        ("Tier 11", 100_000_000, 0, 4),
    )
)

DEFAULT_PAIRS: Sequence[str] = tuple(
    pair.strip().upper()
    for pair in os.getenv("FEE_SERVICE_PAIRS", "BTC-USD,ETH-USD,USDT-USD").split(",")
    if pair.strip()
)


def _create_schema() -> None:
    Base.metadata.create_all(ENGINE)
    with ENGINE.begin() as conn:
        with conn.begin_nested():
            try:
                conn.execute(
                    text(
                        "SELECT create_hypertable('account_fee_volume', 'event_time', if_not_exists => TRUE, migrate_data => TRUE)"
                    )
                )
            except Exception:
                # Fallback for non-Timescale environments (e.g., unit tests using SQLite)
                pass


def _seed_fee_tiers(session: Session, pairs: Iterable[str]) -> None:
    for pair in pairs:
        existing = session.execute(
            select(func.count()).select_from(FeeTierModel).where(FeeTierModel.pair == pair)
        ).scalar_one()
        if existing:
            continue

        session.add_all(
            FeeTierModel(
                pair=pair,
                tier_id=item["tier_id"],
                volume_threshold_usd=item["volume_threshold"],
                maker_bps=item["maker_bps"],
                taker_bps=item["taker_bps"],
            )
            for item in KRAKEN_FEE_SCHEDULE
        )
    session.commit()


@app.on_event("startup")
def _startup() -> None:
    _create_schema()
    with SessionLocal() as session:
        _seed_fee_tiers(session, DEFAULT_PAIRS)


def get_db() -> Iterator[Session]:
    """Dependency that yields a SQLAlchemy session."""

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def fetch_fee_tiers(session: Session, pair: str) -> List[FeeTierModel]:
    stmt = (
        select(FeeTierModel)
        .where(FeeTierModel.pair == pair)
        .order_by(FeeTierModel.volume_threshold_usd.asc())
    )
    return list(session.execute(stmt).scalars())


def fetch_all_fee_tiers(session: Session) -> Dict[str, List[FeeTierModel]]:
    stmt = select(FeeTierModel).order_by(FeeTierModel.pair.asc(), FeeTierModel.volume_threshold_usd.asc())
    rows = session.execute(stmt).scalars().all()
    schedule: Dict[str, List[FeeTierModel]] = {}
    for row in rows:
        schedule.setdefault(row.pair, []).append(row)
    return schedule


def fetch_rolling_30d_volume(session: Session, account_id: str, pair: str, now: datetime) -> float:
    window_start = now - timedelta(days=30)
    stmt = (
        select(func.coalesce(func.sum(AccountFeeVolume.notional_usd), 0))
        .where(AccountFeeVolume.account_id == account_id)
        .where(AccountFeeVolume.pair == pair)
        .where(AccountFeeVolume.event_time >= window_start)
    )
    value = session.execute(stmt).scalar_one()
    return float(value or 0)


def record_notional_event(session: Session, account_id: str, pair: str, notional: float, timestamp: datetime) -> None:
    session.add(
        AccountFeeVolume(
            account_id=account_id,
            pair=pair,
            event_time=timestamp,
            notional_usd=Decimal(str(notional)),
        )
    )
    session.commit()


def determine_fee_tier(tiers: Sequence[FeeTierModel], effective_volume: float) -> FeeTierModel | None:
    if not tiers:
        return None

    matched: FeeTierModel | None = None
    for tier in tiers:
        threshold = float(tier.volume_threshold_usd or 0)
        if effective_volume >= threshold:
            matched = tier
    return matched or tiers[0]


# ---------------------------------------------------------------------------
# Pydantic schema definitions
# ---------------------------------------------------------------------------


class EffectiveFeeQuery(BaseModel):
    account_id: str = Field(..., description="Unique account identifier")
    pair: str = Field(..., min_length=3, max_length=32, description="Trading pair symbol")
    liquidity: str = Field(..., description="Liquidity side: maker or taker")
    notional: float = Field(..., gt=0.0, description="Order notional value in USD")

    @validator("pair")
    def normalize_pair(cls, value: str) -> str:
        return value.upper()

    @validator("liquidity")
    def normalize_liquidity(cls, value: str) -> str:
        normalized = value.lower()
        if normalized not in {"maker", "taker"}:
            raise ValueError("liquidity must be either 'maker' or 'taker'")
        return normalized


class EffectiveFeeResponse(BaseModel):
    bps: float = Field(..., description="Fee rate in basis points")
    usd: float = Field(..., description="Fee amount in USD")
    tier_id: str = Field(..., description="Identifier of the matched fee tier")


class FeeTierSchema(BaseModel):
    tier_id: str
    volume_threshold: float
    maker_bps: float
    taker_bps: float


class FeeTierScheduleResponse(BaseModel):
    pair: str
    tiers: List[FeeTierSchema]


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------


@app.get("/fees/effective", response_model=EffectiveFeeResponse)
def get_effective_fee(
    account_id: str = Query(..., description="Unique account identifier"),
    pair: str = Query(..., description="Trading pair symbol"),
    liquidity: str = Query(..., description="Liquidity side"),
    notional: float = Query(..., gt=0.0, description="Order notional value in USD"),
    session: Session = Depends(get_db),
) -> EffectiveFeeResponse:
    params = EffectiveFeeQuery(
        account_id=account_id,
        pair=pair,
        liquidity=liquidity,
        notional=notional,
    )

    tiers = fetch_fee_tiers(session, params.pair)
    if not tiers:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Fee schedule not found for pair {params.pair}",
        )

    now = datetime.now(timezone.utc)
    rolling_volume = fetch_rolling_30d_volume(session, params.account_id, params.pair, now)
    effective_volume = rolling_volume + params.notional

    tier = determine_fee_tier(tiers, effective_volume)
    if tier is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to determine fee tier")

    bps = float(tier.maker_bps if params.liquidity == "maker" else tier.taker_bps)
    usd_fee = params.notional * bps / 10_000.0

    record_notional_event(session, params.account_id, params.pair, params.notional, now)

    return EffectiveFeeResponse(bps=bps, usd=usd_fee, tier_id=tier.tier_id)


@app.get("/fees/tiers", response_model=List[FeeTierScheduleResponse])
def get_fee_tiers(session: Session = Depends(get_db)) -> List[FeeTierScheduleResponse]:
    schedule = fetch_all_fee_tiers(session)
    if not schedule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No fee tiers available")

    response: List[FeeTierScheduleResponse] = []
    for pair, tiers in schedule.items():
        response.append(
            FeeTierScheduleResponse(
                pair=pair,
                tiers=[
                    FeeTierSchema(
                        tier_id=tier.tier_id,
                        volume_threshold=float(tier.volume_threshold_usd),
                        maker_bps=float(tier.maker_bps),
                        taker_bps=float(tier.taker_bps),
                    )
                    for tier in tiers
                ],
            )
        )

    return response


__all__ = ["app"]

