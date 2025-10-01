"""FastAPI microservice exposing the approved trading universe.

The service combines market capitalisation data sourced from CoinGecko with
Kraken venue liquidity in TimescaleDB to determine which symbols are eligible
for trading.  Operators can manually override the computed universe while all
changes are captured in the audit log for traceability.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Sequence
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import Boolean, Column, DateTime, Float, String, create_engine, func, select
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker


DATABASE_URL = os.getenv(
    "TIMESCALE_DATABASE_URI",
    os.getenv("DATABASE_URL", "postgresql+psycopg2://timescale:password@localhost:5432/aether"),
)


Base = declarative_base()


class Feature(Base):
    """TimescaleDB feature storage used by ingestion pipelines."""

    __tablename__ = "features"

    feature_name = Column(String, primary_key=True)
    entity_id = Column(String, primary_key=True)
    event_timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float)
    attributes = Column("metadata", JSONB)


class OhlcvBar(Base):
    """Kraken OHLCV candles captured in TimescaleDB."""

    __tablename__ = "ohlcv_bars"

    market = Column(String, primary_key=True)
    bucket_start = Column(DateTime(timezone=True), primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)


class UniverseWhitelist(Base):
    """Manual override table persisted in TimescaleDB."""

    __tablename__ = "universe_whitelist"

    asset_id = Column(String, primary_key=True)
    as_of = Column(DateTime(timezone=True), primary_key=True)
    source = Column(String, nullable=False)
    approved = Column(Boolean, nullable=False)
    details = Column("metadata", JSONB, default=dict)


class AuditLog(Base):
    """Audit events recorded for manual overrides."""

    __tablename__ = "audit_log"

    event_id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    entity_type = Column(String, nullable=False)
    entity_id = Column(String, nullable=False)
    actor = Column(String, nullable=False)
    action = Column(String, nullable=False)
    event_time = Column(DateTime(timezone=True), nullable=False)
    attributes = Column("metadata", JSONB, default=dict)


def _normalize_database_url(url: str) -> str:
    """Ensure PostgreSQL URLs use the psycopg2 dialect."""

    if url.startswith("postgresql+psycopg://"):
        return "postgresql+psycopg2://" + url[len("postgresql+psycopg://") :]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg2://" + url[len("postgresql://") :]
    if url.startswith("postgres://"):
        return "postgresql+psycopg2://" + url[len("postgres://") :]
    return url


def _create_engine() -> Engine:
    database_url = _normalize_database_url(DATABASE_URL)
    return create_engine(database_url, future=True)


ENGINE = _create_engine()
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)


def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


app = FastAPI(title="Universe Selection Service")


class UniverseResponse(BaseModel):
    symbols: List[str]
    generated_at: datetime


class OverrideRequest(BaseModel):
    symbol: str = Field(..., description="Base asset symbol to override", example="BTC")
    enabled: bool = Field(..., description="Whether the symbol should be allowed for trading")
    reason: Optional[str] = Field(None, description="Reason for the manual override")
    actor: Optional[str] = Field(
        None,
        description="Identifier for the operator performing the override",
        example="ops_team",
    )


class OverrideResponse(BaseModel):
    symbol: str
    enabled: bool
    reason: Optional[str]
    actor: str
    updated_at: datetime
    audit_event_id: UUID


MARKET_CAP_THRESHOLD = 1_000_000_000.0
VOLUME_THRESHOLD = 100_000_000.0
VOLATILITY_THRESHOLD = 0.40
MANUAL_OVERRIDE_SOURCE = "manual_override"


def _latest_feature_map(session: Session, feature_names: Sequence[str]) -> Dict[str, Feature]:
    """Return latest feature rows keyed by entity id with priority order."""

    results: Dict[str, Feature] = {}
    for feature_name in feature_names:
        subquery = (
            select(Feature.entity_id, func.max(Feature.event_timestamp).label("latest"))
            .where(Feature.feature_name == feature_name)
            .group_by(Feature.entity_id)
            .subquery()
        )

        rows = (
            session.execute(
                select(Feature)
                .where(Feature.feature_name == feature_name)
                .join(
                    subquery,
                    (Feature.entity_id == subquery.c.entity_id)
                    & (Feature.event_timestamp == subquery.c.latest),
                )
            )
            .scalars()
            .all()
        )

        for feature in rows:
            symbol = feature.entity_id.upper()
            results.setdefault(symbol, feature)

    return results


def _kraken_volume_24h(session: Session) -> Dict[str, float]:
    """Aggregate the past 24 hours of Kraken volume per USD-quoted market."""

    since = datetime.now(timezone.utc) - timedelta(days=1)
    rows = session.execute(
        select(OhlcvBar.market, func.sum(OhlcvBar.volume).label("volume"))
        .where(OhlcvBar.bucket_start >= since)
        .group_by(OhlcvBar.market)
    )

    volumes: Dict[str, float] = {}
    for market, volume in rows:
        if volume is None:
            continue
        normalized = _normalize_market(market)
        if normalized is None:
            continue
        base_symbol, quote_symbol = normalized
        if quote_symbol != "USD":
            continue
        volumes[base_symbol] = max(volumes.get(base_symbol, 0.0), float(volume))
    return volumes


def _normalize_market(market: str) -> Optional[tuple[str, str]]:
    """Return (base, quote) from a Kraken market identifier."""

    if not market:
        return None
    market = market.upper()
    if "-" in market:
        base, _, quote = market.partition("-")
        return base, quote
    if market.endswith("USD"):
        return market[:-3], "USD"
    return None


def _latest_manual_overrides(session: Session) -> Dict[str, UniverseWhitelist]:
    """Return the most recent manual override per asset."""

    overrides: Dict[str, UniverseWhitelist] = {}
    rows = session.execute(
        select(UniverseWhitelist)
        .where(UniverseWhitelist.source == MANUAL_OVERRIDE_SOURCE)
        .order_by(UniverseWhitelist.asset_id, UniverseWhitelist.as_of.desc())
    ).scalars()

    for record in rows:
        symbol = record.asset_id.upper()
        if symbol not in overrides:
            overrides[symbol] = record
    return overrides


def _evaluate_universe(session: Session) -> List[str]:
    caps = _latest_feature_map(session, ["coingecko.market_cap", "coingecko_market_cap"])
    vols = _latest_feature_map(
        session,
        ["coingecko.volatility_30d", "coingecko_volatility_30d", "coingecko.volatility"],
    )
    volumes = _kraken_volume_24h(session)
    overrides = _latest_manual_overrides(session)

    approved = set()

    for symbol, cap_feature in caps.items():
        market_cap = float(cap_feature.value or 0.0)
        vol_feature = vols.get(symbol)
        volatility = float(vol_feature.value) if vol_feature and vol_feature.value is not None else 0.0
        kraken_volume = volumes.get(symbol, 0.0)

        if (
            market_cap >= MARKET_CAP_THRESHOLD
            and kraken_volume >= VOLUME_THRESHOLD
            and volatility >= VOLATILITY_THRESHOLD
        ):
            approved.add(symbol)

    for symbol, override in overrides.items():
        if override.approved:
            approved.add(symbol)
        else:
            approved.discard(symbol)

    return sorted(approved)


@app.get("/universe/approved", response_model=UniverseResponse)
def approved_universe(session: Session = Depends(get_session)) -> UniverseResponse:
    symbols = _evaluate_universe(session)
    return UniverseResponse(symbols=symbols, generated_at=datetime.now(timezone.utc))


@app.post("/universe/override", response_model=OverrideResponse, status_code=201)
def override_symbol(request: OverrideRequest, session: Session = Depends(get_session)) -> OverrideResponse:
    symbol = request.symbol.strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="Symbol must not be empty")

    if request.reason is not None and not request.reason.strip():
        raise HTTPException(status_code=400, detail="Override reason must not be empty when provided")

    actor = (request.actor or "universe_service").strip()
    if not actor:
        raise HTTPException(status_code=400, detail="Actor must not be empty")

    latest_overrides = _latest_manual_overrides(session)
    previous = latest_overrides.get(symbol)

    now = datetime.now(timezone.utc)
    details: Dict[str, object] = {}
    if request.reason:
        details["reason"] = request.reason

    override_record = UniverseWhitelist(
        asset_id=symbol,
        as_of=now,
        source=MANUAL_OVERRIDE_SOURCE,
        approved=request.enabled,
        details=details,
    )
    session.add(override_record)

    audit_payload = {
        "previous": {
            "approved": previous.approved if previous else None,
            "reason": (previous.details or {}).get("reason") if previous else None,
        },
        "current": {
            "approved": request.enabled,
            "reason": request.reason,
        },
    }

    audit_entry = AuditLog(
        event_id=uuid4(),
        entity_type="universe.symbol",
        entity_id=symbol,
        actor=actor,
        action="universe.override.enabled" if request.enabled else "universe.override.disabled",
        event_time=now,
        attributes=audit_payload,
    )
    session.add(audit_entry)

    session.commit()

    return OverrideResponse(
        symbol=symbol,
        enabled=request.enabled,
        reason=request.reason,
        actor=actor,
        updated_at=now,
        audit_event_id=audit_entry.event_id,
    )


__all__ = [
    "app",
    "approved_universe",
    "override_symbol",
]

