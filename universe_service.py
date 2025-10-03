"""FastAPI service exposing the approved trading universe.

The service periodically gathers market data in order to produce a whitelist
of tradeable symbols.  Operators can manually override the computed
eligibility of any symbol and the action is recorded in an audit log for
traceability.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, String, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from services.common.security import require_admin_account


LOGGER = logging.getLogger("universe.service")


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./universe.db")


Base = declarative_base()


class UniverseWhitelist(Base):
    """SQLAlchemy model storing the computed trading universe."""

    __tablename__ = "universe_whitelist"

    symbol: str = Column(String, primary_key=True)
    enabled: bool = Column(Boolean, nullable=False, default=True)
    metrics_json: Dict[str, float] = Column(JSON, nullable=False, default=dict)
    ts: datetime = Column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )


class AuditLog(Base):
    """Audit log entry capturing manual overrides."""

    __tablename__ = "audit_log"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    symbol: str = Column(String, nullable=False)
    enabled: bool = Column(Boolean, nullable=False)
    reason: str = Column(String, nullable=False)
    created_at: datetime = Column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )


def _create_engine() -> Engine:
    return create_engine(DATABASE_URL, future=True)


ENGINE = _create_engine()
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)


def get_session() -> Iterable[Session]:
    """Provide a SQLAlchemy session scoped to the request lifecycle."""

    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


app = FastAPI(title="Universe Service")


MARKET_CAP_THRESHOLD = 1_000_000_000.0
GLOBAL_VOLUME_THRESHOLD = 100_000_000.0
KRAKEN_VOLUME_THRESHOLD = 10_000_000.0
ANNUALISED_VOL_THRESHOLD = 0.40


class UniverseThresholds(BaseModel):
    """Threshold configuration applied when computing the trading universe."""

    cap: float = Field(..., description="Minimum required market capitalisation in USD.")
    volume_global: float = Field(..., description="Minimum required global trading volume in USD.")
    volume_kraken: float = Field(..., description="Minimum required Kraken specific volume in USD.")
    ann_vol: float = Field(..., description="Minimum annualised volatility required to participate.")


class UniverseResponse(BaseModel):
    """Response payload returned by ``GET /universe/approved``."""

    symbols: List[str] = Field(..., description="Alphabetically sorted list of approved symbols.")
    generated_at: datetime = Field(..., description="Timestamp when the universe was generated.")
    thresholds: UniverseThresholds


class OverrideRequest(BaseModel):
    """Payload required to override a symbol's eligibility."""

    symbol: str = Field(..., description="Symbol that should be toggled", example="BTC")
    enabled: bool = Field(..., description="Whether the symbol should be part of the universe.")
    reason: str = Field(..., description="Explanation for the manual intervention.")


def _initialise_database() -> None:
    Base.metadata.create_all(bind=ENGINE)


def fetch_coingecko_market_data() -> Dict[str, Dict[str, float]]:
    """Fetch market capitalisation and global volume from CoinGecko.

    The function attempts to retrieve live data and falls back to a small static
    sample in the event of network failures.  The resulting mapping is keyed by
    the uppercase asset symbol.
    """

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "price_change_percentage": "24h",
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        payload = response.json()
        result: Dict[str, Dict[str, float]] = {}
        for entry in payload:
            symbol = str(entry.get("symbol", "")).upper()
            if not symbol:
                continue
            result[symbol] = {
                "market_cap": float(entry.get("market_cap") or 0.0),
                "global_volume": float(entry.get("total_volume") or 0.0),
            }
        if result:
            return result
    except (requests.RequestException, ValueError) as exc:  # pragma: no cover - network fallback
        LOGGER.warning("Failed to fetch CoinGecko data: %s", exc)

    # deterministic fallback ensures the service remains functional without
    # external connectivity.
    return {
        "BTC": {"market_cap": 400_000_000_000.0, "global_volume": 150_000_000_000.0},
        "ETH": {"market_cap": 200_000_000_000.0, "global_volume": 80_000_000_000.0},
        "DOGE": {"market_cap": 12_000_000_000.0, "global_volume": 5_000_000_000.0},
    }


def fetch_kraken_volume(symbols: Iterable[str]) -> Dict[str, float]:
    """Stub for fetching Kraken specific traded volume.

    An integration with the internal market data warehouse can replace this
    implementation at a later time.
    """

    return {symbol: 20_000_000.0 for symbol in symbols}


def fetch_annualised_volatility(symbols: Iterable[str]) -> Dict[str, float]:
    """Stub for computing annualised volatility for each symbol."""

    return {symbol: 0.45 for symbol in symbols}


def _latest_manual_overrides(
    session: Session, symbols: Iterable[str]
) -> Dict[str, Tuple[bool, str, datetime]]:
    """Return the most recent manual override per symbol."""

    symbol_list = sorted({symbol.upper() for symbol in symbols})
    if not symbol_list:
        return {}

    overrides: Dict[str, Tuple[bool, str, datetime]] = {}
    stmt = (
        select(AuditLog.symbol, AuditLog.enabled, AuditLog.reason, AuditLog.created_at)
        .where(AuditLog.symbol.in_(symbol_list))
        .order_by(AuditLog.symbol, AuditLog.created_at.desc())
    )

    for symbol, enabled, reason, created_at in session.execute(stmt):
        if symbol not in overrides:
            overrides[symbol] = (enabled, reason, created_at)

    return overrides


def _compute_universe(session: Session) -> datetime:
    """Compute and persist the approved trading universe.

    Returns the timestamp representing when the universe was generated.
    """

    market_data = fetch_coingecko_market_data()
    symbols = list(market_data.keys())
    kraken_volume = fetch_kraken_volume(symbols)
    annualised_vol = fetch_annualised_volatility(symbols)
    overrides = _latest_manual_overrides(session, symbols)
    generated_at = datetime.now(timezone.utc)

    for symbol, metrics in market_data.items():
        metrics_blob = {
            "cap": metrics.get("market_cap", 0.0),
            "volume_global": metrics.get("global_volume", 0.0),
            "volume_kraken": kraken_volume.get(symbol, 0.0),
            "ann_vol": annualised_vol.get(symbol, 0.0),
        }

        passes_thresholds = (
            metrics_blob["cap"] >= MARKET_CAP_THRESHOLD
            and metrics_blob["volume_global"] >= GLOBAL_VOLUME_THRESHOLD
            and metrics_blob["volume_kraken"] >= KRAKEN_VOLUME_THRESHOLD
            and metrics_blob["ann_vol"] >= ANNUALISED_VOL_THRESHOLD
        )

        entry = session.get(UniverseWhitelist, symbol)
        if entry is None:
            entry = UniverseWhitelist(symbol=symbol)
            session.add(entry)

        override_details = overrides.get(symbol)
        override_enabled: Optional[bool] = None
        if override_details is not None:
            override_enabled, override_reason, override_ts = override_details
            metrics_blob["override_reason"] = override_reason
            metrics_blob["override_at"] = override_ts.isoformat()
        metrics_blob["computed_enabled"] = passes_thresholds
        metrics_blob["override_applied"] = override_enabled is not None

        entry.metrics_json = metrics_blob
        entry.ts = generated_at
        entry.enabled = override_enabled if override_enabled is not None else passes_thresholds

    session.commit()
    return generated_at


def _refresh_universe_periodically() -> None:
    """Background task that recomputes the universe every 24 hours."""

    async def _run() -> None:
        while True:
            try:
                with SessionLocal() as session:
                    _compute_universe(session)
            except SQLAlchemyError as exc:  # pragma: no cover - defensive logging
                LOGGER.exception("Failed to refresh trading universe: %s", exc)
            await asyncio.sleep(timedelta(days=1).total_seconds())

    asyncio.create_task(_run())


@app.on_event("startup")
async def _startup_event() -> None:
    """Initialise the database schema and compute the first universe."""

    _initialise_database()
    with SessionLocal() as session:
        _compute_universe(session)
    _refresh_universe_periodically()


@app.get("/universe/approved", response_model=UniverseResponse)
def get_universe(
    session: Session = Depends(get_session),
    _: str = Depends(require_admin_account),
) -> UniverseResponse:
    """Return the currently approved trading universe."""

    entries = session.execute(
        select(UniverseWhitelist).where(UniverseWhitelist.enabled.is_(True))
    ).scalars()
    symbols = sorted({entry.symbol for entry in entries})

    latest_generated = session.execute(
        select(UniverseWhitelist.ts).order_by(UniverseWhitelist.ts.desc())
    ).scalars().first()

    if latest_generated is None:
        raise HTTPException(status_code=404, detail="Universe has not been generated yet.")

    thresholds = UniverseThresholds(
        cap=MARKET_CAP_THRESHOLD,
        volume_global=GLOBAL_VOLUME_THRESHOLD,
        volume_kraken=KRAKEN_VOLUME_THRESHOLD,
        ann_vol=ANNUALISED_VOL_THRESHOLD,
    )

    return UniverseResponse(symbols=symbols, generated_at=latest_generated, thresholds=thresholds)


@app.post("/universe/override", status_code=204)
def override_symbol(
    payload: OverrideRequest,
    request: Request,
    session: Session = Depends(get_session),
    actor: str = Depends(require_admin_account),
) -> Response:
    """Manually toggle a symbol's eligibility and record the action."""

    symbol = payload.symbol.upper()
    entry = session.get(UniverseWhitelist, symbol)
    if entry is None:
        entry = UniverseWhitelist(symbol=symbol, metrics_json={})
        session.add(entry)

    header_account = (request.headers.get("X-Account-ID") or "").strip().lower()
    normalized_actor = actor.strip().lower()
    if header_account and header_account != normalized_actor:
        raise HTTPException(
            status_code=403,
            detail="Account scope does not match authenticated administrator.",
        )

    request_scopes = getattr(request.state, "account_scopes", None)
    if request_scopes:
        normalized_scopes = {
            str(scope).strip().lower()
            for scope in request_scopes
            if str(scope or "").strip()
        }
        if normalized_scopes and normalized_actor not in normalized_scopes:
            raise HTTPException(
                status_code=403,
                detail="Authenticated administrator lacks access to the requested account scope.",
            )

    entry.enabled = payload.enabled
    entry.ts = datetime.now(timezone.utc)

    audit_row = AuditLog(symbol=symbol, enabled=payload.enabled, reason=payload.reason)
    session.add(audit_row)
    session.commit()

    return Response(status_code=204)


__all__ = ["app"]

