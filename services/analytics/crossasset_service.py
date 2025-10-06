"""FastAPI service exposing cross-asset analytics endpoints.

The service reads historical OHLCV bars from the Timescale-backed
``ohlcv_bars`` hypertable populated by the ingestion pipeline.  Using the
authoritative market data ensures lead/lag relationships, rolling beta, and
stablecoin deviation metrics reflect production state.  Computed metrics are
persisted into a lightweight SQL table for audit and downstream reporting.
"""

from __future__ import annotations

import logging
import math
import os
import statistics
import sys
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence

from fastapi import FastAPI, HTTPException, Query
from prometheus_client import Gauge
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, Float, String, create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from shared.postgres import normalize_sqlalchemy_dsn

LOGGER = logging.getLogger(__name__)

DATA_STALENESS_GAUGE = Gauge(
    "crossasset_data_age_seconds",
    "Age of the latest OHLCV bar used for cross-asset analytics.",
    ["symbol"],
)

DEFAULT_WINDOW_POINTS = 240
MIN_REQUIRED_POINTS = 30
STALE_THRESHOLD = timedelta(hours=6)
STABLECOIN_STALE_THRESHOLD = timedelta(hours=12)

Base = declarative_base()


class CrossAssetMetric(Base):
    """SQLAlchemy model storing computed cross-asset analytics."""

    __tablename__ = "crossasset_metrics"

    pair = Column(String, primary_key=True)
    metric_type = Column(String, primary_key=True)
    ts = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float, nullable=False)


class OhlcvBar(Base):
    """Subset of the ``ohlcv_bars`` table used for analytics."""

    __tablename__ = "ohlcv_bars"

    market = Column(String, primary_key=True)
    bucket_start = Column(DateTime(timezone=True), primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float, nullable=False)
    volume = Column(Float)


class LeadLagResponse(BaseModel):
    """Response payload for the lead/lag endpoint."""

    pair: str = Field(..., description="Asset pair being analysed")
    correlation: float = Field(..., description="Pearson correlation between assets")
    lag: int = Field(..., description="Lag (in observations) that maximises correlation")
    ts: datetime = Field(..., description="Timestamp the metric was generated")


class BetaResponse(BaseModel):
    """Response payload for the rolling beta endpoint."""

    pair: str = Field(..., description="Alt/Base pair used for beta calculation")
    beta: float = Field(..., description="Rolling beta estimate")
    ts: datetime = Field(..., description="Timestamp the metric was generated")


class StablecoinResponse(BaseModel):
    """Response payload for stablecoin peg deviation."""

    symbol: str = Field(..., description="Stablecoin trading symbol (e.g. USDT/USD)")
    price: float = Field(..., description="Latest synthetic price for the symbol")
    deviation: float = Field(..., description="Difference from the 1.0 peg")
    deviation_bps: float = Field(..., description="Deviation expressed in basis points")
    ts: datetime = Field(..., description="Timestamp the metric was generated")


_SQLITE_FALLBACK = "sqlite+pysqlite:///:memory:"


def _resolve_database_url() -> str:
    """Return the configured database URL, enforcing PostgreSQL in production."""

    allow_sqlite = "pytest" in sys.modules

    raw = (
        os.getenv("CROSSASSET_DATABASE_URL")
        or os.getenv("ANALYTICS_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or (_SQLITE_FALLBACK if allow_sqlite else None)
    )

    if raw is None:
        raise RuntimeError(
            "Cross-asset analytics database DSN is not configured. Set CROSSASSET_DATABASE_URL "
            "or ANALYTICS_DATABASE_URL to a PostgreSQL/Timescale connection string."
        )

    candidate = raw.strip()
    if not candidate:
        raise RuntimeError("Cross-asset analytics database DSN cannot be empty once configured.")

    return normalize_sqlalchemy_dsn(
        candidate,
        allow_sqlite=allow_sqlite,
        label="Cross-asset analytics database DSN",
    )


def _engine_options(url: str) -> dict[str, object]:
    options: dict[str, object] = {"future": True}
    if url.startswith("sqlite://") or url.startswith("sqlite+pysqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


DATABASE_URL = _resolve_database_url()
ENGINE: Engine = create_engine(DATABASE_URL, **_engine_options(DATABASE_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

app = FastAPI(title="Cross-Asset Analytics Service", version="1.0.0")


@app.on_event("startup")
def _create_tables() -> None:
    """Ensure the analytics table exists before serving requests."""

    try:
        Base.metadata.create_all(ENGINE)
    except SQLAlchemyError as exc:  # pragma: no cover - defensive logging
        LOGGER.exception("Failed to initialise crossasset tables: %s", exc)
        raise


def _coerce_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _load_price_series(session: Session, symbol: str, window: int) -> tuple[list[float], datetime | None]:
    normalised = symbol.strip().upper()
    stmt = (
        select(OhlcvBar.bucket_start, OhlcvBar.close)
        .where(func.upper(OhlcvBar.market) == normalised)
        .order_by(OhlcvBar.bucket_start.desc())
        .limit(window)
    )

    try:
        rows = session.execute(stmt).all()
    except SQLAlchemyError as exc:
        LOGGER.exception("Failed to load OHLCV bars for %s", normalised)
        raise HTTPException(status_code=503, detail="Database error while loading market data") from exc

    series: list[float] = []
    latest_ts: datetime | None = None
    for bucket_start, close in rows:
        if close is None:
            continue
        series.append(float(close))
        if latest_ts is None:
            latest_ts = _coerce_datetime(bucket_start)

    series.reverse()
    return series, latest_ts


def _record_data_age(symbol: str, observed_at: datetime | None, *, max_age: timedelta) -> None:
    symbol_upper = symbol.strip().upper()
    if observed_at is None:
        DATA_STALENESS_GAUGE.labels(symbol=symbol_upper).set(float("inf"))
        raise HTTPException(status_code=404, detail=f"No price history for {symbol_upper}")

    observed = observed_at.astimezone(timezone.utc)
    age_seconds = max(0.0, (datetime.now(timezone.utc) - observed).total_seconds())
    DATA_STALENESS_GAUGE.labels(symbol=symbol_upper).set(age_seconds)
    if age_seconds > max_age.total_seconds():
        detail = f"Price history for {symbol_upper} is stale ({int(age_seconds)}s old)"
        LOGGER.warning(detail)
        raise HTTPException(status_code=503, detail=detail)


def _require_series(series: Sequence[float], symbol: str, window: int) -> None:
    if len(series) < window:
        raise HTTPException(
            status_code=404,
            detail=f"Insufficient price history for {symbol.upper()}; required {window} points",
        )


def _align_series(series_a: Sequence[float], series_b: Sequence[float]) -> tuple[list[float], list[float]]:
    length = min(len(series_a), len(series_b))
    if length == 0:
        return [], []
    return list(series_a[-length:]), list(series_b[-length:])


def _pearson_correlation(series_a: Sequence[float], series_b: Sequence[float]) -> float:
    """Compute the Pearson correlation between two equally sized sequences."""

    if not series_a or not series_b:
        raise HTTPException(status_code=422, detail="Insufficient data for correlation")

    length = min(len(series_a), len(series_b))
    if length < 2:
        raise HTTPException(status_code=422, detail="Need at least two observations")

    a = series_a[-length:]
    b = series_b[-length:]
    mean_a = statistics.fmean(a)
    mean_b = statistics.fmean(b)
    numerator = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b))
    denominator_a = math.sqrt(sum((x - mean_a) ** 2 for x in a))
    denominator_b = math.sqrt(sum((y - mean_b) ** 2 for y in b))
    if denominator_a == 0 or denominator_b == 0:
        raise HTTPException(status_code=422, detail="Zero variance encountered")
    return numerator / (denominator_a * denominator_b)


def _lag_coefficient(series_a: Sequence[float], series_b: Sequence[float], max_lag: int = 10) -> int:
    """Return the lag (in observations) that maximises correlation."""

    best_lag = 0
    best_corr = float("-inf")
    for lag in range(-max_lag, max_lag + 1):
        if lag < 0:
            aligned_a = series_a[: lag or None]
            aligned_b = series_b[-lag:]
        elif lag > 0:
            aligned_a = series_a[lag:]
            aligned_b = series_b[: -lag or None]
        else:
            aligned_a = series_a
            aligned_b = series_b

        if len(aligned_a) < 2 or len(aligned_b) < 2:
            continue
        corr = _pearson_correlation(aligned_a, aligned_b)
        if corr > best_corr:
            best_corr = corr
            best_lag = lag
    return best_lag


def _rolling_beta(series_alt: Sequence[float], series_base: Sequence[float], window: int = 60) -> float:
    """Compute a simple rolling beta estimate using the last ``window`` points."""

    if not series_alt or not series_base:
        raise HTTPException(status_code=422, detail="Insufficient data for beta calculation")

    length = min(len(series_alt), len(series_base))
    if length < window:
        raise HTTPException(status_code=422, detail=f"Need at least {window} observations")

    alt_window = series_alt[-window:]
    base_window = series_base[-window:]
    mean_alt = statistics.fmean(alt_window)
    mean_base = statistics.fmean(base_window)

    covariance = sum((a - mean_alt) * (b - mean_base) for a, b in zip(alt_window, base_window))
    variance = sum((b - mean_base) ** 2 for b in base_window)
    if variance == 0:
        raise HTTPException(status_code=422, detail="Base asset variance is zero")
    return covariance / variance


def _store_metrics(session: Session, pair: str, metrics: Iterable[tuple[str, float]], ts: datetime) -> None:
    """Persist metrics for a pair at ``ts`` using ``session``."""

    for metric_type, value in metrics:
        record = CrossAssetMetric(pair=pair, metric_type=metric_type, value=float(value), ts=ts)
        session.merge(record)


def _persist_metrics(pair: str, metrics: Iterable[tuple[str, float]], ts: datetime) -> None:
    with SessionLocal() as session:
        _store_metrics(session, pair, metrics, ts)
        session.commit()


@app.get("/crossasset/leadlag", response_model=LeadLagResponse)
def lead_lag(
    base: str = Query(..., description="Base asset symbol"),
    target: str = Query(..., description="Target asset symbol"),
) -> LeadLagResponse:
    """Return correlation and lag coefficient for the provided pair."""

    if not base or not target:
        raise HTTPException(status_code=422, detail="Both base and target are required")

    with SessionLocal() as session:
        base_series, base_ts = _load_price_series(session, base, window=DEFAULT_WINDOW_POINTS)
        target_series, target_ts = _load_price_series(session, target, window=DEFAULT_WINDOW_POINTS)

    _record_data_age(base, base_ts, max_age=STALE_THRESHOLD)
    _record_data_age(target, target_ts, max_age=STALE_THRESHOLD)
    _require_series(base_series, base, MIN_REQUIRED_POINTS)
    _require_series(target_series, target, MIN_REQUIRED_POINTS)

    base_series, target_series = _align_series(base_series, target_series)

    correlation = _pearson_correlation(base_series, target_series)
    lag = _lag_coefficient(base_series, target_series)
    ts = datetime.now(tz=timezone.utc)
    pair = f"{base.upper()}/{target.upper()}"

    _persist_metrics(pair, (("leadlag_correlation", correlation), ("leadlag_lag", float(lag))), ts)

    return LeadLagResponse(pair=pair, correlation=correlation, lag=lag, ts=ts)


@app.get("/crossasset/beta", response_model=BetaResponse)
def rolling_beta(
    alt: str = Query(..., description="Alt asset symbol"),
    base: str = Query(..., description="Base asset symbol"),
    window: int = Query(60, ge=10, le=240, description="Rolling window size"),
) -> BetaResponse:
    """Return a rolling beta estimate for the provided alt/base pair."""

    lookback = max(DEFAULT_WINDOW_POINTS, window * 3)
    with SessionLocal() as session:
        alt_series, alt_ts = _load_price_series(session, alt, window=lookback)
        base_series, base_ts = _load_price_series(session, base, window=lookback)

    _record_data_age(alt, alt_ts, max_age=STALE_THRESHOLD)
    _record_data_age(base, base_ts, max_age=STALE_THRESHOLD)
    _require_series(alt_series, alt, window)
    _require_series(base_series, base, window)

    alt_series, base_series = _align_series(alt_series, base_series)
    beta_value = _rolling_beta(alt_series, base_series, window=window)
    ts = datetime.now(tz=timezone.utc)
    pair = f"{alt.upper()}/{base.upper()}"

    _persist_metrics(pair, (("rolling_beta", beta_value),), ts)

    return BetaResponse(pair=pair, beta=beta_value, ts=ts)


@app.get("/crossasset/stablecoin", response_model=StablecoinResponse)
def stablecoin_deviation(
    symbol: str = Query(..., description="Stablecoin market symbol (e.g. USDT/USD)"),
) -> StablecoinResponse:
    """Return deviation of the stablecoin price from the 1.0 USD peg."""

    if "/" not in symbol:
        raise HTTPException(status_code=422, detail="Symbol must include the quoted currency, e.g. USDT/USD")

    with SessionLocal() as session:
        series, observed_ts = _load_price_series(session, symbol, window=DEFAULT_WINDOW_POINTS)

    _record_data_age(symbol, observed_ts, max_age=STABLECOIN_STALE_THRESHOLD)
    _require_series(series, symbol, 1)

    price = float(series[-1])
    deviation = price - 1.0
    deviation_bps = deviation * 10000
    ts = datetime.now(tz=timezone.utc)

    _persist_metrics(symbol.upper(), (("stablecoin_deviation", deviation),), ts)

    if abs(deviation) > 0.005:
        LOGGER.warning(
            "Stablecoin peg deviation detected: %s deviated by %.4f (%.2f bps)",
            symbol.upper(),
            deviation,
            deviation_bps,
        )

    return StablecoinResponse(
        symbol=symbol.upper(),
        price=price,
        deviation=deviation,
        deviation_bps=deviation_bps,
        ts=ts,
    )


__all__ = [
    "app",
    "lead_lag",
    "rolling_beta",
    "stablecoin_deviation",
]
