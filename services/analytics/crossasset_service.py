"""FastAPI service exposing cross-asset analytics endpoints.

The service provides synthetic yet deterministic market data to estimate
lead/lag relationships, rolling beta between assets, and deviation of
stablecoins from their USD pegs.  Results are persisted into a simple SQL
back-end to mimic operational storage of computed metrics.
"""

from __future__ import annotations

import logging
import math
import os
import statistics
from datetime import datetime, timezone
from typing import Iterable, Sequence

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, Float, String, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

LOGGER = logging.getLogger(__name__)

Base = declarative_base()


class CrossAssetMetric(Base):
    """SQLAlchemy model storing computed cross-asset analytics."""

    __tablename__ = "crossasset_metrics"

    pair = Column(String, primary_key=True)
    metric_type = Column(String, primary_key=True)
    ts = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float, nullable=False)


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


DATABASE_URL = (
    os.getenv("CROSSASSET_DATABASE_URL")
    or os.getenv("ANALYTICS_DATABASE_URL")
    or os.getenv("DATABASE_URL")
    or "sqlite:///./crossasset.db"
)


def _engine_options(url: str) -> dict[str, object]:
    options: dict[str, object] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


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


def _synthetic_price_series(symbol: str, length: int = 240) -> list[float]:
    """Generate a deterministic synthetic price series for ``symbol``.

    The generation uses trigonometric functions to produce pseudo-market
    behaviour while remaining deterministic across invocations.  This keeps the
    service self-contained and easily testable.
    """

    # Normalise symbol to maintain determinism regardless of case.
    normalised = symbol.upper()
    seed = sum(ord(char) for char in normalised)
    base_level = 80 + (seed % 60)
    series: list[float] = []
    price = float(base_level)
    for index in range(length):
        seasonal = math.sin(index / 12 + seed % 11) * 0.005
        drift = math.cos(index / 60 + seed % 7) * 0.002
        noise = ((seed * (index + 1)) % 997) / 9970 - 0.05
        price *= 1 + seasonal + drift + noise / 50
        price = max(price, 0.5)
        series.append(round(price, 8))
    return series


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

    base_series = _synthetic_price_series(base)
    target_series = _synthetic_price_series(target)

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

    alt_series = _synthetic_price_series(alt)
    base_series = _synthetic_price_series(base)
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

    series = _synthetic_price_series(symbol)
    price = series[-1]
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
