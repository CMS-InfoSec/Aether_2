"""FastAPI service exposing volatility analytics for traded symbols."""
from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from typing import Any, Iterator, List, Optional, Sequence


from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from prometheus_client import Gauge
from alembic import command
from alembic.config import Config
from sqlalchemy import Column, DateTime, Float, String, create_engine, func, select
from sqlalchemy.engine import Engine, URL
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

import metrics
from services.common.spot import require_spot_http

logger = logging.getLogger(__name__)


def _require_database_url() -> URL:
    primary = os.getenv("ANALYTICS_DATABASE_URL")
    fallback = os.getenv("TIMESCALE_DATABASE_URI")
    raw_url = primary or fallback

    if not raw_url:
        raise RuntimeError(
            "ANALYTICS_DATABASE_URL must be defined and point to a Timescale/PostgreSQL database."
        )

    normalised = _normalise_database_url(raw_url)

    try:
        url = make_url(normalised)
    except ArgumentError as exc:  # pragma: no cover - configuration error
        raise RuntimeError(f"Invalid analytics database URL '{raw_url}': {exc}") from exc

    drivername = url.drivername.lower()
    if not drivername.startswith("postgresql"):
        raise RuntimeError(
            "Volatility service requires a PostgreSQL/TimescaleDSN; received "
            f"driver '{url.drivername}'."
        )

    return url


def _engine_options(url: URL) -> dict[str, Any]:
    options: dict[str, Any] = {
        "future": True,
        "pool_pre_ping": True,
        "pool_size": int(os.getenv("ANALYTICS_DB_POOL_SIZE", "15")),
        "max_overflow": int(os.getenv("ANALYTICS_DB_MAX_OVERFLOW", "10")),
        "pool_timeout": int(os.getenv("ANALYTICS_DB_POOL_TIMEOUT", "30")),
        "pool_recycle": int(os.getenv("ANALYTICS_DB_POOL_RECYCLE", "1800")),
    }

    connect_args: dict[str, Any] = {}

    forced_sslmode = os.getenv("ANALYTICS_DB_SSLMODE")
    if forced_sslmode:
        connect_args["sslmode"] = forced_sslmode
    elif "sslmode" not in url.query and url.host not in {None, "localhost", "127.0.0.1"}:
        connect_args["sslmode"] = "require"

    if connect_args:
        options["connect_args"] = connect_args

    return options


def _normalise_database_url(url: str) -> str:
    """Ensure SQLAlchemy uses the psycopg2 dialect when possible."""

    if url.startswith("postgresql+psycopg://"):
        return "postgresql+psycopg2://" + url[len("postgresql+psycopg://") :]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg2://" + url[len("postgresql://") :]
    if url.startswith("postgres://"):
        return "postgresql+psycopg2://" + url[len("postgres://") :]
    return url


DATABASE_URL: Optional[URL] = None
ENGINE: Optional[Engine] = None

SessionFactory: Optional[sessionmaker] = None


def _create_engine(url: URL) -> Engine:
    return create_engine(
        url.render_as_string(hide_password=False),
        **_engine_options(url),
    )


_MIGRATIONS_PATH = Path(__file__).resolve().parents[2] / "data" / "migrations"



def run_migrations(url: URL) -> None:

    """Apply all outstanding Timescale migrations for analytics data."""

    _ensure_session_factory()

    if DATABASE_URL is None:
        raise RuntimeError("Analytics database URL not configured; ensure startup completed.")

    config = Config()
    config.set_main_option("script_location", str(_MIGRATIONS_PATH))
    config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False))
    config.attributes["configure_logger"] = False

    command.upgrade(config, "head")


Base = declarative_base()


class OhlcvBar(Base):
    """Representation of the ``ohlcv_bars`` table."""

    __tablename__ = "ohlcv_bars"

    market = Column(String, primary_key=True)
    bucket_start = Column(DateTime(timezone=True), primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)


class VolatilityMetric(Base):
    """Persistence model for computed volatility metrics."""

    __tablename__ = "vol_metrics"

    symbol = Column(String, primary_key=True)
    realized_vol = Column(Float, nullable=False)
    garch_vol = Column(Float, nullable=False)
    jump_prob = Column(Float, nullable=False)
    atr = Column(Float, nullable=False)
    band_width = Column(Float, nullable=False)
    ts = Column(DateTime(timezone=True), primary_key=True)


@dataclass
class SeriesPoint:
    bucket_start: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass
class VolatilityAnalytics:
    symbol: str
    window: int
    realized_vol: float
    garch_vol: float
    jump_probability: float
    atr: float
    band_width: float
    timestamp: datetime


class VolatilityResponse(BaseModel):
    symbol: str = Field(..., description="Symbol for which volatility was evaluated")
    window: int = Field(..., description="Number of observations in the rolling window")
    realized_vol: float = Field(..., description="Sample standard deviation of log returns")
    garch_vol: float = Field(..., description="Forecast volatility from a simple GARCH(1,1) model")
    jump_probability: float = Field(..., description="Share of returns exceeding a three-sigma move")
    atr: float = Field(..., description="Average True Range over the requested window")
    bollinger_bandwidth: float = Field(..., description="Normalised Bollinger band width")
    as_of: datetime = Field(..., description="Timestamp for the analytics snapshot")


app = FastAPI(title="Volatility Analytics Service")
app.state.analytics_database_url = None
app.state.analytics_engine = None
app.state.analytics_sessionmaker = None
metrics.setup_metrics(app, service_name="volatility-service")


_VOLATILITY_INDEX: Gauge | None = None


def _volatility_gauge() -> Gauge:
    global _VOLATILITY_INDEX
    if _VOLATILITY_INDEX is None:
        metrics.init_metrics("volatility-service")
        _VOLATILITY_INDEX = Gauge(
            "volatility_index",
            "Realized volatility derived from log returns",
            ["symbol"],
            registry=metrics._REGISTRY,
        )
    return _VOLATILITY_INDEX


def _initialise_database() -> sessionmaker:
    """Initialise database connectivity once configuration is available."""

    url = _require_database_url()
    engine = create_engine(
        url.render_as_string(hide_password=False),
        **_engine_options(url),
    )
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    return _register_database(url, engine, session_factory)


@app.on_event("startup")
def _on_startup() -> None:

    """Initialise database connectivity and migrations when the app boots."""

    global DATABASE_URL, ENGINE, SessionFactory

    url = _require_database_url()
    engine = _create_engine(url)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    VolatilityMetric.__table__.create(bind=engine, checkfirst=True)
    run_migrations(url)

    DATABASE_URL = url
    ENGINE = engine
    SessionFactory = session_factory


    app.state.analytics_database_url = url
    app.state.analytics_engine = engine
    app.state.analytics_sessionmaker = session_factory


def get_session(request: Request) -> Iterator[Session]:
    session_factory: Optional[sessionmaker] = getattr(request.app.state, "analytics_sessionmaker", None)

    if session_factory is None:
        session_factory = SessionFactory

    if session_factory is None:
        raise RuntimeError(
            "Analytics database session factory is not initialised. Ensure the startup event has run and the "
            "ANALYTICS_DATABASE_URL environment variable is configured."
        )


    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _require_history(rows: Sequence[SeriesPoint], symbol: str, window: int) -> None:
    if len(rows) < window + 1:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Insufficient history for {symbol}; required {window + 1} observations",
        )


def _fetch_ohlcv(session: Session, symbol: str, window: int) -> List[SeriesPoint]:
    normalized = _normalize_symbol(symbol)
    stmt = (
        select(OhlcvBar)
        .where(func.upper(OhlcvBar.market) == normalized)
        .order_by(OhlcvBar.bucket_start.desc())
        .limit(window + 1)
    )
    rows = list(reversed(session.execute(stmt).scalars().all()))
    return [
        SeriesPoint(
            bucket_start=row.bucket_start,
            open=float(row.open),
            high=float(row.high),
            low=float(row.low),
            close=float(row.close),
        )
        for row in rows
    ]


def _log_returns(points: Sequence[SeriesPoint]) -> List[float]:
    returns: List[float] = []
    for prev, curr in zip(points, points[1:]):
        if prev.close <= 0 or curr.close <= 0:
            continue
        returns.append(math.log(curr.close / prev.close))
    return returns


def _realized_volatility(log_returns: Sequence[float]) -> float:
    if not log_returns:
        return 0.0
    mean = sum(log_returns) / len(log_returns)
    variance = sum((value - mean) ** 2 for value in log_returns) / max(len(log_returns) - 1, 1)
    return math.sqrt(max(variance, 0.0))


def _garch_forecast(log_returns: Sequence[float]) -> float:
    if not log_returns:
        return 0.0
    variance = _realized_volatility(log_returns) ** 2
    if variance <= 0:
        return 0.0

    alpha = 0.1
    beta = 0.85
    omega = variance * (1 - alpha - beta)
    sigma2 = variance
    for ret in log_returns:
        sigma2 = omega + alpha * (ret**2) + beta * sigma2
    return math.sqrt(max(sigma2, 0.0))


def _jump_probability(log_returns: Sequence[float]) -> float:
    if not log_returns:
        return 0.0
    mean = sum(log_returns) / len(log_returns)
    std = _realized_volatility(log_returns)
    if std == 0:
        return 0.0
    threshold = 3.0
    jumps = sum(1 for value in log_returns if abs((value - mean) / std) >= threshold)
    return jumps / len(log_returns)


def _average_true_range(points: Sequence[SeriesPoint], window: int) -> float:
    if len(points) < 2:
        return 0.0
    true_ranges: List[float] = []
    for previous, current in zip(points, points[1:]):
        high_low = current.high - current.low
        high_close = abs(current.high - previous.close)
        low_close = abs(current.low - previous.close)
        true_ranges.append(max(high_low, high_close, low_close))
    if not true_ranges:
        return 0.0
    return sum(true_ranges[-window:]) / min(window, len(true_ranges))


def _bollinger_bandwidth(points: Sequence[SeriesPoint]) -> float:
    closes = [point.close for point in points]
    if len(closes) < 2:
        return 0.0
    mean = sum(closes) / len(closes)
    variance = sum((price - mean) ** 2 for price in closes) / max(len(closes) - 1, 1)
    std = math.sqrt(max(variance, 0.0))
    if mean == 0:
        return 0.0
    upper = mean + 2 * std
    lower = mean - 2 * std
    return (upper - lower) / mean if mean != 0 else 0.0


def _compute_metrics(points: Sequence[SeriesPoint], symbol: str, window: int) -> VolatilityAnalytics:
    _require_history(points, symbol, window)
    log_returns = _log_returns(points)
    realized = _realized_volatility(log_returns)
    garch = _garch_forecast(log_returns)
    jumps = _jump_probability(log_returns)
    atr = _average_true_range(points[-(window + 1) :], window)
    bandwidth = _bollinger_bandwidth(points[-window:])
    timestamp = points[-1].bucket_start
    return VolatilityAnalytics(
        symbol=_normalize_symbol(symbol),
        window=window,
        realized_vol=realized,
        garch_vol=garch,
        jump_probability=jumps,
        atr=atr,
        band_width=bandwidth,
        timestamp=timestamp,
    )


def _persist_metrics(session: Session, metrics_payload: VolatilityAnalytics) -> None:
    record = VolatilityMetric(
        symbol=metrics_payload.symbol,
        realized_vol=metrics_payload.realized_vol,
        garch_vol=metrics_payload.garch_vol,
        jump_prob=metrics_payload.jump_probability,
        atr=metrics_payload.atr,
        band_width=metrics_payload.band_width,
        ts=metrics_payload.timestamp,
    )
    session.merge(record)
    session.commit()


def _update_gauge(metrics_payload: VolatilityAnalytics) -> None:
    gauge = _volatility_gauge()
    gauge.labels(symbol=metrics_payload.symbol).set(metrics_payload.realized_vol)


def _build_response(metrics_payload: VolatilityAnalytics) -> VolatilityResponse:
    return VolatilityResponse(
        symbol=metrics_payload.symbol,
        window=metrics_payload.window,
        realized_vol=metrics_payload.realized_vol,
        garch_vol=metrics_payload.garch_vol,
        jump_probability=metrics_payload.jump_probability,
        atr=metrics_payload.atr,
        bollinger_bandwidth=metrics_payload.band_width,
        as_of=metrics_payload.timestamp,
    )


def _evaluate(session: Session, symbol: str, window: int) -> VolatilityResponse:
    normalized = require_spot_http(symbol)
    points = _fetch_ohlcv(session, normalized, window)
    metrics_payload = _compute_metrics(points, normalized, window)
    _persist_metrics(session, metrics_payload)
    _update_gauge(metrics_payload)
    return _build_response(metrics_payload)


@app.get("/volatility/realized", response_model=VolatilityResponse)
def realized_volatility(
    *,
    symbol: str = Query(..., description="Instrument symbol", min_length=1),
    window: int = Query(30, ge=5, le=500, description="Number of bars to evaluate"),
    session: Session = Depends(get_session),
) -> VolatilityResponse:
    return _evaluate(session, symbol, window)


@app.get("/volatility/garch", response_model=VolatilityResponse)
def garch_volatility(
    *,
    symbol: str = Query(..., description="Instrument symbol", min_length=1),
    window: int = Query(30, ge=5, le=500, description="Number of bars to evaluate"),
    session: Session = Depends(get_session),
) -> VolatilityResponse:
    return _evaluate(session, symbol, window)


@app.get("/volatility/jump_test", response_model=VolatilityResponse)
def jump_test(
    *,
    symbol: str = Query(..., description="Instrument symbol", min_length=1),
    window: int = Query(30, ge=5, le=500, description="Number of bars to evaluate"),
    session: Session = Depends(get_session),
) -> VolatilityResponse:
    return _evaluate(session, symbol, window)
