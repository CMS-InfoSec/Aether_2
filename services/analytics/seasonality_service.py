"""FastAPI service exposing historical seasonality analytics for Kraken symbols."""

from __future__ import annotations

import calendar
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Sequence

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, Float, String, create_engine, func, select
from sqlalchemy.engine import Engine, URL
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from auth.service import InMemorySessionStore, RedisSessionStore, SessionStoreProtocol
from services.common import security
from services.common.security import require_admin_account

__all__ = ["app"]


def _normalise_database_url(url: str) -> str:
    """Ensure SQLAlchemy uses the psycopg2 driver for PostgreSQL URLs."""

    if url.startswith("postgresql+psycopg://"):
        return "postgresql+psycopg2://" + url[len("postgresql+psycopg://") :]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg2://" + url[len("postgresql://") :]
    if url.startswith("postgres://"):
        return "postgresql+psycopg2://" + url[len("postgres://") :]
    return url


def _require_database_url() -> URL:
    """Resolve and validate the managed Timescale/PostgreSQL DSN."""

    primary = os.getenv("SEASONALITY_DATABASE_URI")
    fallback = os.getenv("TIMESCALE_DATABASE_URI") or os.getenv("DATABASE_URL")
    raw_url = primary or fallback

    if not raw_url:
        raise RuntimeError(
            "Seasonality service requires SEASONALITY_DATABASE_URI (or TIMESCALE_DATABASE_URI) "
            "to point at a managed Timescale/PostgreSQL database."
        )

    normalised = _normalise_database_url(raw_url)

    try:
        url = make_url(normalised)
    except ArgumentError as exc:  # pragma: no cover - configuration error
        raise RuntimeError(f"Invalid seasonality database URL '{raw_url}': {exc}") from exc

    if not url.drivername.lower().startswith("postgresql"):
        raise RuntimeError(
            "Seasonality service requires a PostgreSQL/Timescale DSN; "
            f"received driver '{url.drivername}'."
        )

    return url


def _engine_options(url: URL) -> Dict[str, Any]:
    options: Dict[str, Any] = {
        "future": True,
        "pool_pre_ping": True,
        "pool_size": int(os.getenv("SEASONALITY_DB_POOL_SIZE", "15")),
        "max_overflow": int(os.getenv("SEASONALITY_DB_MAX_OVERFLOW", "10")),
        "pool_timeout": int(os.getenv("SEASONALITY_DB_POOL_TIMEOUT", "30")),
        "pool_recycle": int(os.getenv("SEASONALITY_DB_POOL_RECYCLE", "1800")),
    }

    connect_args: Dict[str, Any] = {}

    forced_sslmode = os.getenv("SEASONALITY_DB_SSLMODE")
    if forced_sslmode:
        connect_args["sslmode"] = forced_sslmode
    elif "sslmode" not in url.query and url.host not in {None, "localhost", "127.0.0.1"}:
        connect_args["sslmode"] = "require"

    if connect_args:
        options["connect_args"] = connect_args

    return options


DATABASE_URL: Optional[URL] = None
ENGINE: Optional[Engine] = None
SessionFactory: Optional[sessionmaker] = None


def _create_engine(url: URL) -> Engine:
    return create_engine(
        url.render_as_string(hide_password=False),
        **_engine_options(url),
    )


OhlcvBase = declarative_base()
MetricsBase = declarative_base()


class OhlcvBar(OhlcvBase):
    """Minimal OHLCV representation backed by the historical bars table."""

    __tablename__ = "ohlcv_bars"

    market = Column(String, primary_key=True)
    bucket_start = Column(DateTime(timezone=True), primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)


class SeasonalityMetric(MetricsBase):
    """Persisted aggregate used to snapshot historical seasonality results."""

    __tablename__ = "seasonality_metrics"

    symbol = Column(String, primary_key=True)
    period = Column(String, primary_key=True)
    avg_return = Column(Float, nullable=False)
    avg_vol = Column(Float, nullable=False)
    avg_volume = Column(Float, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)


@dataclass
class Bar:
    """Internal representation of an OHLCV bar used for analytics computations."""

    timestamp: datetime
    close: float | None
    volume: float


@dataclass
class AggregatedMetric:
    """Computed metrics for a specific seasonality bucket."""

    key: str
    avg_return: float
    avg_vol: float
    avg_volume: float


class DayOfWeekMetric(BaseModel):
    weekday: str = Field(..., description="Weekday name", example="Monday")
    avg_return: float = Field(..., description="Average close-to-close return")
    avg_vol: float = Field(..., description="Standard deviation of returns")
    avg_volume: float = Field(..., description="Average traded volume")


class DayOfWeekResponse(BaseModel):
    symbol: str
    generated_at: datetime
    metrics: List[DayOfWeekMetric]


class HourOfDayMetric(BaseModel):
    hour: int = Field(..., ge=0, le=23, description="Hour of day in UTC")
    avg_return: float
    avg_vol: float
    avg_volume: float


class HourOfDayResponse(BaseModel):
    symbol: str
    generated_at: datetime
    metrics: List[HourOfDayMetric]


class SessionMetric(BaseModel):
    session: str = Field(..., description="Trading session label", example="EUROPE")
    avg_return: float
    avg_vol: float
    avg_volume: float


class SessionLiquidityResponse(BaseModel):
    symbol: str
    generated_at: datetime
    metrics: List[SessionMetric]


class CurrentSessionResponse(BaseModel):
    session: str
    regime: str
    reference_volume: float
    benchmark_volume: float
    as_of: datetime


SESSION_WINDOWS: Dict[str, range] = {
    "ASIA": range(0, 8),
    "EUROPE": range(8, 16),
    "US": range(16, 24),
}


def get_session(request: Request) -> Iterator[Session]:
    session_factory: Optional[sessionmaker] = getattr(request.app.state, "seasonality_sessionmaker", None)

    if session_factory is None:
        session_factory = SessionFactory

    if session_factory is None:
        raise RuntimeError(
            "Seasonality database session factory is not initialised. Ensure the startup event has run and the "
            "SEASONALITY_DATABASE_URI (or TIMESCALE_DATABASE_URI) environment variable is configured."
        )

    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def _ensure_timezone(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _mean(values: Sequence[float]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


def _std(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_value = sum(values) / len(values)
    variance = sum((value - mean_value) ** 2 for value in values) / len(values)
    return float(variance ** 0.5)


def _load_bars(session: Session, symbol: str) -> List[Bar]:
    query = (
        select(OhlcvBar.bucket_start, OhlcvBar.close, OhlcvBar.volume)
        .where(func.upper(OhlcvBar.market) == symbol)
        .order_by(OhlcvBar.bucket_start)
    )
    rows = session.execute(query).all()

    bars: List[Bar] = []
    for bucket_start, close, volume in rows:
        if bucket_start is None:
            continue
        timestamp = _ensure_timezone(bucket_start)
        close_value = float(close) if close is not None else None
        volume_value = float(volume) if volume is not None else 0.0
        bars.append(Bar(timestamp=timestamp, close=close_value, volume=volume_value))
    return bars


def _aggregate_by_day_of_week(bars: Sequence[Bar]) -> List[AggregatedMetric]:
    buckets: Dict[int, Dict[str, List[float]]] = {
        i: {"returns": [], "volumes": []} for i in range(7)
    }
    prev_close: float | None = None
    for bar in bars:
        day = bar.timestamp.weekday()
        buckets[day]["volumes"].append(bar.volume)
        if bar.close is None or bar.close <= 0:
            prev_close = None
            continue
        if prev_close is not None and prev_close > 0:
            buckets[day]["returns"].append(bar.close / prev_close - 1.0)
        prev_close = bar.close

    metrics: List[AggregatedMetric] = []
    for day in range(7):
        returns = buckets[day]["returns"]
        volumes = buckets[day]["volumes"]
        metrics.append(
            AggregatedMetric(
                key=calendar.day_name[day].upper(),
                avg_return=_mean(returns),
                avg_vol=_std(returns),
                avg_volume=_mean(volumes),
            )
        )
    return metrics


def _aggregate_by_hour_of_day(bars: Sequence[Bar]) -> List[AggregatedMetric]:
    buckets: Dict[int, Dict[str, List[float]]] = {
        hour: {"returns": [], "volumes": []} for hour in range(24)
    }
    prev_close: float | None = None
    for bar in bars:
        hour = bar.timestamp.hour
        buckets[hour]["volumes"].append(bar.volume)
        if bar.close is None or bar.close <= 0:
            prev_close = None
            continue
        if prev_close is not None and prev_close > 0:
            buckets[hour]["returns"].append(bar.close / prev_close - 1.0)
        prev_close = bar.close

    metrics: List[AggregatedMetric] = []
    for hour in range(24):
        returns = buckets[hour]["returns"]
        volumes = buckets[hour]["volumes"]
        metrics.append(
            AggregatedMetric(
                key=f"{hour:02d}",
                avg_return=_mean(returns),
                avg_vol=_std(returns),
                avg_volume=_mean(volumes),
            )
        )
    return metrics


def _session_for_hour(hour: int) -> str:
    for session_name, hours in SESSION_WINDOWS.items():
        if hour in hours:
            return session_name
    # Fallback for hours not covered by ranges (should not happen with defined ranges).
    return "US"


def _aggregate_by_session(bars: Sequence[Bar]) -> List[AggregatedMetric]:
    buckets: Dict[str, Dict[str, List[float]]] = {
        name: {"returns": [], "volumes": []} for name in SESSION_WINDOWS
    }
    prev_close: float | None = None
    for bar in bars:
        session_name = _session_for_hour(bar.timestamp.hour)
        buckets[session_name]["volumes"].append(bar.volume)
        if bar.close is None or bar.close <= 0:
            prev_close = None
            continue
        if prev_close is not None and prev_close > 0:
            buckets[session_name]["returns"].append(bar.close / prev_close - 1.0)
        prev_close = bar.close

    metrics: List[AggregatedMetric] = []
    for session_name in ("ASIA", "EUROPE", "US"):
        returns = buckets[session_name]["returns"]
        volumes = buckets[session_name]["volumes"]
        metrics.append(
            AggregatedMetric(
                key=session_name,
                avg_return=_mean(returns),
                avg_vol=_std(returns),
                avg_volume=_mean(volumes),
            )
        )
    return metrics


def _persist_metric(
    session: Session,
    *,
    symbol: str,
    period: str,
    avg_return: float,
    avg_vol: float,
    avg_volume: float,
    ts: datetime,
) -> None:
    record = session.get(SeasonalityMetric, {"symbol": symbol, "period": period})
    if record is None:
        record = SeasonalityMetric(
            symbol=symbol,
            period=period,
            avg_return=avg_return,
            avg_vol=avg_vol,
            avg_volume=avg_volume,
            ts=ts,
        )
        session.add(record)
    else:
        record.avg_return = avg_return
        record.avg_vol = avg_vol
        record.avg_volume = avg_volume
        record.ts = ts


def _validate_symbol(symbol: str) -> str:
    symbol_key = symbol.strip().upper()
    if not symbol_key:
        raise HTTPException(status_code=422, detail="Symbol must be provided")
    return symbol_key


def _assert_data_available(bars: Sequence[Bar], symbol: str) -> None:
    closes = [bar.close for bar in bars if bar.close is not None]
    if len(closes) < 2:
        raise HTTPException(
            status_code=404,
            detail=f"Insufficient OHLCV history for symbol={symbol}",
        )


app = FastAPI(title="Seasonality Analytics Service")
app.state.seasonality_database_url = None
app.state.seasonality_engine = None
app.state.seasonality_sessionmaker = None


@app.on_event("startup")
def _configure_database() -> None:
    """Initialise database connectivity once the application starts."""

    global DATABASE_URL, ENGINE, SessionFactory

    url = _require_database_url()
    engine = _create_engine(url)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    SeasonalityMetric.__table__.create(bind=engine, checkfirst=True)

    DATABASE_URL = url
    ENGINE = engine
    SessionFactory = session_factory

    app.state.seasonality_database_url = url
    app.state.seasonality_engine = engine
    app.state.seasonality_sessionmaker = session_factory


def _resolve_session_store_dsn() -> str:
    for env_var in ("SESSION_REDIS_URL", "SESSION_STORE_URL", "SESSION_BACKEND_DSN"):
        value = os.getenv(env_var)
        if value:
            return value
    raise RuntimeError(
        "Session store misconfigured: set SESSION_REDIS_URL, SESSION_STORE_URL, or SESSION_BACKEND_DSN "
        "so the seasonality service can validate administrator tokens."
    )


def _configure_session_store(application: FastAPI) -> SessionStoreProtocol:
    existing = getattr(application.state, "session_store", None)
    if isinstance(existing, SessionStoreProtocol):
        store = existing
    else:
        dsn = _resolve_session_store_dsn()
        ttl_minutes = int(os.getenv("SESSION_TTL_MINUTES", "60"))
        if dsn.startswith("memory://"):
            store = InMemorySessionStore(ttl_minutes=ttl_minutes)
        else:
            try:  # pragma: no cover - optional dependency for production deployments
                import redis  # type: ignore[import-not-found]
            except ImportError as exc:  # pragma: no cover - surfaced when redis missing locally
                raise RuntimeError(
                    "redis package is required when SESSION_REDIS_URL is configured for the seasonality service."
                ) from exc

            client = redis.Redis.from_url(dsn)
            store = RedisSessionStore(client, ttl_minutes=ttl_minutes)

        application.state.session_store = store

    security.set_default_session_store(store)
    return store


SESSION_STORE: SessionStoreProtocol | None = None


@app.on_event("startup")
def _initialise_session_store() -> None:
    """Initialise the session backend once the FastAPI app starts."""

    global SESSION_STORE
    SESSION_STORE = _configure_session_store(app)


@app.get("/seasonality/dayofweek", response_model=DayOfWeekResponse)
def day_of_week(
    *,
    symbol: str = Query(..., description="Market symbol", example="BTC/USD"),
    session: Session = Depends(get_session),
    _caller: str = Depends(require_admin_account),
) -> DayOfWeekResponse:
    symbol_key = _validate_symbol(symbol)
    bars = _load_bars(session, symbol_key)
    _assert_data_available(bars, symbol_key)

    metrics = _aggregate_by_day_of_week(bars)
    now = datetime.now(timezone.utc)

    for metric in metrics:
        period_key = f"dayofweek:{metric.key}"
        _persist_metric(
            session,
            symbol=symbol_key,
            period=period_key,
            avg_return=metric.avg_return,
            avg_vol=metric.avg_vol,
            avg_volume=metric.avg_volume,
            ts=now,
        )
    session.commit()

    response_metrics = [
        DayOfWeekMetric(
            weekday=metric.key.capitalize(),
            avg_return=metric.avg_return,
            avg_vol=metric.avg_vol,
            avg_volume=metric.avg_volume,
        )
        for metric in metrics
    ]
    return DayOfWeekResponse(symbol=symbol_key, generated_at=now, metrics=response_metrics)


@app.get("/seasonality/hourofday", response_model=HourOfDayResponse)
def hour_of_day(
    *,
    symbol: str = Query(..., description="Market symbol", example="BTC/USD"),
    session: Session = Depends(get_session),
    _caller: str = Depends(require_admin_account),
) -> HourOfDayResponse:
    symbol_key = _validate_symbol(symbol)
    bars = _load_bars(session, symbol_key)
    _assert_data_available(bars, symbol_key)

    metrics = _aggregate_by_hour_of_day(bars)
    now = datetime.now(timezone.utc)

    for metric in metrics:
        period_key = f"hourofday:{metric.key}"
        _persist_metric(
            session,
            symbol=symbol_key,
            period=period_key,
            avg_return=metric.avg_return,
            avg_vol=metric.avg_vol,
            avg_volume=metric.avg_volume,
            ts=now,
        )
    session.commit()

    response_metrics = [
        HourOfDayMetric(
            hour=int(metric.key),
            avg_return=metric.avg_return,
            avg_vol=metric.avg_vol,
            avg_volume=metric.avg_volume,
        )
        for metric in metrics
    ]
    return HourOfDayResponse(symbol=symbol_key, generated_at=now, metrics=response_metrics)


@app.get("/seasonality/session_liquidity", response_model=SessionLiquidityResponse)
def session_liquidity(
    *,
    symbol: str = Query(..., description="Market symbol", example="BTC/USD"),
    session: Session = Depends(get_session),
    _caller: str = Depends(require_admin_account),
) -> SessionLiquidityResponse:
    symbol_key = _validate_symbol(symbol)
    bars = _load_bars(session, symbol_key)
    _assert_data_available(bars, symbol_key)

    metrics = _aggregate_by_session(bars)
    now = datetime.now(timezone.utc)

    for metric in metrics:
        period_key = f"session:{metric.key}"
        _persist_metric(
            session,
            symbol=symbol_key,
            period=period_key,
            avg_return=metric.avg_return,
            avg_vol=metric.avg_vol,
            avg_volume=metric.avg_volume,
            ts=now,
        )
    session.commit()

    response_metrics = [
        SessionMetric(
            session=metric.key,
            avg_return=metric.avg_return,
            avg_vol=metric.avg_vol,
            avg_volume=metric.avg_volume,
        )
        for metric in metrics
    ]
    return SessionLiquidityResponse(symbol=symbol_key, generated_at=now, metrics=response_metrics)


@app.get("/seasonality/current_session", response_model=CurrentSessionResponse)
def current_session(
    *,
    session: Session = Depends(get_session),
    _caller: str = Depends(require_admin_account),
) -> CurrentSessionResponse:
    now = datetime.now(timezone.utc)
    session_name = _session_for_hour(now.hour)

    session_metrics = (
        session.execute(
            select(SeasonalityMetric).where(SeasonalityMetric.period == f"session:{session_name}")
        )
        .scalars()
        .all()
    )
    if not session_metrics:
        raise HTTPException(status_code=404, detail="No session liquidity metrics available")

    reference_volume = _mean([metric.avg_volume for metric in session_metrics])

    all_session_metrics = (
        session.execute(
            select(SeasonalityMetric).where(SeasonalityMetric.period.like("session:%"))
        )
        .scalars()
        .all()
    )
    if all_session_metrics:
        benchmark_volume = _mean([metric.avg_volume for metric in all_session_metrics])
    else:
        benchmark_volume = reference_volume

    regime: str
    if benchmark_volume <= 0:
        regime = "normal"
    else:
        ratio = reference_volume / benchmark_volume
        if ratio >= 1.2:
            regime = "high"
        elif ratio <= 0.8:
            regime = "low"
        else:
            regime = "normal"

    return CurrentSessionResponse(
        session=session_name,
        regime=regime,
        reference_volume=reference_volume,
        benchmark_volume=benchmark_volume,
        as_of=now,
    )
