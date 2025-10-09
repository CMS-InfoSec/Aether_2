"""Cross-asset analytics FastAPI service with strict typing support."""
from __future__ import annotations

import logging
import math
import os
import statistics
import sys
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional, Sequence, TypeVar, cast

from prometheus_client import Gauge

try:  # pragma: no cover - metrics helper optional when FastAPI unavailable
    from metrics import setup_metrics
except ModuleNotFoundError:  # pragma: no cover - fallback stub for optional dependency
    def setup_metrics(*_: Any, **__: Any) -> None:
        return None
from sqlalchemy import Column, DateTime, Float, String, create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from services.common.spot import require_spot_http
from shared.postgres import normalize_sqlalchemy_dsn
from shared.pydantic_compat import BaseModel, Field

if TYPE_CHECKING:  # pragma: no cover - FastAPI is optional for type checking
    from fastapi import FastAPI, HTTPException, Query
else:  # pragma: no cover - runtime fallbacks when FastAPI is not installed
    try:
        from fastapi import FastAPI, HTTPException, Query
    except ModuleNotFoundError:  # pragma: no cover - minimal shim for optional dependency
        class HTTPException(Exception):
            """Lightweight HTTP exception mirroring FastAPI semantics."""

            def __init__(self, status_code: int, detail: str) -> None:
                super().__init__(detail)
                self.status_code = status_code
                self.detail = detail

        class _State:  # pragma: no cover - generic container for FastAPI state
            pass

        class FastAPI:  # pragma: no cover - decorator-friendly shim
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                del args, kwargs
                self.state = _State()

            def get(self, *args: Any, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
                def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                    return func

                return decorator

            def on_event(self, *_: Any, **__: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
                def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                    return func

                return decorator

        def Query(default: Any = None, **_: Any) -> Any:  # pragma: no cover - simple passthrough
            return default


LOGGER = logging.getLogger(__name__)

TCallable = TypeVar("TCallable", bound=Callable[..., Any])


def typed_app_event(application: FastAPI, event: str) -> Callable[[TCallable], TCallable]:
    """Wrap ``FastAPI.on_event`` to preserve typing for decorated callables."""

    def decorator(func: TCallable) -> TCallable:
        wrapped = application.on_event(event)(func)
        return cast(TCallable, wrapped)

    return decorator


def typed_app_get(application: FastAPI, path: str, **kwargs: Any) -> Callable[[TCallable], TCallable]:
    """Wrap ``FastAPI.get`` to retain endpoint typing under strict mypy."""

    def decorator(func: TCallable) -> TCallable:
        wrapped = application.get(path, **kwargs)(func)
        return cast(TCallable, wrapped)

    return decorator


if TYPE_CHECKING:  # pragma: no cover - typed declarative base for mypy
    class _DeclarativeBase:
        metadata: Any
        registry: Any

    DeclarativeBase = _DeclarativeBase
else:  # pragma: no cover - runtime declarative base when SQLAlchemy is installed
    DeclarativeBase = declarative_base()


ENGINE_STATE_KEY = "crossasset_engine"
SESSIONMAKER_STATE_KEY = "crossasset_sessionmaker"
DATABASE_URL_STATE_KEY = "crossasset_database_url"

DATABASE_URL: Optional[str] = None
ENGINE: Engine | None = None
SessionFactory = Callable[[], Session]
SESSION_FACTORY: SessionFactory | None = None

DATA_STALENESS_GAUGE = Gauge(
    "crossasset_data_age_seconds",
    "Age of the latest OHLCV bar used for cross-asset analytics.",
    ["symbol"],
)

DEFAULT_WINDOW_POINTS = 240
MIN_REQUIRED_POINTS = 30
STALE_THRESHOLD = timedelta(hours=6)
STABLECOIN_STALE_THRESHOLD = timedelta(hours=12)
_SQLITE_FALLBACK = "sqlite+pysqlite:///:memory:"


if TYPE_CHECKING:
    Base = DeclarativeBase
else:
    Base = DeclarativeBase


class CrossAssetMetric(Base):
    """SQLAlchemy model storing computed cross-asset analytics."""

    __tablename__ = "crossasset_metrics"

    if TYPE_CHECKING:  # pragma: no cover - inform mypy of constructor/table fields
        __table__: Any

        def __init__(
            self,
            *,
            pair: str,
            metric_type: str,
            ts: datetime,
            value: float,
        ) -> None: ...

    pair = Column(String, primary_key=True)
    metric_type = Column(String, primary_key=True)
    ts = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float, nullable=False)


class OhlcvBar(Base):
    """Subset of the ``ohlcv_bars`` table used for analytics."""

    __tablename__ = "ohlcv_bars"

    if TYPE_CHECKING:  # pragma: no cover - enhanced constructor for static analysis
        __table__: Any

        def __init__(
            self,
            *,
            market: str,
            bucket_start: datetime,
            open: float,
            high: float,
            low: float,
            close: float,
            volume: float,
        ) -> None: ...

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


app = FastAPI(title="Cross-Asset Analytics Service", version="1.0.0")
setup_metrics(app, service_name="crossasset-analytics-service")
setattr(app.state, DATABASE_URL_STATE_KEY, None)
setattr(app.state, ENGINE_STATE_KEY, None)
setattr(app.state, SESSIONMAKER_STATE_KEY, None)


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

    normalized = normalize_sqlalchemy_dsn(
        candidate,
        allow_sqlite=allow_sqlite,
        label="Cross-asset analytics database DSN",
    )
    return cast(str, normalized)


def _engine_options(url: str) -> dict[str, object]:
    options: dict[str, object] = {"future": True}
    if url.startswith("sqlite://") or url.startswith("sqlite+pysqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


def _create_engine(url: str) -> Engine:
    return create_engine(url, **_engine_options(url))


def _register_database(url: str, engine: Engine, session_factory: SessionFactory) -> SessionFactory:
    """Persist database artefacts on the module and FastAPI state."""

    global DATABASE_URL, ENGINE, SESSION_FACTORY

    DATABASE_URL = url
    ENGINE = engine
    SESSION_FACTORY = session_factory

    setattr(app.state, DATABASE_URL_STATE_KEY, url)
    setattr(app.state, ENGINE_STATE_KEY, engine)
    setattr(app.state, SESSIONMAKER_STATE_KEY, session_factory)

    return session_factory


def _ensure_session_factory() -> SessionFactory:
    session_factory = SESSION_FACTORY
    if session_factory is None:
        raise RuntimeError(
            "Cross-asset analytics session factory is not initialised. Ensure startup has run and "
            "CROSSASSET_DATABASE_URL (or ANALYTICS_DATABASE_URL) is configured."
        )
    return session_factory


@typed_app_event(app, "startup")
def _on_startup() -> None:
    """Initialise the Timescale connection and ensure tables exist."""

    url = _resolve_database_url()
    engine = _create_engine(url)
    session_factory = cast(
        SessionFactory,
        sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True),
    )

    try:
        Base.metadata.create_all(engine)
    except SQLAlchemyError as exc:  # pragma: no cover - defensive logging
        LOGGER.exception("Failed to initialise crossasset tables: %s", exc)
        raise

    _register_database(url, engine, session_factory)


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
    candidates = {normalised, normalised.replace("-", "/")}
    stmt = (
        select(OhlcvBar.bucket_start, OhlcvBar.close)
        .where(func.upper(OhlcvBar.market).in_(candidates))
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
    if observed_at is None:
        detail = f"No price history found for {symbol}"
        LOGGER.warning(detail)
        raise HTTPException(status_code=503, detail=detail)

    age_seconds = (datetime.now(tz=timezone.utc) - observed_at).total_seconds()
    DATA_STALENESS_GAUGE.labels(symbol=symbol).set(age_seconds)

    if age_seconds > max_age.total_seconds():
        detail = f"Price history for {symbol} is stale ({int(age_seconds)}s old)"
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
    session_factory = _ensure_session_factory()
    with session_factory() as session:
        _store_metrics(session, pair, metrics, ts)
        session.commit()


@typed_app_get(app, "/crossasset/leadlag", response_model=LeadLagResponse)
def lead_lag(
    base: str = Query(..., description="Base asset symbol"),
    target: str = Query(..., description="Target asset symbol"),
) -> LeadLagResponse:
    """Return correlation and lag coefficient for the provided pair."""

    base_symbol = require_spot_http(base, param="base", logger=LOGGER)
    target_symbol = require_spot_http(target, param="target", logger=LOGGER)

    session_factory = _ensure_session_factory()
    with session_factory() as session:
        base_series, base_ts = _load_price_series(session, base_symbol, window=DEFAULT_WINDOW_POINTS)
        target_series, target_ts = _load_price_series(session, target_symbol, window=DEFAULT_WINDOW_POINTS)

    _record_data_age(base_symbol, base_ts, max_age=STALE_THRESHOLD)
    _record_data_age(target_symbol, target_ts, max_age=STALE_THRESHOLD)
    _require_series(base_series, base_symbol, MIN_REQUIRED_POINTS)
    _require_series(target_series, target_symbol, MIN_REQUIRED_POINTS)

    base_series_aligned, target_series_aligned = _align_series(base_series, target_series)

    correlation = _pearson_correlation(base_series_aligned, target_series_aligned)
    lag = _lag_coefficient(base_series_aligned, target_series_aligned)
    ts = datetime.now(tz=timezone.utc)
    pair = f"{base_symbol}/{target_symbol}"

    _persist_metrics(pair, (("leadlag_correlation", correlation), ("leadlag_lag", float(lag))), ts)

    return LeadLagResponse(pair=pair, correlation=correlation, lag=lag, ts=ts)


@typed_app_get(app, "/crossasset/beta", response_model=BetaResponse)
def rolling_beta(
    alt: str = Query(..., description="Alt asset symbol"),
    base: str = Query(..., description="Base asset symbol"),
    window: int = Query(60, ge=10, le=240, description="Rolling window size"),
) -> BetaResponse:
    """Return a rolling beta estimate for the provided alt/base pair."""

    alt_symbol = require_spot_http(alt, param="alt", logger=LOGGER)
    base_symbol = require_spot_http(base, param="base", logger=LOGGER)
    lookback = max(DEFAULT_WINDOW_POINTS, window * 3)

    session_factory = _ensure_session_factory()
    with session_factory() as session:
        alt_series, alt_ts = _load_price_series(session, alt_symbol, window=lookback)
        base_series, base_ts = _load_price_series(session, base_symbol, window=lookback)

    _record_data_age(alt_symbol, alt_ts, max_age=STALE_THRESHOLD)
    _record_data_age(base_symbol, base_ts, max_age=STALE_THRESHOLD)
    _require_series(alt_series, alt_symbol, window)
    _require_series(base_series, base_symbol, window)

    alt_series_aligned, base_series_aligned = _align_series(alt_series, base_series)
    beta_value = _rolling_beta(alt_series_aligned, base_series_aligned, window=window)
    ts = datetime.now(tz=timezone.utc)
    pair = f"{alt_symbol}/{base_symbol}"

    _persist_metrics(pair, (("rolling_beta", beta_value),), ts)

    return BetaResponse(pair=pair, beta=beta_value, ts=ts)


@typed_app_get(app, "/crossasset/stablecoin", response_model=StablecoinResponse)
def stablecoin_deviation(
    symbol: str = Query(..., description="Stablecoin market symbol (e.g. USDT/USD)"),
) -> StablecoinResponse:
    """Return deviation of the stablecoin price from the 1.0 USD peg."""

    normalized = require_spot_http(symbol, logger=LOGGER)

    session_factory = _ensure_session_factory()
    with session_factory() as session:
        series, observed_ts = _load_price_series(session, normalized, window=DEFAULT_WINDOW_POINTS)

    _record_data_age(normalized, observed_ts, max_age=STABLECOIN_STALE_THRESHOLD)
    _require_series(series, normalized, 1)

    price = float(series[-1])
    deviation = price - 1.0
    deviation_bps = deviation * 10000
    ts = datetime.now(tz=timezone.utc)

    _persist_metrics(normalized, (("stablecoin_deviation", deviation),), ts)

    if abs(deviation) > 0.005:
        LOGGER.warning(
            "Stablecoin peg deviation detected: %s deviated by %.4f (%.2f bps)",
            normalized,
            deviation,
            deviation_bps,
        )

    return StablecoinResponse(
        symbol=normalized,
        price=price,
        deviation=deviation,
        deviation_bps=deviation_bps,
        ts=ts,
    )


__all__ = [
    "app",
    "DATABASE_URL",
    "ENGINE",
    "SESSION_FACTORY",
    "CrossAssetMetric",
    "LeadLagResponse",
    "BetaResponse",
    "StablecoinResponse",
]
