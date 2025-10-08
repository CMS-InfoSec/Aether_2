"""FastAPI service exposing volatility analytics for traded symbols."""
from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterator, List, Optional, Sequence, TypeVar, cast

from alembic import command
from alembic.config import Config
from prometheus_client import Gauge
from sqlalchemy import Column, DateTime, Float, String, create_engine, func, select
from sqlalchemy.engine import Engine, URL
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

import metrics
from services.common.spot import require_spot_http
from shared.pydantic_compat import BaseModel, Field

if TYPE_CHECKING:  # pragma: no cover - FastAPI is optional in some environments
    from fastapi import Depends, FastAPI, HTTPException, Query, Request
    from starlette import status
else:  # pragma: no cover - lightweight fallbacks when FastAPI is unavailable
    try:
        from fastapi import Depends, FastAPI, HTTPException, Query, Request
        from starlette import status
    except ModuleNotFoundError:  # pragma: no cover - minimal shims for optional deps
        class HTTPException(Exception):
            """Lightweight HTTP exception carrying status and detail."""

            def __init__(self, status_code: int, detail: str) -> None:
                super().__init__(detail)
                self.status_code = status_code
                self.detail = detail

        class _State:  # pragma: no cover - simple container for stateful attrs
            pass

        class FastAPI:  # pragma: no cover - decorator-friendly application shim
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                del args, kwargs
                self.state = _State()

            def get(
                self, *args: Any, **kwargs: Any
            ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
                def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                    return func

                return decorator

            def on_event(
                self, *_: Any, **__: Any
            ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
                def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                    return func

                return decorator

        class Request:  # pragma: no cover - exposes ``app`` and ``state`` like FastAPI
            def __init__(self) -> None:
                self.app = FastAPI()
                self.state = self.app.state

        def Depends(dependency: Callable[..., Any]) -> Callable[..., Any]:  # type: ignore[misc]
            return dependency

        def Query(default: Any = None, **_: Any) -> Any:
            return default

        class status:  # pragma: no cover - subset of Starlette status codes
            HTTP_404_NOT_FOUND = 404


TCallable = TypeVar("TCallable", bound=Callable[..., Any])


def typed_app_event(
    application: FastAPI, event: str
) -> Callable[[TCallable], TCallable]:
    """Wrap ``FastAPI.on_event`` so decorated callables retain their type."""

    def decorator(func: TCallable) -> TCallable:
        wrapped = application.on_event(event)(func)
        return cast(TCallable, wrapped)

    return decorator


def typed_app_get(
    application: FastAPI, path: str, **kwargs: Any
) -> Callable[[TCallable], TCallable]:
    """Wrap ``FastAPI.get`` to preserve typing for decorated endpoints."""

    def decorator(func: TCallable) -> TCallable:
        wrapped = application.get(path, **kwargs)(func)
        return cast(TCallable, wrapped)

    return decorator


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

if TYPE_CHECKING:
    class _DeclarativeBase:
        """Typed stub mirroring the SQLAlchemy declarative base."""

        metadata: Any
        registry: Any

    DeclarativeBase = _DeclarativeBase
else:  # pragma: no cover - runtime base when SQLAlchemy is installed
    DeclarativeBase = declarative_base()


DATABASE_URL: URL | None = None
ENGINE: Engine | None = None

SessionFactory = Callable[[], Session]
SESSION_FACTORY: SessionFactory | None = None

ENGINE_STATE_KEY = "volatility_engine"
SESSIONMAKER_STATE_KEY = "volatility_sessionmaker"
DATABASE_URL_STATE_KEY = "volatility_database_url"


def _create_engine(url: URL) -> Engine:
    return create_engine(
        url.render_as_string(hide_password=False),
        **_engine_options(url),
    )


_MIGRATIONS_PATH = Path(__file__).resolve().parents[2] / "data" / "migrations"

def run_migrations(url: URL) -> None:
    """Apply all outstanding Timescale migrations for analytics data."""

    config = Config()
    config.set_main_option("script_location", str(_MIGRATIONS_PATH))
    config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False))
    config.attributes["configure_logger"] = False

    command.upgrade(config, "head")


if TYPE_CHECKING:
    Base = DeclarativeBase
else:
    Base = DeclarativeBase


class OhlcvBar(Base):
    """Representation of the ``ohlcv_bars`` table."""

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
    close = Column(Float)
    volume = Column(Float)


class VolatilityMetric(Base):
    """Persistence model for computed volatility metrics."""

    __tablename__ = "vol_metrics"

    if TYPE_CHECKING:  # pragma: no cover - inform mypy of constructor/table fields
        __table__: Any

        def __init__(
            self,
            *,
            symbol: str,
            realized_vol: float,
            garch_vol: float,
            jump_prob: float,
            atr: float,
            band_width: float,
            ts: datetime,
        ) -> None: ...

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
setattr(app.state, DATABASE_URL_STATE_KEY, None)
setattr(app.state, ENGINE_STATE_KEY, None)
setattr(app.state, SESSIONMAKER_STATE_KEY, None)
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


def _initialise_database() -> SessionFactory:
    """Initialise database connectivity once configuration is available."""

    url = _require_database_url()
    engine = _create_engine(url)
    session_factory = cast(
        SessionFactory,
        sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True),
    )

    return _register_database(url, engine, session_factory)


def _register_database(
    url: URL, engine: Engine, session_factory: SessionFactory
) -> SessionFactory:
    """Store database artefacts for reuse across FastAPI requests."""

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
            "Analytics database session factory is not initialised. Ensure startup has completed and "
            "ANALYTICS_DATABASE_URL is configured."
        )
    return session_factory


@typed_app_event(app, "startup")
def _on_startup() -> None:

    """Initialise database connectivity and migrations when the app boots."""

    url = _require_database_url()
    engine = _create_engine(url)
    session_factory = cast(
        SessionFactory,
        sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True),
    )

    VolatilityMetric.__table__.create(bind=engine, checkfirst=True)
    run_migrations(url)

    _register_database(url, engine, session_factory)


def get_session(request: Request) -> Iterator[Session]:
    session_factory = cast(
        Optional[SessionFactory],
        getattr(request.app.state, SESSIONMAKER_STATE_KEY, None),
    )

    if session_factory is None:
        session_factory = SESSION_FACTORY

    if session_factory is None:
        session_factory = _ensure_session_factory()

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


@typed_app_get(app, "/volatility/realized", response_model=VolatilityResponse)
def realized_volatility(
    *,
    symbol: str = Query(..., description="Instrument symbol", min_length=1),
    window: int = Query(30, ge=5, le=500, description="Number of bars to evaluate"),
    session: Session = Depends(get_session),
) -> VolatilityResponse:
    return _evaluate(session, symbol, window)


@typed_app_get(app, "/volatility/garch", response_model=VolatilityResponse)
def garch_volatility(
    *,
    symbol: str = Query(..., description="Instrument symbol", min_length=1),
    window: int = Query(30, ge=5, le=500, description="Number of bars to evaluate"),
    session: Session = Depends(get_session),
) -> VolatilityResponse:
    return _evaluate(session, symbol, window)


@typed_app_get(app, "/volatility/jump_test", response_model=VolatilityResponse)
def jump_test(
    *,
    symbol: str = Query(..., description="Instrument symbol", min_length=1),
    window: int = Query(30, ge=5, le=500, description="Number of bars to evaluate"),
    session: Session = Depends(get_session),
) -> VolatilityResponse:
    return _evaluate(session, symbol, window)
