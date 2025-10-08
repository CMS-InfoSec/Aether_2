"""FastAPI service exposing historical seasonality analytics for Kraken symbols."""

from __future__ import annotations

import calendar
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Sequence, TypeVar, cast

if TYPE_CHECKING:  # pragma: no cover - imported for static analysis only
    from fastapi import Depends, FastAPI, HTTPException, Query, Request
else:  # pragma: no cover - runtime fallback when FastAPI is optional
    try:
        from fastapi import Depends, FastAPI, HTTPException, Query, Request
    except ModuleNotFoundError:  # pragma: no cover - minimal stub for optional dependency
        class HTTPException(Exception):
            """Lightweight HTTP exception capturing status and detail."""

            def __init__(self, status_code: int, detail: str) -> None:
                super().__init__(detail)
                self.status_code = status_code
                self.detail = detail

        class _State:  # pragma: no cover - generic container for state attributes
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

        class Request:  # pragma: no cover - simple stub exposing ``app`` and ``state``
            def __init__(self) -> None:
                self.app = FastAPI()
                self.state = self.app.state

        def Depends(dependency: Callable[..., Any]) -> Callable[..., Any]:  # type: ignore[misc]
            return dependency

        def Query(default: Any = None, **_: Any) -> Any:
            return default

from sqlalchemy import Column, DateTime, Float, String, create_engine, func, select
from sqlalchemy.engine import Engine, URL
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from auth.service import (
    InMemorySessionStore,
    SessionStoreProtocol,
    build_session_store_from_url,
)
from services.common import security
from services.common.security import require_admin_account
from services.common.spot import require_spot_http
from shared.session_config import load_session_ttl_minutes
from shared.pydantic_compat import BaseModel, Field

__all__ = ["app", "ENGINE", "SessionLocal", "SESSION_STORE"]


ENGINE_STATE_KEY = "seasonality_engine"
SESSIONMAKER_STATE_KEY = "seasonality_sessionmaker"

ENGINE: Engine | None = None
SessionFactory = Callable[[], Session]
SessionLocal: SessionFactory | None = None
SESSION_STORE: SessionStoreProtocol | None = None


def _normalise_database_url(url: str) -> str:
    """Ensure SQLAlchemy uses the psycopg2 driver for PostgreSQL URLs."""

    if url.startswith("postgresql+psycopg://"):
        return "postgresql+psycopg2://" + url[len("postgresql+psycopg://") :]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg2://" + url[len("postgresql://") :]
    if url.startswith("postgres://"):
        return "postgresql+psycopg2://" + url[len("postgres://") :]
    return url


def _require_database_url() -> str:
    """Resolve and validate the managed Timescale/PostgreSQL DSN."""

    primary = os.getenv("SEASONALITY_DATABASE_URI")
    fallback = os.getenv("TIMESCALE_DATABASE_URI") or os.getenv("DATABASE_URL")
    raw_url = primary or fallback

    if not raw_url:
        raise RuntimeError(
            "Seasonality service requires SEASONALITY_DATABASE_URI (or TIMESCALE_DATABASE_URI) "
            "to point at a managed Timescale/PostgreSQL database."
        )

    normalised = _normalise_database_url(raw_url.strip())
    allow_sqlite = "pytest" in sys.modules

    if _SQLALCHEMY_AVAILABLE:
        try:
            url_obj = make_url(normalised)
        except ArgumentError as exc:  # pragma: no cover - configuration error
            raise RuntimeError(f"Invalid seasonality database URL '{raw_url}': {exc}") from exc

        driver = url_obj.drivername.lower()
        if driver.startswith("postgresql"):
            return url_obj.render_as_string(hide_password=False)
        if allow_sqlite and driver.startswith("sqlite"):
            return url_obj.render_as_string(hide_password=False)
        raise RuntimeError(
            "Seasonality service requires a PostgreSQL/Timescale DSN (SQLite is permitted for tests); "
            f"received driver '{url_obj.drivername}'."
        )

    driver = normalised.split("://", 1)[0].lower()
    if driver.startswith("postgresql") or driver.startswith("postgres"):
        return normalised
    if driver.startswith("sqlite"):
        return normalised
    raise RuntimeError(
        "Seasonality service requires a PostgreSQL/Timescale DSN (SQLite is permitted for tests)."
    )


def _engine_options(url: str) -> Dict[str, Any]:
    if not _SQLALCHEMY_AVAILABLE:
        return {}

    url_obj = make_url(url)
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
    elif url_obj.drivername.lower().startswith("postgresql"):
        if "sslmode" not in url_obj.query and url_obj.host not in {None, "localhost", "127.0.0.1"}:
            connect_args["sslmode"] = "require"

    if connect_args:
        options["connect_args"] = connect_args

    return options


def _create_engine(database_url: str) -> Engine | _InMemoryEngine:
    if _SQLALCHEMY_AVAILABLE:
        return create_engine(database_url, **_engine_options(database_url))
    return _InMemoryEngine(database_url)

if _SQLALCHEMY_AVAILABLE:
    OhlcvBase = declarative_base()
    MetricsBase = declarative_base()

    class OhlcvBar(OhlcvBase):
        """Minimal OHLCV representation backed by the historical bars table."""

if TYPE_CHECKING:
    class _DeclarativeBase:
        """Typed stub mirroring SQLAlchemy's declarative base attributes."""

        metadata: Any
        registry: Any


    OhlcvBase = _DeclarativeBase
    MetricsBase = _DeclarativeBase
else:  # pragma: no cover - runtime declarative bases when SQLAlchemy is available
    OhlcvBase = declarative_base()
    MetricsBase = declarative_base()

if TYPE_CHECKING:
    class _DeclarativeBase:
        """Typed stub mirroring SQLAlchemy's declarative base attributes."""

        metadata: Any
        registry: Any


    OhlcvBase = _DeclarativeBase
    MetricsBase = _DeclarativeBase
else:  # pragma: no cover - runtime declarative bases when SQLAlchemy is available
    OhlcvBase = declarative_base()
    MetricsBase = declarative_base()

    class SeasonalityMetric(MetricsBase):
        """Persisted aggregate used to snapshot historical seasonality results."""

        __tablename__ = "seasonality_metrics"

        symbol = Column(String, primary_key=True)
        period = Column(String, primary_key=True)
        avg_return = Column(Float, nullable=False)
        avg_vol = Column(Float, nullable=False)
        avg_volume = Column(Float, nullable=False)
        ts = Column(DateTime(timezone=True), nullable=False)
else:
    class _InMemoryBase:
        metadata = SimpleNamespace(create_all=lambda **_: None)

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

    @dataclass
    class OhlcvBar:  # type: ignore[override]
        """Minimal OHLCV representation used when SQLAlchemy is unavailable."""

        market: str
        bucket_start: datetime
        open: float | None = None
        high: float | None = None
        low: float | None = None
        close: float | None = None
        volume: float | None = None

    @dataclass
    class SeasonalityMetric:  # type: ignore[override]
        """In-memory representation of computed seasonality metrics."""

    if TYPE_CHECKING:  # pragma: no cover - enhanced constructor for static analysis
        __table__: Any

        def __init__(
            self,
            *,
            symbol: str,
            period: str,
            avg_return: float,
            avg_vol: float,
            avg_volume: float,
            ts: datetime,
        ) -> None: ...

    if TYPE_CHECKING:  # pragma: no cover - enhanced constructor for static analysis
        __table__: Any

        def __init__(
            self,
            *,
            symbol: str,
            period: str,
            avg_return: float,
            avg_vol: float,
            avg_volume: float,
            ts: datetime,
        ) -> None: ...

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


class _InMemorySeasonalityStore:
    """Persist OHLCV bars and computed metrics without SQLAlchemy."""

    def __init__(self) -> None:
        self._bars: List[OhlcvBar] = []
        self._metrics: Dict[tuple[str, str], SeasonalityMetric] = {}
        self._lock = RLock()

    def add_bar(self, bar: OhlcvBar) -> None:
        with self._lock:
            self._bars.append(bar)
            self._bars.sort(key=lambda item: getattr(item, "bucket_start", datetime.min))

    def load_bars(self, symbol: str) -> List[OhlcvBar]:
        key = _symbol_key(symbol)
        with self._lock:
            return [
                bar
                for bar in self._bars
                if _symbol_key(getattr(bar, "market", "")) == key
            ]

    def add_metric_object(self, metric: SeasonalityMetric) -> None:
        key = (metric.symbol.upper(), metric.period)
        with self._lock:
            self._metrics[key] = metric

    def get_metric(self, symbol: str, period: str) -> Optional[SeasonalityMetric]:
        key = (symbol.upper(), period)
        with self._lock:
            return self._metrics.get(key)

    def upsert_metric(
        self,
        *,
        symbol: str,
        period: str,
        avg_return: float,
        avg_vol: float,
        avg_volume: float,
        ts: datetime,
    ) -> SeasonalityMetric:
        key = (symbol.upper(), period)
        with self._lock:
            record = self._metrics.get(key)
            if record is None:
                record = SeasonalityMetric(
                    symbol=symbol,
                    period=period,
                    avg_return=avg_return,
                    avg_vol=avg_vol,
                    avg_volume=avg_volume,
                    ts=ts,
                )
                self._metrics[key] = record
            else:
                record.avg_return = avg_return
                record.avg_vol = avg_vol
                record.avg_volume = avg_volume
                record.ts = ts
            return record

    def metrics_by_period(
        self, *, period: Optional[str] = None, prefix: Optional[str] = None
    ) -> List[SeasonalityMetric]:
        with self._lock:
            records = list(self._metrics.values())
        if period is not None:
            records = [record for record in records if record.period == period]
        if prefix is not None:
            records = [record for record in records if record.period.startswith(prefix)]
        records.sort(key=lambda record: (record.symbol, record.period, record.ts))
        return records

    def reset(self) -> None:
        with self._lock:
            self._bars.clear()
            self._metrics.clear()


class _InMemorySession:
    """Lightweight session that persists data via ``_InMemorySeasonalityStore``."""

    def __init__(self, store: _InMemorySeasonalityStore) -> None:
        self._store = store
        self._pending_bars: List[OhlcvBar] = []
        self._pending_metrics: List[SeasonalityMetric] = []
        self._closed = False

    def __enter__(self) -> "_InMemorySession":
        return self

    def __exit__(
        self,
        _exc_type: Optional[type[BaseException]],
        _exc: Optional[BaseException],
        _tb: Optional[Any],
    ) -> None:
        self.close()

    def add(self, obj: Any) -> None:
        if isinstance(obj, OhlcvBar):
            self._pending_bars.append(obj)
        elif isinstance(obj, SeasonalityMetric):
            self._pending_metrics.append(obj)

    def get(self, model: Any, identity: Mapping[str, Any]) -> Optional[SeasonalityMetric]:
        if model is SeasonalityMetric:
            symbol = identity.get("symbol")
            period = identity.get("period")
            if isinstance(symbol, str) and isinstance(period, str):
                return self._store.get_metric(symbol, period)
        return None

    def commit(self) -> None:
        for bar in self._pending_bars:
            self._store.add_bar(bar)
        self._pending_bars.clear()

        for metric in self._pending_metrics:
            self._store.add_metric_object(metric)
        self._pending_metrics.clear()

    def close(self) -> None:
        self._pending_bars.clear()
        self._pending_metrics.clear()
        self._closed = True

    def load_bars(self, symbol: str) -> List[OhlcvBar]:
        committed = self._store.load_bars(symbol)
        pending = [
            bar
            for bar in self._pending_bars
            if _symbol_key(getattr(bar, "market", "")) == _symbol_key(symbol)
        ]
        return sorted(committed + pending, key=lambda bar: getattr(bar, "bucket_start", datetime.min))

    def metrics_by_period(
        self, *, period: Optional[str] = None, prefix: Optional[str] = None
    ) -> List[SeasonalityMetric]:
        committed = self._store.metrics_by_period(period=period, prefix=prefix)
        pending = [metric for metric in self._pending_metrics if _match_metric(metric, period, prefix)]
        combined = committed + pending
        combined.sort(key=lambda record: (record.symbol, record.period, record.ts))
        return combined

    def upsert_metric(
        self,
        *,
        symbol: str,
        period: str,
        avg_return: float,
        avg_vol: float,
        avg_volume: float,
        ts: datetime,
    ) -> SeasonalityMetric:
        record = self._store.upsert_metric(
            symbol=symbol,
            period=period,
            avg_return=avg_return,
            avg_vol=avg_vol,
            avg_volume=avg_volume,
            ts=ts,
        )
        return record


class _InMemoryEngine:
    """Placeholder Engine implementation used when SQLAlchemy is unavailable."""

    def __init__(self, url: str) -> None:
        self.url = url

    def dispose(self) -> None:  # pragma: no cover - API parity
        return None


def _match_metric(
    metric: SeasonalityMetric, period: Optional[str], prefix: Optional[str]
) -> bool:
    if period is not None and metric.period != period:
        return False
    if prefix is not None and not metric.period.startswith(prefix):
        return False
    return True


__all__ = ["app", "ENGINE", "SessionLocal", "SESSION_STORE"]


ENGINE_STATE_KEY = "seasonality_engine"
SESSIONMAKER_STATE_KEY = "seasonality_sessionmaker"

_IN_MEMORY_STORE = _InMemorySeasonalityStore()

ENGINE: Engine | _InMemoryEngine | None = None
SessionLocal: Callable[[], Session] | Callable[[], _InMemorySession] | None = None
SESSION_STORE: SessionStoreProtocol | None = None


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
    session_factory_obj = getattr(request.app.state, SESSIONMAKER_STATE_KEY, None)
    session_factory: SessionFactory | None
    if callable(session_factory_obj):
        session_factory = cast(SessionFactory, session_factory_obj)
    else:
        session_factory = SessionLocal

    if session_factory is None:
        raise RuntimeError(
            "Seasonality database session maker is not initialised. "
            "Ensure the application startup hook has run successfully."
        )

    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def _configure_database_for_app(application: FastAPI) -> None:
    global ENGINE
    global SessionLocal
    try:
        database_url = _require_database_url()
    except RuntimeError as exc:
        raise RuntimeError(
            "Seasonality service startup failed: configure SEASONALITY_DATABASE_URI, "
            "TIMESCALE_DATABASE_URI, or DATABASE_URL."
        ) from exc

    engine = create_engine(
        database_url.render_as_string(hide_password=False),
        **_engine_options(database_url),
    )
    session_factory = sessionmaker(
        bind=engine, autoflush=False, expire_on_commit=False, future=True
    )

    SeasonalityMetric.__table__.create(bind=engine, checkfirst=True)

    setattr(application.state, ENGINE_STATE_KEY, engine)
    setattr(application.state, SESSIONMAKER_STATE_KEY, session_factory)
    setattr(application.state, "seasonality_database_url", database_url)
    setattr(application.state, "seasonality_engine", engine)
    setattr(application.state, "seasonality_sessionmaker", session_factory)

    ENGINE = engine
    SessionLocal = session_factory


def _dispose_database(application: FastAPI) -> None:
    global ENGINE
    global SessionLocal

    engine = getattr(application.state, ENGINE_STATE_KEY, None)
    if isinstance(engine, Engine):
        engine.dispose()

    for key in (
        ENGINE_STATE_KEY,
        SESSIONMAKER_STATE_KEY,
        "seasonality_database_url",
        "seasonality_engine",
        "seasonality_sessionmaker",
    ):
        if hasattr(application.state, key):
            delattr(application.state, key)

    ENGINE = None
    SessionLocal = None


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
    if _SQLALCHEMY_AVAILABLE and hasattr(session, "execute"):
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

    loader = getattr(session, "load_bars", None)
    records: Sequence[Any]
    if callable(loader):
        records = loader(symbol)
    else:
        records = []

    bars: List[Bar] = []
    for record in records:
        bucket_start = getattr(record, "bucket_start", None)
        if bucket_start is None:
            continue
        timestamp = _ensure_timezone(bucket_start)
        close_raw = getattr(record, "close", None)
        close_value = float(close_raw) if close_raw is not None else None
        volume_raw = getattr(record, "volume", 0.0)
        try:
            volume_value = float(volume_raw)
        except (TypeError, ValueError):
            volume_value = 0.0
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


def _fetch_metrics(
    session: Session,
    *,
    period: Optional[str] = None,
    prefix: Optional[str] = None,
) -> List[SeasonalityMetric]:
    metrics_lookup = getattr(session, "metrics_by_period", None)
    if callable(metrics_lookup):
        return metrics_lookup(period=period, prefix=prefix)

    if not _SQLALCHEMY_AVAILABLE or not hasattr(session, "execute"):
        return []

    query = select(SeasonalityMetric)
    if period is not None:
        query = query.where(SeasonalityMetric.period == period)
    if prefix is not None:
        pattern = prefix if prefix.endswith("%") else f"{prefix}%"
        query = query.where(SeasonalityMetric.period.like(pattern))
    return session.execute(query).scalars().all()


def _validate_symbol(symbol: str) -> str:
    return cast(str, require_spot_http(symbol))


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


RouteFn = TypeVar("RouteFn", bound=Callable[..., Any])


def _app_get(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around :meth:`FastAPI.get` for strict type checking."""

    return cast(Callable[[RouteFn], RouteFn], app.get(*args, **kwargs))


def _app_on_event(event: str) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around :meth:`FastAPI.on_event` for static analysis."""

    return cast(Callable[[RouteFn], RouteFn], app.on_event(event))


def _resolve_session_store_dsn() -> str:
    for env_var in ("SESSION_REDIS_URL", "SESSION_STORE_URL", "SESSION_BACKEND_DSN"):
        raw = os.getenv(env_var)
        if raw is None:
            continue
        value = raw.strip()
        if not value:
            raise RuntimeError(
                f"{env_var} is set but empty; configure a redis:// DSN for the seasonality session store."
            )
        return value
    raise RuntimeError(
        "Session store misconfigured: set SESSION_REDIS_URL, SESSION_STORE_URL, or SESSION_BACKEND_DSN "
        "so the seasonality service can validate administrator tokens."
    )


def _configure_session_store(application: FastAPI) -> SessionStoreProtocol:
    global SESSION_STORE
    existing = getattr(application.state, "session_store", None)
    if isinstance(existing, SessionStoreProtocol):
        store = existing
    else:
        dsn = _resolve_session_store_dsn()
        ttl_minutes = load_session_ttl_minutes()
        if dsn.lower().startswith("memory://"):
            if "pytest" not in sys.modules:
                raise RuntimeError(
                    "Session store misconfigured: memory:// DSNs are only supported when running tests."
                )
            store = InMemorySessionStore(ttl_minutes=ttl_minutes)
        else:
            store = build_session_store_from_url(dsn, ttl_minutes=ttl_minutes)

        application.state.session_store = store

    security.set_default_session_store(store)
    SESSION_STORE = store
    return store



try:
    _configure_database_for_app(app)
except RuntimeError:
    ENGINE = None
    SessionLocal = None

try:
    _configure_session_store(app)
except RuntimeError:
    SESSION_STORE = None


@_app_on_event("startup")
def _on_startup() -> None:
    _configure_database_for_app(app)
    _configure_session_store(app)


@_app_on_event("shutdown")
def _on_shutdown() -> None:
    _dispose_database(app)



@_app_get("/seasonality/dayofweek", response_model=DayOfWeekResponse)
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


@_app_get("/seasonality/hourofday", response_model=HourOfDayResponse)
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


@_app_get("/seasonality/session_liquidity", response_model=SessionLiquidityResponse)
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


@_app_get("/seasonality/current_session", response_model=CurrentSessionResponse)
def current_session(
    *,
    session: Session = Depends(get_session),
    _caller: str = Depends(require_admin_account),
) -> CurrentSessionResponse:
    now = datetime.now(timezone.utc)
    session_name = _session_for_hour(now.hour)

    session_metrics = _fetch_metrics(session, period=f"session:{session_name}")
    if not session_metrics:
        raise HTTPException(status_code=404, detail="No session liquidity metrics available")

    reference_volume = _mean([metric.avg_volume for metric in session_metrics])

    all_session_metrics = _fetch_metrics(session, prefix="session:")
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
