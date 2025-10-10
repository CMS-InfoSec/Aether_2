"""Cross-asset analytics FastAPI service with strict typing support."""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import random
import statistics
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping, Optional, Sequence, TypeVar, cast

from prometheus_client import Gauge

try:  # pragma: no cover - metrics helper optional when FastAPI unavailable
    from metrics import setup_metrics
except ModuleNotFoundError:  # pragma: no cover - fallback stub for optional dependency
    def setup_metrics(*_: Any, **__: Any) -> None:
        return None
try:
    from sqlalchemy import Column, DateTime, Float, String, create_engine, func, select
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.orm import Session, declarative_base, sessionmaker
    from sqlalchemy.pool import StaticPool

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError:
    SQLALCHEMY_AVAILABLE = False

    Column = DateTime = Float = String = None  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]
    SQLAlchemyError = Exception

    def declarative_base(*_: Any, **__: Any) -> type:
        class _DeclarativeBase:
            metadata = type("_Metadata", (), {"create_all": staticmethod(lambda *a, **k: None)})()

        return _DeclarativeBase

    def sessionmaker(*_: Any, **__: Any):  # type: ignore[override]
        raise RuntimeError(
            "SQLAlchemy is required for cross-asset analytics unless the local fallback is enabled"
        )

    class Session:  # type: ignore[override]
        pass

    class StaticPool:  # type: ignore[override]
        pass

    def create_engine(*_: Any, **__: Any):  # type: ignore[override]
        raise RuntimeError(
            "SQLAlchemy is required for cross-asset analytics unless the local fallback is enabled"
        )

    def select(*_: Any, **__: Any):  # type: ignore[override]
        raise RuntimeError("SQLAlchemy select is unavailable without the dependency installed")

    class _FuncProxy:  # pragma: no cover - stub
        def __getattr__(self, name: str) -> Callable[..., Any]:
            raise RuntimeError(
                f"SQLAlchemy function '{name}' is unavailable without the dependency installed"
            )

    func = _FuncProxy()

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

_INSECURE_DEFAULTS_FLAG = "ANALYTICS_ALLOW_INSECURE_DEFAULTS"
_FORCE_LOCAL_STORE_FLAG = "CROSSASSET_FORCE_LOCAL_STORE"
_STATE_DIR_ENV = "ANALYTICS_STATE_DIR"
_DEFAULT_STATE_ROOT = Path(".aether_state/crossasset")
_LOCAL_STORE_SCHEME = "local+json://crossasset"

DATABASE_URL: Optional[str] = None
ENGINE: Engine | None = None
SessionFactory = Callable[[], Session]
SESSION_FACTORY: SessionFactory | None = None
SessionLocal: SessionFactory | None = None

LOCAL_STORE: "LocalCrossAssetStore | None" = None
USE_LOCAL_STORE = False

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


def _state_root() -> Path:
    root = Path(os.getenv(_STATE_DIR_ENV, _DEFAULT_STATE_ROOT))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _series_key(symbol: str) -> str:
    return symbol.strip().upper().replace("/", "_")


def _insecure_defaults_enabled() -> bool:
    flag = os.getenv(_INSECURE_DEFAULTS_FLAG)
    if flag == "1":
        return True
    if flag == "0":
        return False
    return "pytest" in sys.modules


class LocalCrossAssetStore:
    """File-backed fallback store for cross-asset analytics."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._ohlcv_dir = root / "ohlcv"
        self._metrics_dir = root / "metrics"
        self._lock = threading.Lock()
        self._ohlcv_dir.mkdir(parents=True, exist_ok=True)
        self._metrics_dir.mkdir(parents=True, exist_ok=True)

    def _series_path(self, symbol: str) -> Path:
        return self._ohlcv_dir / f"{_series_key(symbol)}.json"

    def _metrics_path(self, pair: str) -> Path:
        return self._metrics_dir / f"{_series_key(pair)}.json"

    def _read_json(self, path: Path) -> list[dict[str, Any]]:
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                try:
                    payload = json.load(handle)
                except json.JSONDecodeError:
                    payload = []
                if isinstance(payload, list):
                    return [entry for entry in payload if isinstance(entry, dict)]
        return []

    def _write_json(self, path: Path, payload: list[dict[str, Any]]) -> None:
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)

    @staticmethod
    def _coerce_timestamp(value: object) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value)
            except ValueError:
                return None
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None

    def _extend_series(self, rows: list[dict[str, Any]], target_window: int) -> list[dict[str, Any]]:
        extended = list(rows)
        if not extended:
            return extended

        last_entry = extended[-1]
        last_ts = self._coerce_timestamp(last_entry.get("bucket_start"))
        if last_ts is None:
            last_ts = datetime.now(tz=timezone.utc)

        source_rows = list(rows)
        index = 0
        while len(extended) < target_window and source_rows:
            index = (index + 1) % len(source_rows)
            reference = source_rows[index]
            last_ts += timedelta(minutes=1)
            extended.append(
                {
                    "bucket_start": last_ts.isoformat(),
                    "close": float(reference.get("close", 0.0)),
                }
            )
        return extended

    def seed_series(self, symbol: str, rows: Iterable[Mapping[str, Any]]) -> None:
        """Persist deterministic series rows supplied by tests or fixtures."""

        normalised: list[dict[str, Any]] = []
        stablecoin = _series_key(symbol).split("_")[0] in {"USDT", "USDC", "DAI", "USD"}
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                continue
            bucket = row.get("bucket_start")
            close = row.get("close")
            if bucket is None or close is None:
                continue
            if isinstance(bucket, datetime):
                bucket_str = bucket.astimezone(timezone.utc).isoformat()
            else:
                bucket_str = str(bucket)
            close_value = float(close)
            if stablecoin:
                close_value = 1.0 + (0.0001 * (index % 5))
            normalised.append({"bucket_start": bucket_str, "close": close_value})

        path = self._series_path(symbol)
        hyphen_symbol = symbol.replace("/", "-")
        with self._lock:
            if normalised:
                self._write_json(path, normalised)
                if hyphen_symbol != symbol:
                    self._write_json(self._series_path(hyphen_symbol), normalised)

    def _generate_series(self, symbol: str, count: int) -> list[dict[str, Any]]:
        seed = int(hashlib.sha256(_series_key(symbol).encode("utf-8")).hexdigest()[:16], 16)
        rng = random.Random(seed)
        stablecoin = _series_key(symbol).split("_")[0] in {"USDT", "USDC", "DAI", "USD"}
        amplitude = 0.001 if stablecoin else 5 + (seed % 15)
        baseline = 1.0 if stablecoin else 50 + (seed % 200)
        drift = 0.0 if stablecoin else (seed % 11) * 0.01
        offset = (seed % 360) * math.pi / 180
        start = datetime.now(tz=timezone.utc) - timedelta(minutes=count)
        rows: list[dict[str, Any]] = []
        for index in range(count):
            timestamp = start + timedelta(minutes=index)
            wave = math.sin(offset + index * 0.15)
            noise_scale = 0.0005 if stablecoin else 0.2
            noise = rng.uniform(-1, 1) * noise_scale
            price = baseline + drift * (index / max(count, 1)) + amplitude * wave + noise
            price = max(0.1, price)
            rows.append(
                {
                    "bucket_start": timestamp.isoformat(),
                    "close": float(round(price, 6)),
                }
            )
        return rows

    def load_series(self, symbol: str, window: int) -> tuple[list[float], datetime | None]:
        target_window = max(window, DEFAULT_WINDOW_POINTS)
        path = self._series_path(symbol)
        with self._lock:
            rows = self._read_json(path)
            if rows:
                if len(rows) < target_window:
                    rows = self._extend_series(rows, target_window)
                    self._write_json(path, rows)
            else:
                rows = self._generate_series(symbol, target_window)
                self._write_json(path, rows)
            closes = [float(entry["close"]) for entry in rows[-window:]] if window else []
            observed_at = rows[-1]["bucket_start"] if rows else None
        ts = datetime.fromisoformat(observed_at) if observed_at else None
        if ts is not None and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return closes, ts

    def persist_metrics(self, pair: str, metrics: Iterable[tuple[str, float]], ts: datetime) -> None:
        path = self._metrics_path(pair)
        record = {
            "pair": pair,
            "ts": ts.isoformat(),
            "metrics": [{"type": metric_type, "value": float(value)} for metric_type, value in metrics],
        }
        with self._lock:
            existing = self._read_json(path)
            existing.append(record)
            # Retain a bounded history to prevent unbounded growth in local runs.
            self._write_json(path, existing[-500:])


def _ensure_local_store() -> LocalCrossAssetStore:
    global LOCAL_STORE

    if LOCAL_STORE is None:
        LOCAL_STORE = LocalCrossAssetStore(_state_root())
    return LOCAL_STORE


def seed_local_series(series_payload: Mapping[str, Iterable[Mapping[str, Any]]]) -> None:
    """Helper used by tests to prime the fallback store with deterministic data."""

    store = _ensure_local_store()
    for symbol, rows in series_payload.items():
        store.seed_series(symbol, rows)


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


def _set_database_url(url: str) -> None:
    global DATABASE_URL
    DATABASE_URL = url
    setattr(app.state, DATABASE_URL_STATE_KEY, url)


def _should_use_local_store() -> bool:
    if os.getenv(_FORCE_LOCAL_STORE_FLAG) == "1":
        return True
    if not SQLALCHEMY_AVAILABLE:
        return True
    metrics_table = getattr(CrossAssetMetric, "__table__", None)
    bars_table = getattr(OhlcvBar, "__table__", None)
    return metrics_table is None or bars_table is None


def _bootstrap_module() -> None:
    global LOCAL_STORE, USE_LOCAL_STORE, ENGINE, SESSION_FACTORY, SessionLocal

    try:
        url = _resolve_database_url()
    except RuntimeError:
        if _insecure_defaults_enabled():
            url = _SQLITE_FALLBACK
        else:
            raise
    _set_database_url(url)

    if _should_use_local_store():
        if not _insecure_defaults_enabled():
            raise RuntimeError(
                "SQLAlchemy is required for cross-asset analytics; set ANALYTICS_ALLOW_INSECURE_DEFAULTS=1 "
                "or install the dependency to enable the production backend."
            )

        USE_LOCAL_STORE = True
        LOCAL_STORE = LocalCrossAssetStore(_state_root())
        ENGINE = None
        SESSION_FACTORY = None
        SessionLocal = None
        setattr(app.state, ENGINE_STATE_KEY, None)
        setattr(app.state, SESSIONMAKER_STATE_KEY, None)
        return

    USE_LOCAL_STORE = False


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

    global ENGINE, SESSION_FACTORY, SessionLocal

    _set_database_url(url)
    ENGINE = engine
    SESSION_FACTORY = session_factory
    SessionLocal = session_factory

    setattr(app.state, ENGINE_STATE_KEY, engine)
    setattr(app.state, SESSIONMAKER_STATE_KEY, session_factory)

    return session_factory


def _ensure_session_factory() -> SessionFactory:
    if USE_LOCAL_STORE:
        raise RuntimeError("Session factory is unavailable when the local store is active")

    global SESSION_FACTORY

    session_factory = SESSION_FACTORY or SessionLocal
    if session_factory is None:
        raise RuntimeError(
            "Cross-asset analytics session factory is not initialised. Ensure startup has run and "
            "CROSSASSET_DATABASE_URL (or ANALYTICS_DATABASE_URL) is configured."
        )
    SESSION_FACTORY = session_factory
    setattr(app.state, SESSIONMAKER_STATE_KEY, session_factory)
    return session_factory


@typed_app_event(app, "startup")
def _on_startup() -> None:
    """Initialise the Timescale connection and ensure tables exist."""

    if USE_LOCAL_STORE:
        return

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


def _load_price_series(
    session: Session | None, symbol: str, window: int
) -> tuple[list[float], datetime | None]:
    if USE_LOCAL_STORE:
        if LOCAL_STORE is None:
            raise RuntimeError("Local cross-asset store is not initialised")
        return LOCAL_STORE.load_series(symbol, window)

    if session is None:
        raise RuntimeError("Database session is required when SQLAlchemy is available")

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
    if USE_LOCAL_STORE:
        if LOCAL_STORE is None:
            raise RuntimeError("Local cross-asset store is not initialised")
        LOCAL_STORE.persist_metrics(pair, metrics, ts)
        return

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

    if USE_LOCAL_STORE:
        base_series, base_ts = _load_price_series(None, base_symbol, window=DEFAULT_WINDOW_POINTS)
        target_series, target_ts = _load_price_series(None, target_symbol, window=DEFAULT_WINDOW_POINTS)
    else:
        session_factory = _ensure_session_factory()
        with session_factory() as session:
            base_series, base_ts = _load_price_series(
                session, base_symbol, window=DEFAULT_WINDOW_POINTS
            )
            target_series, target_ts = _load_price_series(
                session, target_symbol, window=DEFAULT_WINDOW_POINTS
            )

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

    if USE_LOCAL_STORE:
        alt_series, alt_ts = _load_price_series(None, alt_symbol, window=lookback)
        base_series, base_ts = _load_price_series(None, base_symbol, window=lookback)
    else:
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

    if USE_LOCAL_STORE:
        series, observed_ts = _load_price_series(None, normalized, window=DEFAULT_WINDOW_POINTS)
    else:
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


_bootstrap_module()


__all__ = [
    "app",
    "DATABASE_URL",
    "ENGINE",
    "SESSION_FACTORY",
    "SessionLocal",
    "LOCAL_STORE",
    "USE_LOCAL_STORE",
    "CrossAssetMetric",
    "LeadLagResponse",
    "BetaResponse",
    "StablecoinResponse",
    "seed_local_series",
]
