"""Risk correlation service exposing rolling correlation matrices."""

from __future__ import annotations

import logging
import math
import os
import sys
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Dict, Iterable, Iterator, List, Mapping, MutableMapping, Sequence

from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import Depends, FastAPI, HTTPException, Query, status
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[assignment]
        Depends,
        FastAPI,
        HTTPException,
        Query,
        status,
    )

from metrics import setup_metrics
from pydantic import BaseModel, Field

try:  # pragma: no cover - optional dependency
    import sqlalchemy as _sqlalchemy_module  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - executed when SQLAlchemy missing
    _sqlalchemy_module = None  # type: ignore[assignment]

if _sqlalchemy_module is not None and not getattr(
    _sqlalchemy_module, "__aether_stub__", False
):
    from sqlalchemy import (  # type: ignore[import-not-found]
        Column,
        DateTime,
        Float,
        Integer,
        String,
        UniqueConstraint,
        create_engine,
        select,
        text,
    )
    from sqlalchemy.engine import Engine  # type: ignore[import-not-found]
    from sqlalchemy.exc import SQLAlchemyError  # type: ignore[import-not-found]
    from sqlalchemy.orm import Session, declarative_base, sessionmaker  # type: ignore[import-not-found]
    from sqlalchemy.pool import StaticPool  # type: ignore[import-not-found]

    SQLALCHEMY_AVAILABLE = True
else:  # pragma: no cover - executed when SQLAlchemy is unavailable or shimmed
    Column = DateTime = Float = Integer = String = UniqueConstraint = object  # type: ignore[assignment]
    Engine = object  # type: ignore[assignment]
    SQLAlchemyError = Exception  # type: ignore[assignment]

    def create_engine(*_args, **_kwargs):  # type: ignore[override]
        raise RuntimeError("sqlalchemy is required for correlation persistence")

    def select(*_args, **_kwargs):  # type: ignore[override]
        raise RuntimeError("sqlalchemy is required for correlation persistence")

    def text(query: str) -> str:  # type: ignore[override]
        return query

    def declarative_base():  # type: ignore[override]
        raise RuntimeError("sqlalchemy is required for correlation persistence")

    def sessionmaker(*_args, **_kwargs):  # type: ignore[override]
        raise RuntimeError("sqlalchemy is required for correlation persistence")

    StaticPool = object  # type: ignore[assignment]
    Session = object  # type: ignore[assignment]
    SQLALCHEMY_AVAILABLE = False

from services.alert_manager import AlertManager, RiskEvent, get_alert_metrics
from shared.postgres import normalize_sqlalchemy_dsn
from shared.spot import filter_spot_symbols


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database models and helpers
# ---------------------------------------------------------------------------


_DEFAULT_CORRELATION_DB_URL = "sqlite:///./risk_correlations.db"


def _database_url() -> str:
    """Resolve the correlation service database connection string."""

    allow_sqlite = os.getenv("RISK_CORRELATION_ALLOW_SQLITE") == "1"
    if not allow_sqlite and "pytest" in sys.modules:
        allow_sqlite = os.getenv("PYTEST_CURRENT_TEST") is not None
    raw = (
        os.getenv("RISK_CORRELATION_DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or (_DEFAULT_CORRELATION_DB_URL if allow_sqlite else None)
    )

    if raw is None:
        raise RuntimeError(
            "Risk correlation database DSN is not configured. "
            "Set RISK_CORRELATION_DATABASE_URL or TIMESCALE_DSN to a PostgreSQL/Timescale URI.",
        )

    candidate = raw.strip()
    if not candidate:
        raise RuntimeError("Risk correlation database DSN cannot be empty once configured.")

    return normalize_sqlalchemy_dsn(
        candidate,
        allow_sqlite=allow_sqlite,
        label="Risk correlation database DSN",
    )


def _engine_options(url: str) -> dict[str, object]:
    options: dict[str, object] = {"future": True, "pool_pre_ping": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


if SQLALCHEMY_AVAILABLE:
    _DB_URL: str | None = None
    _ENGINE: Engine | None = None
    SessionLocal: sessionmaker | None = None

    Base = declarative_base()

    class CorrelationRecord(Base):
        """Persistence model capturing a correlation observation."""

        __tablename__ = "correlations"

        id = Column(Integer, primary_key=True, autoincrement=True)
        timestamp = Column(DateTime(timezone=True), index=True, nullable=False)
        symbol1 = Column(String(32), nullable=False)
        symbol2 = Column(String(32), nullable=False)
        correlation = Column(Float, nullable=False)

        __table_args__ = (
            UniqueConstraint("timestamp", "symbol1", "symbol2", name="uq_correlation_pair"),
        )

    def _ensure_engine() -> tuple[Engine, sessionmaker]:
        global _DB_URL, _ENGINE, SessionLocal

        if _ENGINE is None or SessionLocal is None:
            url = _database_url()
            _DB_URL = url
            _ENGINE = create_engine(url, **_engine_options(url))
            SessionLocal = sessionmaker(
                bind=_ENGINE, autoflush=False, expire_on_commit=False, future=True
            )
        assert _ENGINE is not None
        assert SessionLocal is not None
        return _ENGINE, SessionLocal

else:
    Base = None  # type: ignore[assignment]


class _CorrelationPersistence:
    """Interface for correlation persistence backends."""

    def create_schema(self) -> None:
        raise NotImplementedError

    def session(self) -> Iterator[object]:
        raise NotImplementedError

    def persist(self, session: object, *, timestamp: datetime, matrix: Mapping[str, Mapping[str, float]]) -> None:
        raise NotImplementedError


class _InMemoryPersistence(_CorrelationPersistence):
    """Lightweight persistence fallback used when SQLAlchemy is unavailable."""

    def __init__(self) -> None:
        self._records: MutableMapping[tuple[datetime, str, str], float] = {}

    def create_schema(self) -> None:  # pragma: no cover - nothing to initialise
        return

    @contextmanager
    def session(self) -> Iterator[object]:  # pragma: no cover - simple context manager
        yield self

    def persist(
        self,
        _session: object,
        *,
        timestamp: datetime,
        matrix: Mapping[str, Mapping[str, float]],
    ) -> None:
        for symbol_a, row in matrix.items():
            for symbol_b, value in row.items():
                sym1, sym2 = _canonical_pair(symbol_a, symbol_b)
                self._records[(timestamp, sym1, sym2)] = float(value)

    def snapshot(self) -> Mapping[tuple[datetime, str, str], float]:
        return dict(self._records)


if SQLALCHEMY_AVAILABLE:

    class _SQLAlchemyPersistence(_CorrelationPersistence):
        def create_schema(self) -> None:
            engine, _ = _ensure_engine()
            Base.metadata.create_all(bind=engine)

        @contextmanager
        def session(self) -> Iterator[Session]:
            _, factory = _ensure_engine()
            session = factory()
            try:
                yield session
            finally:
                session.close()

        def persist(
            self,
            session: Session,
            *,
            timestamp: datetime,
            matrix: Mapping[str, Mapping[str, float]],
        ) -> None:
            records: list[CorrelationRecord] = []
            for symbol_a, row in matrix.items():
                for symbol_b, value in row.items():
                    if symbol_a > symbol_b:
                        continue
                    sym1, sym2 = _canonical_pair(symbol_a, symbol_b)
                    existing = session.execute(
                        select(CorrelationRecord).where(
                            CorrelationRecord.timestamp == timestamp,
                            CorrelationRecord.symbol1 == sym1,
                            CorrelationRecord.symbol2 == sym2,
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.correlation = float(value)
                        continue
                    records.append(
                        CorrelationRecord(
                            timestamp=timestamp,
                            symbol1=sym1,
                            symbol2=sym2,
                            correlation=float(value),
                        )
                    )

            if records:
                session.add_all(records)
            session.commit()


    correlation_persistence: _CorrelationPersistence = _SQLAlchemyPersistence()
else:
    correlation_persistence = _InMemoryPersistence()


# ---------------------------------------------------------------------------
# Market data warehouse integration
# ---------------------------------------------------------------------------


def _tracked_symbols() -> Sequence[str]:
    configured = os.getenv(
        "RISK_CORRELATION_SYMBOLS",
        "BTC-USD,ETH-USD,SOL-USD,ADA-USD",
    )
    configured_symbols = [symbol.strip() for symbol in configured.split(",") if symbol.strip()]

    filtered = filter_spot_symbols(configured_symbols, logger=logger)
    if not filtered:
        logger.warning(
            "Risk correlation service defaulting tracked symbols to BTC-USD and ETH-USD",
            extra={"configured_symbols": configured_symbols or None},
        )
        filtered = ["BTC-USD", "ETH-USD"]

    return tuple(filtered)


def _staleness_threshold() -> timedelta:
    minutes = int(os.getenv("RISK_MARKETDATA_STALE_MINUTES", "30"))
    return timedelta(minutes=max(1, minutes))


def _marketdata_table() -> str:
    return os.getenv("RISK_MARKETDATA_TABLE", "ohlcv_bars")


_MARKETDATA_ENGINE: Engine | None = None


def _marketdata_url() -> str:
    allow_sqlite = "pytest" in sys.modules
    candidates = (
        os.getenv("RISK_MARKETDATA_URL"),
        os.getenv("RISK_MARKETDATA_DATABASE_URL"),
        os.getenv("TIMESCALE_DATABASE_URI"),
        os.getenv("DATABASE_URL"),
    )
    for raw in candidates:
        if not raw:
            continue
        candidate = raw.strip()
        if not candidate:
            continue
        return normalize_sqlalchemy_dsn(
            candidate,
            allow_sqlite=allow_sqlite,
            label="Risk market data DSN",
        )
    raise RuntimeError(
        "Risk correlation service requires RISK_MARKETDATA_URL (or TIMESCALE_DATABASE_URI) "
        "to load OHLCV history from the market data warehouse.",
    )


def _marketdata_engine() -> Engine:
    if not SQLALCHEMY_AVAILABLE:
        raise RuntimeError("sqlalchemy is required to query market data history")
    global _MARKETDATA_ENGINE
    if _MARKETDATA_ENGINE is not None:
        return _MARKETDATA_ENGINE

    url = _marketdata_url()
    _MARKETDATA_ENGINE = create_engine(url, **_engine_options(url))
    return _MARKETDATA_ENGINE


def _reset_marketdata_engine() -> None:  # pragma: no cover - used in tests
    global _MARKETDATA_ENGINE
    if _MARKETDATA_ENGINE is not None:
        _MARKETDATA_ENGINE.dispose()
        _MARKETDATA_ENGINE = None


def _coerce_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        try:
            ts = datetime.fromisoformat(value)
        except ValueError:  # pragma: no cover - defensive guard
            return None
    else:
        return None

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Price history store used for rolling correlations
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class PricePoint:
    symbol: str
    timestamp: datetime
    price: float


class PriceHistoryStore:
    """Thread-safe price history store used for correlation calculations."""

    def __init__(self) -> None:
        self._prices: Dict[str, List[PricePoint]] = defaultdict(list)
        self._latest: Dict[str, datetime] = {}
        self._lock = Lock()

    # ------------------------------------------------------------------
    # Mutation helpers
    # ------------------------------------------------------------------
    def record(self, point: PricePoint) -> None:
        with self._lock:
            series = self._prices.setdefault(point.symbol, [])
            series.append(point)
            series.sort(key=lambda entry: entry.timestamp)
            self._latest[point.symbol] = series[-1].timestamp

    def bulk_record(self, points: Iterable[PricePoint]) -> None:
        for point in points:
            self.record(point)

    def replace(self, symbol: str, points: Iterable[PricePoint]) -> None:
        ordered = sorted(points, key=lambda entry: entry.timestamp)
        with self._lock:
            self._prices[symbol] = ordered
            if ordered:
                self._latest[symbol] = ordered[-1].timestamp
            else:
                self._latest.pop(symbol, None)

    def clear(self) -> None:
        with self._lock:
            self._prices.clear()
            self._latest.clear()

    def series_length(self, symbol: str) -> int:
        with self._lock:
            return len(self._prices.get(symbol, []))

    def latest_timestamp(self, symbol: str) -> datetime | None:
        with self._lock:
            return self._latest.get(symbol)

    def symbols(self) -> list[str]:
        with self._lock:
            return list(self._prices.keys())

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def aligned_prices(
        self, window: int
    ) -> tuple[list[datetime], dict[str, list[float]]]:
        """Return aligned price series for the requested rolling window."""

        if window < 2:
            raise ValueError("window must be at least 2 to compute returns")

        with self._lock:
            trimmed: dict[str, list[PricePoint]] = {}
            for symbol, series in self._prices.items():
                if len(series) < window + 1:
                    continue
                trimmed[symbol] = series[-(window + 1) :]

        if not trimmed:
            return [], {}

        common: set[datetime] | None = None
        for points in trimmed.values():
            timestamps = {point.timestamp for point in points}
            common = timestamps if common is None else common & timestamps
            if not common:
                return [], {}

        assert common is not None
        aligned_timestamps = sorted(common)[-(window + 1) :]
        if len(aligned_timestamps) < window + 1:
            return [], {}

        prices: dict[str, list[float]] = {}
        for symbol, points in trimmed.items():
            by_timestamp = {point.timestamp: float(point.price) for point in points}
            prices[symbol] = [by_timestamp[ts] for ts in aligned_timestamps]

        return aligned_timestamps, prices


price_store = PriceHistoryStore()


def _fetch_price_history(*, symbols: Sequence[str], lookback: int) -> Mapping[str, List[PricePoint]]:
    if not SQLALCHEMY_AVAILABLE:
        raise RuntimeError("sqlalchemy is required to fetch market data history")
    engine = _marketdata_engine()
    table = _marketdata_table()
    history: dict[str, List[PricePoint]] = {}
    limit = max(lookback + 1, 2)

    try:
        with engine.connect() as conn:
            for symbol in symbols:
                rows = conn.execute(
                    text(
                        f"""
                        SELECT bucket_start, close
                        FROM {table}
                        WHERE upper(market) = :symbol
                        ORDER BY bucket_start DESC
                        LIMIT :limit
                        """
                    ),
                    {"symbol": symbol.upper(), "limit": limit},
                ).all()

                points: List[PricePoint] = []
                for bucket_start, close in rows:
                    if close is None:
                        continue
                    ts = _coerce_timestamp(bucket_start)
                    if ts is None:
                        continue
                    points.append(PricePoint(symbol=symbol, timestamp=ts, price=float(close)))
                points.reverse()
                history[symbol] = points
    except SQLAlchemyError as exc:  # pragma: no cover - defensive
        logger.exception("Failed to load OHLCV history from warehouse")
        raise RuntimeError("Unable to load OHLCV history from market data warehouse") from exc

    missing = [symbol for symbol, points in history.items() if not points]
    if missing:
        raise RuntimeError(
            f"No price history available for symbols: {', '.join(sorted(missing))}"
        )

    return history


def _prime_price_cache(*, symbols: Sequence[str], lookback: int) -> None:
    if not SQLALCHEMY_AVAILABLE:
        logger.warning(
            "sqlalchemy not available; skipping market data preload for correlation service"
        )
        return

    history = _fetch_price_history(symbols=symbols, lookback=lookback)
    if not history:
        raise RuntimeError("Market data warehouse returned no history for configured symbols")

    price_store.clear()
    for symbol, points in history.items():
        price_store.replace(symbol, points)

    logger.info("Loaded market data history for %s from warehouse", ", ".join(symbols))


def _ensure_market_data(symbols: Sequence[str], window: int) -> None:
    missing: list[str] = []
    stale: list[tuple[str, datetime]] = []
    now = datetime.now(timezone.utc)
    threshold = _staleness_threshold()

    for symbol in symbols:
        length = price_store.series_length(symbol)
        if length < window + 1:
            missing.append(symbol)
            continue
        latest = price_store.latest_timestamp(symbol)
        if latest is None:
            missing.append(symbol)
            continue
        if now - latest > threshold:
            stale.append((symbol, latest))

    if missing or stale:
        if missing:
            detail = "Missing market data for symbols: " + ", ".join(sorted(set(missing)))
            event = RiskEvent(
                event_type="market_data_missing",
                severity="critical",
                description=detail,
                labels={"symbols": ",".join(sorted(set(missing)))},
            )
        else:
            stale_desc = ", ".join(
                f"{symbol} ({int((now - ts).total_seconds())}s old)" for symbol, ts in stale
            )
            detail = f"Stale market data: {stale_desc}"
            event = RiskEvent(
                event_type="market_data_stale",
                severity="critical",
                description=detail,
                labels={
                    "symbols": ",".join(symbol for symbol, _ in stale),
                    "threshold_seconds": str(int(threshold.total_seconds())),
                },
            )

        alert_manager.handle_risk_event(event)
        logger.error(detail)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=detail)


# ---------------------------------------------------------------------------
# FastAPI models
# ---------------------------------------------------------------------------


class CorrelationMatrixResponse(BaseModel):
    """Response payload for correlation matrix requests."""

    timestamp: datetime = Field(..., description="Timestamp when the correlation was computed")
    window: int = Field(..., ge=2, description="Number of returns used for the rolling window")
    symbols: List[str] = Field(..., description="Ordered list of symbols included in the matrix")
    matrix: Dict[str, Dict[str, float]] = Field(
        ..., description="Nested mapping of symbol correlations"
    )


# ---------------------------------------------------------------------------
# FastAPI application setup
# ---------------------------------------------------------------------------


app = FastAPI(title="Risk Correlation Service")
setup_metrics(app, service_name="risk-correlation-service")

alert_manager = AlertManager(metrics=get_alert_metrics())


@app.on_event("startup")
def _on_startup() -> None:
    """Ensure the persistence backend is ready and cache market data history."""

    try:
        correlation_persistence.create_schema()
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to initialise correlation persistence backend")
        raise

    symbols = _tracked_symbols()
    lookback = int(os.getenv("RISK_MARKETDATA_LOOKBACK", "240"))

    try:
        _prime_price_cache(symbols=symbols, lookback=max(lookback, 2))
    except Exception as exc:  # pragma: no cover - exercised by integration tests
        logger.exception("Failed to load market data from warehouse")
        raise


def get_session() -> Iterator[object]:
    """FastAPI dependency that yields a persistence session/context."""

    with correlation_persistence.session() as session:
        yield session


# ---------------------------------------------------------------------------
# Core correlation logic
# ---------------------------------------------------------------------------


def _returns_from_prices(prices: Mapping[str, Sequence[float]]) -> Dict[str, List[float]]:
    returns: Dict[str, List[float]] = {}
    for symbol, series in prices.items():
        if len(series) < 2:
            continue
        prev = series[0]
        symbol_returns: List[float] = []
        for price in series[1:]:
            if prev == 0:
                raise ValueError("cannot compute returns when a previous price is zero")
            change = (price - prev) / prev
            symbol_returns.append(change)
            prev = price
        if symbol_returns:
            returns[symbol] = symbol_returns
    return returns


def _canonical_pair(symbol_a: str, symbol_b: str) -> tuple[str, str]:
    return tuple(sorted((symbol_a, symbol_b)))


def _pearson_correlation(series_a: Sequence[float], series_b: Sequence[float]) -> float:
    if len(series_a) != len(series_b):
        raise ValueError("correlation series must have matching lengths")
    if len(series_a) < 2:
        raise ValueError("at least two observations are required to compute correlation")

    mean_a = sum(series_a) / len(series_a)
    mean_b = sum(series_b) / len(series_b)

    diff_a = [value - mean_a for value in series_a]
    diff_b = [value - mean_b for value in series_b]

    numerator = sum(a * b for a, b in zip(diff_a, diff_b))
    denom_a = sum(value * value for value in diff_a)
    denom_b = sum(value * value for value in diff_b)

    if denom_a <= 0 or denom_b <= 0:
        return 0.0

    value = numerator / math.sqrt(denom_a * denom_b)
    if math.isnan(value):  # pragma: no cover - defensive guard
        return 0.0
    return max(-1.0, min(1.0, value))


def _compute_correlation_matrix(returns: Mapping[str, Sequence[float]]) -> Dict[str, Dict[str, float]]:
    symbols = sorted(returns.keys())
    if not symbols:
        return {}

    expected_length = {len(returns[symbol]) for symbol in symbols}
    if len(expected_length) != 1:
        raise ValueError("return series must have consistent lengths")

    matrix: Dict[str, Dict[str, float]] = {}
    for symbol_a in symbols:
        row: Dict[str, float] = {}
        for symbol_b in symbols:
            if symbol_a == symbol_b:
                row[symbol_b] = 1.0
            else:
                row[symbol_b] = _pearson_correlation(returns[symbol_a], returns[symbol_b])
        matrix[symbol_a] = row
    return matrix


def _persist_matrix(
    session: object, *, timestamp: datetime, matrix: Mapping[str, Mapping[str, float]]
) -> None:
    correlation_persistence.persist(session, timestamp=timestamp, matrix=matrix)


def _trigger_breakdown_alert(
    matrix: Mapping[str, Mapping[str, float]], threshold: float = 0.2
) -> None:
    symbol_a, symbol_b = "BTC-USD", "ETH-USD"
    row = matrix.get(symbol_a)
    if not row or symbol_b not in row:
        return

    value = float(row[symbol_b])
    if value >= threshold:
        return

    alert_manager.handle_risk_event(
        RiskEvent(
            event_type="correlation_breakdown",
            severity="warning",
            description=(
                f"Correlation between {symbol_a} and {symbol_b} dropped below {threshold:.2f}: "
                f"{value:.2f}"
            ),
            labels={"symbol_1": symbol_a, "symbol_2": symbol_b, "threshold": str(threshold)},
        )
    )
    logger.warning(
        "Correlation breakdown detected for %s/%s: %.3f (threshold %.2f)",
        symbol_a,
        symbol_b,
        value,
        threshold,
    )


# ---------------------------------------------------------------------------
# HTTP routes
# ---------------------------------------------------------------------------


@app.get("/correlations/matrix", response_model=CorrelationMatrixResponse)
def correlation_matrix(
    window: int = Query(30, ge=2, le=500, description="Rolling window length in observations"),
    session: object = Depends(get_session),
) -> CorrelationMatrixResponse:
    symbols = _tracked_symbols()
    _ensure_market_data(symbols, window)

    _, prices = price_store.aligned_prices(window)
    if not prices:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Insufficient price history to compute correlations for the requested window.",
        )

    try:
        returns = _returns_from_prices(prices)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    if not returns:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Unable to compute returns for the requested window.",
        )

    try:
        matrix = _compute_correlation_matrix(returns)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    if not matrix:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Unable to compute correlation matrix for the requested window.",
        )
    timestamp = datetime.now(timezone.utc)

    _persist_matrix(session, timestamp=timestamp, matrix=matrix)
    _trigger_breakdown_alert(matrix)

    return CorrelationMatrixResponse(
        timestamp=timestamp,
        window=window,
        symbols=list(matrix.keys()),
        matrix={symbol: dict(row) for symbol, row in matrix.items()},
    )


__all__ = [
    "app",
    "correlation_matrix",
    "CorrelationMatrixResponse",
    "price_store",
    "_reset_marketdata_engine",
]
