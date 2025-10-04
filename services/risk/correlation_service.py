"""Risk correlation service exposing rolling correlation matrices."""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Dict, Iterable, Iterator, List, Mapping, Sequence

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import (
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
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from services.alert_manager import AlertManager, RiskEvent, get_alert_metrics


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database models and helpers
# ---------------------------------------------------------------------------


_DEFAULT_CORRELATION_DB_URL = "sqlite:///./risk_correlations.db"


def _database_url() -> str:
    """Return the configured database URL using SQLite as a default."""

    return os.getenv("RISK_CORRELATION_DATABASE_URL", _DEFAULT_CORRELATION_DB_URL)


ENGINE: Engine = create_engine(_database_url(), future=True)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

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


# ---------------------------------------------------------------------------
# Market data warehouse integration
# ---------------------------------------------------------------------------


def _tracked_symbols() -> Sequence[str]:
    configured = os.getenv(
        "RISK_CORRELATION_SYMBOLS",
        "BTC-USD,ETH-USD,SOL-USD,ADA-USD",
    )
    symbols = [symbol.strip() for symbol in configured.split(",") if symbol.strip()]
    if not symbols:
        symbols = ["BTC-USD", "ETH-USD"]
    return tuple(dict.fromkeys(symbols))


def _staleness_threshold() -> timedelta:
    minutes = int(os.getenv("RISK_MARKETDATA_STALE_MINUTES", "30"))
    return timedelta(minutes=max(1, minutes))


def _marketdata_table() -> str:
    return os.getenv("RISK_MARKETDATA_TABLE", "ohlcv_bars")


_MARKETDATA_ENGINE: Engine | None = None


def _marketdata_url() -> str:
    candidates = (
        os.getenv("RISK_MARKETDATA_URL"),
        os.getenv("RISK_MARKETDATA_DATABASE_URL"),
        os.getenv("TIMESCALE_DATABASE_URI"),
        os.getenv("DATABASE_URL"),
    )
    for candidate in candidates:
        if candidate:
            return candidate
    raise RuntimeError(
        "Risk correlation service requires RISK_MARKETDATA_URL (or TIMESCALE_DATABASE_URI) "
        "to load OHLCV history from the market data warehouse."
    )


def _marketdata_engine() -> Engine:
    global _MARKETDATA_ENGINE
    if _MARKETDATA_ENGINE is not None:
        return _MARKETDATA_ENGINE

    url = _marketdata_url()
    options: dict[str, object] = {"future": True, "pool_pre_ping": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
    _MARKETDATA_ENGINE = create_engine(url, **options)
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
    def price_frame(self, window: int) -> pd.DataFrame:
        """Return a price dataframe with aligned timestamps across symbols."""

        if window < 2:
            raise ValueError("window must be at least 2 to compute returns")

        with self._lock:
            frames: list[pd.Series] = []
            for symbol, series in self._prices.items():
                if len(series) < window + 1:
                    continue
                trimmed = series[-(window + 1) :]
                index = pd.Index([point.timestamp for point in trimmed], name="timestamp")
                values = [point.price for point in trimmed]
                frames.append(pd.Series(values, index=index, name=symbol))

        if not frames:
            return pd.DataFrame()

        frame = pd.concat(frames, axis=1).sort_index()
        frame = frame.dropna(how="any")
        return frame


price_store = PriceHistoryStore()


def _fetch_price_history(*, symbols: Sequence[str], lookback: int) -> Mapping[str, List[PricePoint]]:
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

alert_manager = AlertManager(metrics=get_alert_metrics())


@app.on_event("startup")
def _on_startup() -> None:
    """Ensure the database is ready and cache market data history."""

    Base.metadata.create_all(bind=ENGINE)
    symbols = _tracked_symbols()
    lookback = int(os.getenv("RISK_MARKETDATA_LOOKBACK", "240"))

    try:
        _prime_price_cache(symbols=symbols, lookback=max(lookback, 2))
    except Exception as exc:  # pragma: no cover - exercised by integration tests
        logger.exception("Failed to load market data from warehouse")
        raise


def get_session() -> Iterator[Session]:
    """FastAPI dependency that yields a SQLAlchemy session."""

    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Core correlation logic
# ---------------------------------------------------------------------------


def _returns_from_prices(prices: pd.DataFrame) -> pd.DataFrame:
    returns = prices.pct_change().dropna(how="any")
    return returns


def _canonical_pair(symbol_a: str, symbol_b: str) -> tuple[str, str]:
    return tuple(sorted((symbol_a, symbol_b)))


def _persist_matrix(
    session: Session, *, timestamp: datetime, matrix: pd.DataFrame
) -> None:
    records: list[CorrelationRecord] = []
    for symbol_a in matrix.columns:
        for symbol_b in matrix.columns:
            if symbol_a > symbol_b:
                continue
            value = float(matrix.at[symbol_a, symbol_b])
            sym1, sym2 = _canonical_pair(symbol_a, symbol_b)
            existing = session.execute(
                select(CorrelationRecord).where(
                    CorrelationRecord.timestamp == timestamp,
                    CorrelationRecord.symbol1 == sym1,
                    CorrelationRecord.symbol2 == sym2,
                )
            ).scalar_one_or_none()
            if existing:
                existing.correlation = value
                continue
            records.append(
                CorrelationRecord(
                    timestamp=timestamp,
                    symbol1=sym1,
                    symbol2=sym2,
                    correlation=value,
                )
            )

    if records:
        session.add_all(records)
    session.commit()


def _matrix_to_nested_dict(matrix: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    nested: Dict[str, Dict[str, float]] = {}
    for symbol, row in matrix.iterrows():
        nested[symbol] = {col: float(row[col]) for col in matrix.columns}
    return nested


def _trigger_breakdown_alert(matrix: pd.DataFrame, threshold: float = 0.2) -> None:
    symbol_a, symbol_b = "BTC-USD", "ETH-USD"
    if symbol_a not in matrix.columns or symbol_b not in matrix.columns:
        return

    value = float(matrix.at[symbol_a, symbol_b])
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
    session: Session = Depends(get_session),
) -> CorrelationMatrixResponse:
    symbols = _tracked_symbols()
    _ensure_market_data(symbols, window)

    prices = price_store.price_frame(window)
    if prices.empty:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Insufficient price history to compute correlations for the requested window.",
        )

    returns = _returns_from_prices(prices)
    if returns.empty or len(returns) < 1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Unable to compute returns for the requested window.",
        )

    correlation_matrix = returns.corr()
    timestamp = datetime.now(timezone.utc)

    _persist_matrix(session, timestamp=timestamp, matrix=correlation_matrix)
    _trigger_breakdown_alert(correlation_matrix)

    nested = _matrix_to_nested_dict(correlation_matrix)
    return CorrelationMatrixResponse(
        timestamp=timestamp,
        window=window,
        symbols=list(correlation_matrix.columns),
        matrix=nested,
    )


__all__ = [
    "app",
    "correlation_matrix",
    "CorrelationMatrixResponse",
    "price_store",
    "_reset_marketdata_engine",
]
