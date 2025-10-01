"""Risk correlation service exposing rolling correlation matrices."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Dict, Iterable, Iterator, List

import numpy as np
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
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from services.alert_manager import AlertManager, RiskEvent, get_alert_metrics


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database models and helpers
# ---------------------------------------------------------------------------


DEFAULT_DATABASE_URL = "sqlite:///./risk_correlations.db"


def _database_url() -> str:
    """Return the configured database URL using SQLite as a default."""

    return DEFAULT_DATABASE_URL


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
# Synthetic price history store used for rolling correlations
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
        self._lock = Lock()

    # ------------------------------------------------------------------
    # Mutation helpers
    # ------------------------------------------------------------------
    def record(self, point: PricePoint) -> None:
        with self._lock:
            series = self._prices.setdefault(point.symbol, [])
            series.append(point)
            series.sort(key=lambda entry: entry.timestamp)

    def bulk_record(self, points: Iterable[PricePoint]) -> None:
        for point in points:
            self.record(point)

    def load_synthetic_history(
        self,
        *,
        symbols: Iterable[str],
        periods: int = 240,
        step: timedelta = timedelta(minutes=5),
        seed: int = 13,
    ) -> None:
        """Seed the store with a synthetic but reproducible price series."""

        symbols = list(symbols)
        rng = np.random.default_rng(seed)
        now = datetime.now(timezone.utc)
        start = now - periods * step

        # Create a positive semi-definite covariance matrix with moderate correlation.
        base_cov = np.array(
            [
                [1.0, 0.75, 0.55],
                [0.75, 1.0, 0.45],
                [0.55, 0.45, 1.0],
            ]
        )

        if len(symbols) > 3:
            # Extend the covariance matrix with decaying off-diagonal correlations.
            extra = len(symbols) - 3
            extension = 0.35 * np.ones((extra, extra)) + np.eye(extra) * 0.65
            cross = 0.4 * np.ones((3, extra))
            top = np.concatenate([base_cov, cross], axis=1)
            bottom = np.concatenate([cross.T, extension], axis=1)
            covariance = np.concatenate([top, bottom], axis=0)
        else:
            covariance = base_cov[: len(symbols), : len(symbols)]

        # Scale covariance to represent daily log-return volatility.
        covariance *= 0.0004
        drift = np.full(len(symbols), 0.0005)
        log_returns = rng.multivariate_normal(drift, covariance, size=periods)

        initial_prices = np.linspace(100.0, 150.0, len(symbols))
        timestamps = [start + step * i for i in range(periods)]
        cumulative_returns = np.exp(np.cumsum(log_returns, axis=0))
        price_paths = initial_prices * cumulative_returns

        for idx, symbol in enumerate(symbols):
            for ts, price in zip(timestamps, price_paths[:, idx], strict=False):
                self.record(PricePoint(symbol=symbol, timestamp=ts, price=float(price)))

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
    """Ensure the database is ready and seed synthetic prices."""

    Base.metadata.create_all(bind=ENGINE)
    if not price_store.price_frame(3).empty:
        return

    symbols = ("BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD")
    price_store.load_synthetic_history(symbols=symbols)
    logger.info("Seeded synthetic price history for symbols: %s", ", ".join(symbols))


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
            description=
            f"Correlation between {symbol_a} and {symbol_b} dropped below {threshold:.2f}: {value:.2f}",
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


__all__ = ["app", "correlation_matrix", "CorrelationMatrixResponse"]

