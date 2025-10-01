"""Rolling VWAP analytics service and FastAPI endpoints."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from statistics import pstdev

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    select,
)
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


LOGGER = logging.getLogger(__name__)

DEFAULT_DATABASE_URL = "sqlite:///./analytics.db"
DEFAULT_WINDOW_SECONDS = int(os.getenv("VWAP_WINDOW_SECONDS", "300"))


class VWAPComputationError(RuntimeError):
    """Raised when VWAP calculations cannot be performed."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(slots=True)
class TradeSample:
    ts: datetime
    price: float
    volume: float


class VWAPDivergenceResponse(BaseModel):
    """Response payload returned by the VWAP divergence endpoint."""

    symbol: str = Field(..., description="Instrument symbol for the computed VWAP")
    vwap: float = Field(..., description="Rolling volume-weighted average price")
    current_price: float = Field(..., description="Most recent trade price within the window")
    divergence_pct: float = Field(
        ...,
        description="Percentage difference between the current price and the VWAP",
        example=1.2,
    )
    std_dev_pct: float = Field(
        ...,
        description="Standard deviation of divergences within the window expressed as a percentage",
    )
    overextended: bool = Field(
        ...,
        description="Whether the current price is more than two standard deviations away from the VWAP",
    )
    window_start: datetime = Field(..., description="Start timestamp of the rolling VWAP window (UTC)")
    window_end: datetime = Field(..., description="End timestamp of the rolling VWAP window (UTC)")


class VWAPAnalyticsService:
    """Provides rolling VWAP calculations backed by TimescaleDB tables."""

    def __init__(
        self,
        *,
        engine: Engine | None = None,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
        schema: str | None = None,
    ) -> None:
        self._engine = engine or create_engine(self._database_url(), future=True, pool_pre_ping=True)
        self._schema = self._resolve_schema(self._engine, schema)
        self._window = timedelta(seconds=max(1, window_seconds))

        market_metadata = MetaData(schema=self._schema)
        self._bars = Table(
            "bars",
            market_metadata,
            Column("symbol", String, nullable=False),
            Column("ts", DateTime(timezone=True), nullable=False),
            Column("close", Float, nullable=False),
            Column("volume", Float, nullable=False),
        )

        metrics_metadata = MetaData(schema=self._schema)
        self._metrics = Table(
            "vwap_metrics",
            metrics_metadata,
            Column("symbol", String, nullable=False),
            Column("ts", DateTime(timezone=True), nullable=False),
            Column("vwap", Float, nullable=False),
            Column("divergence_pct", Float, nullable=False),
            PrimaryKeyConstraint("symbol", "ts", name="pk_vwap_metrics"),
        )
        metrics_metadata.create_all(self._engine, checkfirst=True)

    @staticmethod
    def _database_url() -> str:
        url = os.getenv("TIMESCALE_DATABASE_URI") or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+psycopg2://", 1)
        return url

    @staticmethod
    def _resolve_schema(engine: Engine, requested: str | None) -> str | None:
        if requested:
            return requested
        if engine.dialect.name == "sqlite":
            return None
        return os.getenv("VWAP_SCHEMA") or os.getenv("TIMESCALE_SCHEMA") or "public"

    def compute(self, symbol: str) -> VWAPDivergenceResponse:
        if not symbol:
            raise VWAPComputationError("Symbol must be provided", status_code=422)

        window_end = datetime.now(timezone.utc)
        window_start = window_end - self._window
        samples = self._load_samples(symbol, window_start, window_end)
        if not samples:
            raise VWAPComputationError(
                f"No market data found for symbol '{symbol}'", status_code=404
            )

        total_volume = sum(sample.volume for sample in samples if sample.volume > 0)
        if total_volume <= 0:
            raise VWAPComputationError("Insufficient volume to compute VWAP", status_code=422)

        weighted_price = sum(sample.price * sample.volume for sample in samples if sample.volume > 0)
        vwap = weighted_price / total_volume

        price_series = [sample.price for sample in samples if sample.volume > 0]
        if not price_series:
            raise VWAPComputationError("No valid price samples available", status_code=422)

        divergences = [((price - vwap) / vwap) if vwap else 0.0 for price in price_series]
        current_price = price_series[-1]
        current_divergence = divergences[-1]
        std_dev = pstdev(divergences) if len(divergences) > 1 else 0.0
        overextended = abs(current_divergence) > (2 * std_dev) if std_dev > 0 else False

        divergence_pct = current_divergence * 100.0
        std_dev_pct = std_dev * 100.0

        self._persist_metric(symbol, vwap, divergence_pct, window_end)

        return VWAPDivergenceResponse(
            symbol=symbol,
            vwap=vwap,
            current_price=current_price,
            divergence_pct=divergence_pct,
            std_dev_pct=std_dev_pct,
            overextended=overextended,
            window_start=window_start,
            window_end=window_end,
        )

    def _load_samples(self, symbol: str, start: datetime, end: datetime) -> list[TradeSample]:
        stmt = (
            select(self._bars.c.ts, self._bars.c.close, self._bars.c.volume)
            .where(self._bars.c.symbol == symbol)
            .where(self._bars.c.ts >= start)
            .where(self._bars.c.ts <= end)
            .order_by(self._bars.c.ts.asc())
        )
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).all()
        except SQLAlchemyError as exc:
            LOGGER.exception("Failed to load market data for %s", symbol)
            raise VWAPComputationError(
                "Database error while loading market data", status_code=500
            ) from exc

        samples: list[TradeSample] = []
        for ts, price, volume in rows:
            if ts is None:
                continue
            price_f = _to_float(price)
            volume_f = _to_float(volume)
            samples.append(TradeSample(ts=ts, price=price_f, volume=volume_f))
        return samples

    def _persist_metric(self, symbol: str, vwap: float, divergence_pct: float, ts: datetime) -> None:
        payload = {"symbol": symbol, "ts": ts, "vwap": vwap, "divergence_pct": divergence_pct}
        try:
            with self._engine.begin() as conn:
                conn.execute(self._metrics.insert(), [payload])
        except SQLAlchemyError:
            LOGGER.exception("Failed to persist VWAP metric for %s", symbol)


def _to_float(value: float | int | Decimal | None) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (float, int)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


router = APIRouter(prefix="/vwap", tags=["analytics"])


@lru_cache(maxsize=1)
def get_service() -> VWAPAnalyticsService:
    """Provide a cached instance of :class:`VWAPAnalyticsService`."""

    return VWAPAnalyticsService()


@router.get("/divergence", response_model=VWAPDivergenceResponse)
def vwap_divergence(
    *, symbol: str = Query(..., description="Symbol to compute VWAP divergence for"), service: VWAPAnalyticsService = Depends(get_service)
) -> VWAPDivergenceResponse:
    try:
        return service.compute(symbol)
    except VWAPComputationError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - unexpected defensive guard
        LOGGER.exception("Unhandled error while computing VWAP divergence for %s", symbol)
        raise HTTPException(status_code=500, detail="Failed to compute VWAP divergence") from exc


__all__ = ["router", "VWAPAnalyticsService", "VWAPDivergenceResponse", "vwap_divergence"]
