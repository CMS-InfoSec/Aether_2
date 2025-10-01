"""Transaction cost analysis (TCA) service exposed via FastAPI.

This module is intentionally self-contained so the service can be deployed as a
small microservice beside the other operational APIs.  It provides two
endpoints:

* ``GET /tca/trade`` – materialises a detailed report for an individual trade
  (identified by ``trade_id``).
* ``GET /tca/summary`` – aggregates the realised slippage profile for an
  account across a single trading day.

The service is backed by TimescaleDB (or any PostgreSQL compatible database)
using SQLAlchemy for the ORM layer.  Whenever a trade report is generated the
volume weighted slippage (in basis points) and the fees are persisted to the
``tca_results`` table so that downstream systems can reuse the normalised data
set.
"""

from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, Float, MetaData, String, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool


LOGGER = logging.getLogger(__name__)


DEFAULT_DATABASE_URL = "sqlite:///./tca.db"


def _database_url() -> str:
    """Resolve the database connection string used by the service."""

    url = (
        os.getenv("TCA_DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )
    if url.startswith("postgresql://"):
        # SQLAlchemy expects the ``psycopg2`` driver for historic reasons.
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def _engine_options(url: str) -> dict[str, Any]:
    options: dict[str, Any] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


_DB_URL = _database_url()
ENGINE: Engine = create_engine(_DB_URL, **_engine_options(_DB_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)


Base = declarative_base(metadata=MetaData())


class TCAResult(Base):
    """Persistence model storing derived per-trade TCA metrics."""

    __tablename__ = "tca_results"

    account_id = Column(String, primary_key=True)
    trade_id = Column(String, primary_key=True)
    slippage_bps = Column(Float, nullable=False)
    fees_usd = Column(Float, nullable=True)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))


Base.metadata.create_all(bind=ENGINE)


def _ensure_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _coerce_decimal(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalise_metadata(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, Mapping):
            return dict(parsed)
    return {}


def _mid_price_from_metadata(*payloads: Mapping[str, Any]) -> float | None:
    for payload in payloads:
        mid = payload.get("mid_price_at_submit")
        if mid is None:
            continue
        try:
            return float(mid)
        except (TypeError, ValueError):  # pragma: no cover - defensive guard
            continue
    return None


def _liquidity_flag(*payloads: Mapping[str, Any]) -> str | None:
    candidates = ("liquidity", "liquidity_type", "maker_taker")
    for payload in payloads:
        for key in candidates:
            value = payload.get(key)
            if value is None:
                continue
            text_value = str(value).lower()
            if "maker" in text_value:
                return "maker"
            if "taker" in text_value:
                return "taker"
    maker_flag = payloads[0].get("is_maker") if payloads else None
    if isinstance(maker_flag, bool):
        return "maker" if maker_flag else "taker"
    return None


def _slippage_bps(fill_price: float, mid_price: float) -> float:
    if mid_price == 0:
        return 0.0
    return ((fill_price - mid_price) / mid_price) * 10_000.0


def _daterange_bounds(target_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(target_date, time.min, tzinfo=UTC)
    end = start + timedelta(days=1)
    return start, end


@dataclass
class FillMetrics:
    fill_id: str
    fill_time: datetime
    fill_price: float
    size: float
    fee: float
    mid_price: float | None
    slippage_bps: float
    notional: float
    liquidity: str | None


class FillMetricsModel(BaseModel):
    fill_id: str
    fill_time: datetime
    fill_price: float
    size: float
    fee: float
    mid_price_at_submit: float | None
    slippage_bps: float
    notional_usd: float
    liquidity: str | None = Field(None, description="Maker or taker attribution if available")

    class Config:
        json_encoders = {datetime: lambda value: value.isoformat()}


class TradeReportModel(BaseModel):
    trade_id: str
    account_id: str
    market: str
    submitted_at: datetime
    average_slippage_bps: float
    total_slippage_cost_usd: float
    fees_usd: float
    maker_ratio: float
    taker_ratio: float
    fills: list[FillMetricsModel]


class DailySummaryModel(BaseModel):
    account_id: str
    date: date
    avg_slippage_bps: float
    total_cost_usd: float
    maker_ratio: float
    taker_ratio: float
    fee_attribution: dict[str, float]
    trade_count: int


app = FastAPI(title="TCA Service", version="1.0.0")


def _persist_result(
    session: Session,
    *,
    account_id: str,
    trade_id: str,
    slippage_bps: float,
    fees_usd: float,
) -> None:
    record = session.get(TCAResult, (account_id, trade_id))
    if record is None:
        record = TCAResult(
            account_id=account_id,
            trade_id=trade_id,
            slippage_bps=slippage_bps,
            fees_usd=fees_usd,
            ts=datetime.now(UTC),
        )
        session.add(record)
    else:
        record.slippage_bps = slippage_bps
        record.fees_usd = fees_usd
        record.ts = datetime.now(UTC)


def _fetch_trade_rows(session: Session, trade_id: str) -> list[Mapping[str, Any]]:
    query = text(
        """
        SELECT
            o.order_id AS trade_id,
            o.account_id,
            o.market,
            o.submitted_at,
            o.metadata AS order_metadata,
            f.fill_id,
            f.fill_time,
            f.price AS fill_price,
            f.size,
            f.fee,
            f.metadata AS fill_metadata
        FROM orders o
        JOIN fills f ON f.order_id = o.order_id
        WHERE o.order_id = :trade_id
        ORDER BY f.fill_time ASC
        """
    )
    result = session.execute(query, {"trade_id": trade_id})
    return [dict(row._mapping) for row in result]


def _build_fill_metrics(rows: Iterable[Mapping[str, Any]]) -> tuple[list[FillMetrics], Mapping[str, Any]]:
    fills: list[FillMetrics] = []
    order_info: MutableMapping[str, Any] | None = None
    for row in rows:
        if order_info is None:
            order_info = {
                "trade_id": row.get("trade_id"),
                "account_id": row.get("account_id"),
                "market": row.get("market"),
                "submitted_at": _ensure_datetime(row.get("submitted_at")),
                "order_metadata": _normalise_metadata(row.get("order_metadata")),
            }
        fill_metadata = _normalise_metadata(row.get("fill_metadata"))
        order_metadata = order_info.get("order_metadata", {}) if order_info else {}
        mid_price = _mid_price_from_metadata(fill_metadata, order_metadata) if order_info else None
        fill_price = _coerce_decimal(row.get("fill_price"))
        size = _coerce_decimal(row.get("size"))
        fee = _coerce_decimal(row.get("fee"))
        slippage = _slippage_bps(fill_price, mid_price) if mid_price is not None else 0.0
        notional = fill_price * size
        liquidity = _liquidity_flag(fill_metadata, order_metadata)
        fills.append(
            FillMetrics(
                fill_id=str(row.get("fill_id")),
                fill_time=_ensure_datetime(row.get("fill_time")) or datetime.now(UTC),
                fill_price=fill_price,
                size=size,
                fee=fee,
                mid_price=mid_price,
                slippage_bps=slippage,
                notional=notional,
                liquidity=liquidity,
            )
        )
    if order_info is None:
        return [], {}
    return fills, order_info


def _aggregate_trade(
    fills: Sequence[FillMetrics],
) -> tuple[float, float, float, float, float]:
    if not fills:
        return 0.0, 0.0, 0.0, 0.0, 0.0

    total_size = sum(fill.size for fill in fills if fill.size)
    total_notional = sum(fill.notional for fill in fills)
    total_cost = sum((fill.fill_price - (fill.mid_price or fill.fill_price)) * fill.size for fill in fills)
    total_fees = sum(fill.fee for fill in fills)

    if total_size == 0:
        avg_slippage = 0.0
    else:
        weighted = sum(fill.slippage_bps * fill.size for fill in fills)
        avg_slippage = weighted / total_size

    maker_notional = sum(fill.notional for fill in fills if fill.liquidity == "maker")
    taker_notional = sum(fill.notional for fill in fills if fill.liquidity == "taker")

    if total_notional:
        maker_ratio = maker_notional / total_notional
        taker_ratio = taker_notional / total_notional
    else:
        maker_ratio = taker_ratio = 0.0

    return avg_slippage, total_cost, total_fees, maker_ratio, taker_ratio


def _daily_summary(
    session: Session,
    *,
    account_id: str,
    start: datetime,
    end: datetime,
) -> DailySummaryModel:
    query = text(
        """
        SELECT
            o.order_id AS trade_id,
            o.account_id,
            o.market,
            o.submitted_at,
            o.metadata AS order_metadata,
            f.fill_id,
            f.fill_time,
            f.price AS fill_price,
            f.size,
            f.fee,
            f.metadata AS fill_metadata
        FROM orders o
        JOIN fills f ON f.order_id = o.order_id
        WHERE o.account_id = :account_id
          AND f.fill_time >= :start_ts
          AND f.fill_time < :end_ts
        ORDER BY o.order_id, f.fill_time
        """
    )

    rows = session.execute(
        query,
        {"account_id": account_id, "start_ts": start, "end_ts": end},
    )

    fills_by_trade: dict[str, list[FillMetrics]] = defaultdict(list)
    orders: dict[str, Mapping[str, Any]] = {}

    for row in rows:
        mapping = dict(row._mapping)
        trade_id = str(mapping.get("trade_id"))
        fill_metadata = _normalise_metadata(mapping.get("fill_metadata"))
        order_metadata = _normalise_metadata(mapping.get("order_metadata"))
        mid_price = _mid_price_from_metadata(fill_metadata, order_metadata)
        fill_price = _coerce_decimal(mapping.get("fill_price"))
        size = _coerce_decimal(mapping.get("size"))
        fee = _coerce_decimal(mapping.get("fee"))
        slippage = _slippage_bps(fill_price, mid_price) if mid_price is not None else 0.0
        notional = fill_price * size
        liquidity = _liquidity_flag(fill_metadata, order_metadata)
        fills_by_trade[trade_id].append(
            FillMetrics(
                fill_id=str(mapping.get("fill_id")),
                fill_time=_ensure_datetime(mapping.get("fill_time")) or datetime.now(UTC),
                fill_price=fill_price,
                size=size,
                fee=fee,
                mid_price=mid_price,
                slippage_bps=slippage,
                notional=notional,
                liquidity=liquidity,
            )
        )
        if trade_id not in orders:
            orders[trade_id] = {
                "trade_id": trade_id,
                "account_id": mapping.get("account_id"),
                "submitted_at": mapping.get("submitted_at"),
            }

    if not fills_by_trade:
        raise HTTPException(status_code=404, detail="No fills found for account and date")

    total_slippage_weighted = 0.0
    total_size = 0.0
    total_cost = 0.0
    total_fees = 0.0
    total_notional = 0.0
    maker_notional = 0.0
    taker_notional = 0.0
    maker_fees = 0.0
    taker_fees = 0.0

    for trade_id, fills in fills_by_trade.items():
        for fill in fills:
            total_slippage_weighted += fill.slippage_bps * fill.size
            total_size += fill.size
            total_cost += (fill.fill_price - (fill.mid_price or fill.fill_price)) * fill.size
            total_fees += fill.fee
            total_notional += fill.notional
            if fill.liquidity == "maker":
                maker_notional += fill.notional
                maker_fees += fill.fee
            elif fill.liquidity == "taker":
                taker_notional += fill.notional
                taker_fees += fill.fee

    avg_slippage = total_slippage_weighted / total_size if total_size else 0.0
    maker_ratio = maker_notional / total_notional if total_notional else 0.0
    taker_ratio = taker_notional / total_notional if total_notional else 0.0

    fee_attribution = {
        "total_fees_usd": total_fees,
        "maker_fees_usd": maker_fees,
        "taker_fees_usd": taker_fees,
    }

    return DailySummaryModel(
        account_id=account_id,
        date=start.date(),
        avg_slippage_bps=avg_slippage,
        total_cost_usd=total_cost,
        maker_ratio=maker_ratio,
        taker_ratio=taker_ratio,
        fee_attribution=fee_attribution,
        trade_count=len(fills_by_trade),
    )


@app.get("/tca/trade", response_model=TradeReportModel)
def get_trade_report(trade_id: str = Query(..., description="Unique identifier for the trade/order")):
    with SessionLocal() as session:
        try:
            rows = _fetch_trade_rows(session, trade_id)
        except SQLAlchemyError as exc:  # pragma: no cover - defensive guard
            LOGGER.exception("Database query failed for trade_id=%s", trade_id)
            raise HTTPException(status_code=500, detail="Database error") from exc

        fills, order_info = _build_fill_metrics(rows)
        if not fills or not order_info:
            raise HTTPException(status_code=404, detail="Trade not found")

        avg_slippage, total_cost, total_fees, maker_ratio, taker_ratio = _aggregate_trade(fills)

        _persist_result(
            session,
            account_id=str(order_info["account_id"]),
            trade_id=str(order_info["trade_id"]),
            slippage_bps=avg_slippage,
            fees_usd=total_fees,
        )
        session.commit()

        response = TradeReportModel(
            trade_id=str(order_info["trade_id"]),
            account_id=str(order_info["account_id"]),
            market=str(order_info["market"]),
            submitted_at=order_info["submitted_at"],
            average_slippage_bps=avg_slippage,
            total_slippage_cost_usd=total_cost,
            fees_usd=total_fees,
            maker_ratio=maker_ratio,
            taker_ratio=taker_ratio,
            fills=[
                FillMetricsModel(
                    fill_id=fill.fill_id,
                    fill_time=fill.fill_time,
                    fill_price=fill.fill_price,
                    size=fill.size,
                    fee=fill.fee,
                    mid_price_at_submit=fill.mid_price,
                    slippage_bps=fill.slippage_bps,
                    notional_usd=fill.notional,
                    liquidity=fill.liquidity,
                )
                for fill in fills
            ],
        )
        return response


@app.get("/tca/summary", response_model=DailySummaryModel)
def get_daily_summary(
    account_id: str = Query(..., description="Account identifier"),
    date_str: str | None = Query(None, alias="date", description="Trading day in ISO format"),
):
    target_date = date.fromisoformat(date_str) if date_str else datetime.now(UTC).date()
    start, end = _daterange_bounds(target_date)

    with SessionLocal() as session:
        try:
            summary = _daily_summary(session, account_id=account_id, start=start, end=end)
        except SQLAlchemyError as exc:  # pragma: no cover - defensive guard
            LOGGER.exception("Failed to compute TCA summary for account=%s", account_id)
            raise HTTPException(status_code=500, detail="Database error") from exc
    return summary


__all__ = ["app"]

