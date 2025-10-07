"""Transaction cost analysis (TCA) service exposed via FastAPI.

This module is intentionally self-contained so the service can be deployed as a
small microservice beside the other operational APIs.  It provides several
endpoints and helper utilities:

* ``GET /tca/trade`` – materialises a detailed report for an individual trade
  (identified by ``trade_id``).
* ``GET /tca/summary`` – aggregates the realised slippage profile for an
  account across a single trading day.
* ``GET /tca/report`` – compares the expected execution profile with the
  realised outcome for an account/symbol on a specific trading day.

The service is backed by TimescaleDB (or any PostgreSQL compatible database)
using SQLAlchemy for the ORM layer.  Whenever a trade report is generated the
volume weighted slippage (in basis points) and the fees are persisted to the
``tca_results`` table so that downstream systems can reuse the normalised data
set.  Daily expected-vs-realised comparisons are stored in ``tca_reports`` so
that risk directors can review the health of the execution programme.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Sequence

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, MetaData, Numeric, String, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from services.common.security import require_admin_account
from services.common.spot import require_spot_http
from shared.audit_hooks import load_audit_hooks, log_event_with_fallback
from shared.postgres import normalize_sqlalchemy_dsn
from shared.spot import is_spot_symbol, normalize_spot_symbol


LOGGER = logging.getLogger(__name__)
AUDIT_LOGGER = logging.getLogger("tca.audit")


DEFAULT_DATABASE_URL = "sqlite:///./tca.db"


DECIMAL_ZERO = Decimal("0")
DECIMAL_EIGHT_DP = Decimal("0.00000001")
DECIMAL_FOUR_DP = Decimal("0.0001")

PRICE_QUANT = DECIMAL_EIGHT_DP
SIZE_QUANT = DECIMAL_EIGHT_DP
FEE_QUANT = DECIMAL_EIGHT_DP
NOTIONAL_QUANT = DECIMAL_EIGHT_DP
USD_QUANT = DECIMAL_EIGHT_DP
BPS_QUANT = DECIMAL_FOUR_DP
RATIO_QUANT = DECIMAL_FOUR_DP
BPS_FACTOR = Decimal("10000")


def _database_url() -> str:
    """Resolve the database connection string used by the service."""

    allow_sqlite = "pytest" in sys.modules
    raw = (
        os.getenv("TCA_DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or (DEFAULT_DATABASE_URL if allow_sqlite else None)
    )

    if raw is None:
        raise RuntimeError(
            "TCA database DSN is not configured. Set TCA_DATABASE_URL or TIMESCALE_DSN "
            "to a PostgreSQL/Timescale connection string.",
        )

    candidate = raw.strip()
    if not candidate:
        raise RuntimeError("TCA database DSN cannot be empty once configured.")

    return normalize_sqlalchemy_dsn(
        candidate,
        allow_sqlite=allow_sqlite,
        label="TCA database DSN",
    )


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
    slippage_bps = Column(Numeric(24, 12), nullable=False)
    fees_usd = Column(Numeric(24, 12), nullable=True)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))


class TCAReport(Base):
    """Persistence model for daily expected-vs-realised execution reports."""

    __tablename__ = "tca_reports"

    account_id = Column(String, primary_key=True)
    symbol = Column(String, primary_key=True)
    ts = Column(DateTime(timezone=True), primary_key=True, default=lambda: datetime.now(UTC))
    expected_cost = Column(Numeric(24, 12), nullable=False)
    realized_cost = Column(Numeric(24, 12), nullable=False)
    slippage_bps = Column(Numeric(24, 12), nullable=False)


Base.metadata.create_all(bind=ENGINE)


def _audit_access(
    request: Request,
    actor: str,
    *,
    action: str,
    entity: str,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    """Record audit information for report access using the verified identity."""

    ip_address = request.client.host if request.client else None
    audit_hooks = load_audit_hooks()
    log_event_with_fallback(
        audit_hooks,
        AUDIT_LOGGER,
        actor=actor,
        action=action,
        entity=entity,
        before={},
        after=dict(metadata or {}),
        ip_address=ip_address,
        failure_message=f"Failed to write audit log for action={action} entity={entity}",
        disabled_message=f"Audit logging disabled; skipping {action} for {entity}",
    )


def _ensure_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _extract_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _coerce_decimal(value: Any, *, default: Decimal = DECIMAL_ZERO) -> Decimal:
    extracted = _extract_decimal(value)
    if extracted is None:
        return default
    return extracted


def _quantize(value: Decimal, quantum: Decimal) -> Decimal:
    return value.quantize(quantum, rounding=ROUND_HALF_UP)


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


def _mid_price_from_metadata(*payloads: Mapping[str, Any]) -> Decimal | None:
    for payload in payloads:
        mid = payload.get("mid_price_at_submit")
        if mid is None:
            continue
        value = _extract_decimal(mid)
        if value is not None:
            return value
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


def _trade_direction(size: float, *payloads: Mapping[str, Any]) -> int:
    """Infer trade direction from metadata falling back to fill size."""

    for payload in payloads:
        side = payload.get("side") or payload.get("trade_side") or payload.get("direction")
        if side is None:
            continue
        text_value = str(side).strip().lower()
        if text_value in {"buy", "bid", "long", "b"}:
            return 1
        if text_value in {"sell", "ask", "short", "s"}:
            return -1
    if size > 0:
        return 1
    if size < 0:
        return -1
    return 1


def _expected_price_from_metadata(*payloads: Mapping[str, Any]) -> Decimal | None:
    """Extract the expected execution price if available."""

    candidate_keys = (
        "expected_price",
        "benchmark_price",
        "target_price",
        "arrival_price",
        "limit_price",
        "reference_price",
    )
    for payload in payloads:
        for key in candidate_keys:
            value = payload.get(key)
            if value is None:
                continue
            price = _extract_decimal(value)
            if price is not None:
                return price
    return _mid_price_from_metadata(*payloads)


def _expected_fee_from_metadata(*payloads: Mapping[str, Any]) -> Decimal:
    """Extract the expected fees if they were estimated upstream."""

    candidate_keys = (
        "expected_fee",
        "expected_fees",
        "fee_estimate",
        "estimated_fee",
        "estimated_fees",
    )
    for payload in payloads:
        for key in candidate_keys:
            value = payload.get(key)
            if value is None:
                continue
            fee = _extract_decimal(value)
            if fee is not None:
                return fee
    return DECIMAL_ZERO


def _slippage_bps(fill_price: Decimal, mid_price: Decimal) -> Decimal:
    if mid_price == DECIMAL_ZERO:
        return DECIMAL_ZERO
    return ((fill_price - mid_price) / mid_price) * BPS_FACTOR


def _daterange_bounds(target_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(target_date, time.min, tzinfo=UTC)
    end = start + timedelta(days=1)
    return start, end


@dataclass
class FillMetrics:
    fill_id: str
    fill_time: datetime
    fill_price: Decimal
    size: Decimal
    fee: Decimal
    mid_price: Decimal | None
    slippage_bps: Decimal
    notional: Decimal
    liquidity: str | None


class FillMetricsModel(BaseModel):
    fill_id: str
    fill_time: datetime
    fill_price: Decimal
    size: Decimal
    fee: Decimal
    mid_price_at_submit: Decimal | None
    slippage_bps: Decimal
    notional_usd: Decimal
    liquidity: str | None = Field(None, description="Maker or taker attribution if available")

    class Config:
        json_encoders = {
            datetime: lambda value: value.isoformat(),
            Decimal: lambda value: format(value, "f"),
        }


class TradeReportModel(BaseModel):
    trade_id: str
    account_id: str
    market: str
    submitted_at: datetime
    average_slippage_bps: Decimal
    total_slippage_cost_usd: Decimal
    fees_usd: Decimal
    maker_ratio: Decimal
    taker_ratio: Decimal
    fills: list[FillMetricsModel]


class DailySummaryModel(BaseModel):
    account_id: str
    date: date
    avg_slippage_bps: Decimal
    total_cost_usd: Decimal
    maker_ratio: Decimal
    taker_ratio: Decimal
    fee_attribution: dict[str, Decimal]
    trade_count: int


class TCAReportModel(BaseModel):
    account_id: str
    symbol: str
    date: date
    expected_cost_usd: Decimal
    realized_cost_usd: Decimal
    slippage_bps: Decimal
    slippage_cost_usd: Decimal
    fill_quality_bps: Decimal
    fee_impact_usd: Decimal
    trade_count: int

    class Config:
        json_encoders = {
            datetime: lambda value: value.isoformat(),
            Decimal: lambda value: format(value, "f"),
        }


@dataclass
class ExpectedVsRealised:
    expected_cost: Decimal
    realized_cost: Decimal
    slippage_bps: Decimal
    slippage_cost_usd: Decimal
    fill_quality_bps: Decimal
    fee_impact_usd: Decimal
    trade_count: int


app = FastAPI(title="TCA Service", version="1.0.0")


def _serialize_fill(fill: FillMetrics) -> FillMetricsModel:
    return FillMetricsModel(
        fill_id=fill.fill_id,
        fill_time=fill.fill_time,
        fill_price=_quantize(fill.fill_price, PRICE_QUANT),
        size=_quantize(fill.size, SIZE_QUANT),
        fee=_quantize(fill.fee, FEE_QUANT),
        mid_price_at_submit=(
            _quantize(fill.mid_price, PRICE_QUANT) if fill.mid_price is not None else None
        ),
        slippage_bps=_quantize(fill.slippage_bps, BPS_QUANT),
        notional_usd=_quantize(fill.notional, NOTIONAL_QUANT),
        liquidity=fill.liquidity,
    )


def _persist_result(
    session: Session,
    *,
    account_id: str,
    trade_id: str,
    slippage_bps: Decimal,
    fees_usd: Decimal,
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
        slippage = _slippage_bps(fill_price, mid_price) if mid_price is not None else DECIMAL_ZERO
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


def _compare_expected_realised(rows: Sequence[Mapping[str, Any]]) -> ExpectedVsRealised:
    if not rows:
        return ExpectedVsRealised(
            expected_cost=DECIMAL_ZERO,
            realized_cost=DECIMAL_ZERO,
            slippage_bps=DECIMAL_ZERO,
            slippage_cost_usd=DECIMAL_ZERO,
            fill_quality_bps=DECIMAL_ZERO,
            fee_impact_usd=DECIMAL_ZERO,
            trade_count=0,
        )

    expected_cost_total = DECIMAL_ZERO
    realized_cost_total = DECIMAL_ZERO
    weighted_slippage = DECIMAL_ZERO
    weighted_quality = DECIMAL_ZERO
    slippage_cost_total = DECIMAL_ZERO
    fee_impact_total = DECIMAL_ZERO
    total_size = DECIMAL_ZERO
    seen_trades: set[str] = set()

    for row in rows:
        trade_id = str(row.get("trade_id"))
        seen_trades.add(trade_id)

        order_metadata = _normalise_metadata(row.get("order_metadata"))
        fill_metadata = _normalise_metadata(row.get("fill_metadata"))

        fill_price = _coerce_decimal(row.get("fill_price"))
        size = _coerce_decimal(row.get("size"))
        abs_size = abs(size)
        if abs_size == DECIMAL_ZERO:
            continue

        expected_price = _expected_price_from_metadata(fill_metadata, order_metadata)
        if expected_price is None or expected_price == DECIMAL_ZERO:
            expected_price = fill_price

        expected_fee = _expected_fee_from_metadata(fill_metadata, order_metadata)
        realized_fee = _coerce_decimal(row.get("fee"))
        direction = _trade_direction(size, fill_metadata, order_metadata)

        expected_notional = expected_price * abs_size
        realized_notional = fill_price * abs_size

        expected_cost_total += expected_notional + expected_fee
        realized_cost_total += realized_notional + realized_fee

        if expected_price != DECIMAL_ZERO:
            slippage_bps = ((fill_price - expected_price) * direction / expected_price) * BPS_FACTOR
        else:
            slippage_bps = DECIMAL_ZERO
        slippage_cost = (fill_price - expected_price) * abs_size * direction
        fee_impact = realized_fee - expected_fee

        weighted_slippage += slippage_bps * abs_size
        weighted_quality += (-slippage_bps) * abs_size
        slippage_cost_total += slippage_cost
        fee_impact_total += fee_impact
        total_size += abs_size

    avg_slippage = weighted_slippage / total_size if total_size else DECIMAL_ZERO
    avg_quality = weighted_quality / total_size if total_size else DECIMAL_ZERO

    return ExpectedVsRealised(
        expected_cost=expected_cost_total,
        realized_cost=realized_cost_total,
        slippage_bps=avg_slippage,
        slippage_cost_usd=slippage_cost_total,
        fill_quality_bps=avg_quality,
        fee_impact_usd=fee_impact_total,
        trade_count=len(seen_trades),
    )


def _aggregate_trade(
    fills: Sequence[FillMetrics],
) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
    if not fills:
        return (
            DECIMAL_ZERO,
            DECIMAL_ZERO,
            DECIMAL_ZERO,
            DECIMAL_ZERO,
            DECIMAL_ZERO,
        )

    total_size = DECIMAL_ZERO
    total_notional = DECIMAL_ZERO
    total_cost = DECIMAL_ZERO
    total_fees = DECIMAL_ZERO
    weighted = DECIMAL_ZERO
    maker_notional = DECIMAL_ZERO
    taker_notional = DECIMAL_ZERO

    for fill in fills:
        if fill.size != DECIMAL_ZERO:
            total_size += fill.size
            weighted += fill.slippage_bps * fill.size
        total_notional += fill.notional
        reference_price = fill.mid_price or fill.fill_price
        total_cost += (fill.fill_price - reference_price) * fill.size
        total_fees += fill.fee
        if fill.liquidity == "maker":
            maker_notional += fill.notional
        elif fill.liquidity == "taker":
            taker_notional += fill.notional

    if total_size == DECIMAL_ZERO:
        avg_slippage = DECIMAL_ZERO
    else:
        avg_slippage = weighted / total_size

    if total_notional != DECIMAL_ZERO:
        maker_ratio = maker_notional / total_notional
        taker_ratio = taker_notional / total_notional
    else:
        maker_ratio = taker_ratio = DECIMAL_ZERO

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

    total_slippage_weighted = DECIMAL_ZERO
    total_size = DECIMAL_ZERO
    total_cost = DECIMAL_ZERO
    total_fees = DECIMAL_ZERO
    total_notional = DECIMAL_ZERO
    maker_notional = DECIMAL_ZERO
    taker_notional = DECIMAL_ZERO
    maker_fees = DECIMAL_ZERO
    taker_fees = DECIMAL_ZERO

    for trade_id, fills in fills_by_trade.items():
        for fill in fills:
            total_slippage_weighted += fill.slippage_bps * fill.size
            total_size += fill.size
            reference_price = fill.mid_price or fill.fill_price
            total_cost += (fill.fill_price - reference_price) * fill.size
            total_fees += fill.fee
            total_notional += fill.notional
            if fill.liquidity == "maker":
                maker_notional += fill.notional
                maker_fees += fill.fee
            elif fill.liquidity == "taker":
                taker_notional += fill.notional
                taker_fees += fill.fee

    avg_slippage = total_slippage_weighted / total_size if total_size else DECIMAL_ZERO
    maker_ratio = maker_notional / total_notional if total_notional else DECIMAL_ZERO
    taker_ratio = taker_notional / total_notional if total_notional else DECIMAL_ZERO

    fee_attribution = {
        "total_fees_usd": total_fees,
        "maker_fees_usd": maker_fees,
        "taker_fees_usd": taker_fees,
    }

    return DailySummaryModel(
        account_id=account_id,
        date=start.date(),
        avg_slippage_bps=_quantize(avg_slippage, BPS_QUANT),
        total_cost_usd=_quantize(total_cost, USD_QUANT),
        maker_ratio=_quantize(maker_ratio, RATIO_QUANT),
        taker_ratio=_quantize(taker_ratio, RATIO_QUANT),
        fee_attribution={
            key: _quantize(value, USD_QUANT)
            for key, value in fee_attribution.items()
        },
        trade_count=len(fills_by_trade),
    )


def _persist_report(
    session: Session,
    *,
    account_id: str,
    symbol: str,
    expected_cost: Decimal,
    realized_cost: Decimal,
    slippage_bps: Decimal,
) -> None:
    session.add(
        TCAReport(
            account_id=account_id,
            symbol=symbol,
            expected_cost=expected_cost,
            realized_cost=realized_cost,
            slippage_bps=slippage_bps,
            ts=datetime.now(UTC),
        )
    )


def _fetch_execution_rows(
    session: Session,
    *,
    account_id: str,
    symbol: str,
    start: datetime,
    end: datetime,
) -> list[Mapping[str, Any]]:
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
          AND o.market = :symbol
          AND f.fill_time >= :start_ts
          AND f.fill_time < :end_ts
        ORDER BY f.fill_time
        """
    )
    result = session.execute(
        query,
        {"account_id": account_id, "symbol": symbol, "start_ts": start, "end_ts": end},
    )
    return [dict(row._mapping) for row in result]


def _build_report_response(
    *,
    account_id: str,
    symbol: str,
    target_date: date,
    metrics: ExpectedVsRealised,
) -> TCAReportModel:
    return TCAReportModel(
        account_id=account_id,
        symbol=symbol,
        date=target_date,
        expected_cost_usd=_quantize(metrics.expected_cost, USD_QUANT),
        realized_cost_usd=_quantize(metrics.realized_cost, USD_QUANT),
        slippage_bps=_quantize(metrics.slippage_bps, BPS_QUANT),
        slippage_cost_usd=_quantize(metrics.slippage_cost_usd, USD_QUANT),
        fill_quality_bps=_quantize(metrics.fill_quality_bps, BPS_QUANT),
        fee_impact_usd=_quantize(metrics.fee_impact_usd, USD_QUANT),
        trade_count=metrics.trade_count,
    )


@app.get("/tca/trade", response_model=TradeReportModel)
def get_trade_report(
    request: Request,
    trade_id: str = Query(..., description="Unique identifier for the trade/order"),
    actor_account: str = Depends(require_admin_account),
):
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
            average_slippage_bps=_quantize(avg_slippage, BPS_QUANT),
            total_slippage_cost_usd=_quantize(total_cost, USD_QUANT),
            fees_usd=_quantize(total_fees, USD_QUANT),
            maker_ratio=_quantize(maker_ratio, RATIO_QUANT),
            taker_ratio=_quantize(taker_ratio, RATIO_QUANT),
            fills=[_serialize_fill(fill) for fill in fills],
        )
        _audit_access(
            request,
            actor_account,
            action="tca.trade.report",
            entity=str(order_info["trade_id"]),
            metadata={
                "account_id": str(order_info["account_id"]),
                "market": str(order_info["market"]),
            },
        )
        return response


@app.get("/tca/summary", response_model=DailySummaryModel)
def get_daily_summary(
    request: Request,
    account_id: str = Query(..., description="Account identifier"),
    date_str: str | None = Query(None, alias="date", description="Trading day in ISO format"),
    actor_account: str = Depends(require_admin_account),
):
    target_date = date.fromisoformat(date_str) if date_str else datetime.now(UTC).date()
    start, end = _daterange_bounds(target_date)

    with SessionLocal() as session:
        try:
            summary = _daily_summary(session, account_id=account_id, start=start, end=end)
        except SQLAlchemyError as exc:  # pragma: no cover - defensive guard
            LOGGER.exception("Failed to compute TCA summary for account=%s", account_id)
            raise HTTPException(status_code=500, detail="Database error") from exc
    _audit_access(
        request,
        actor_account,
        action="tca.summary",
        entity=f"{account_id}:{target_date.isoformat()}",
        metadata={"account_id": account_id, "date": target_date.isoformat()},
    )
    return summary


@app.get("/tca/report", response_model=TCAReportModel)
def get_tca_report(
    request: Request,
    account_id: str = Query(..., description="Account identifier"),
    symbol: str = Query(..., description="Market or symbol identifier"),
    date_str: str | None = Query(None, alias="date", description="Trading day in ISO format"),
    actor_account: str = Depends(require_admin_account),
):
    symbol = _require_spot_symbol(symbol)
    target_date = date.fromisoformat(date_str) if date_str else datetime.now(UTC).date()
    start, end = _daterange_bounds(target_date)

    with SessionLocal() as session:
        try:
            rows = _fetch_execution_rows(
                session,
                account_id=account_id,
                symbol=symbol,
                start=start,
                end=end,
            )
        except SQLAlchemyError as exc:  # pragma: no cover - defensive guard
            LOGGER.exception(
                "Failed to fetch execution rows for account=%s symbol=%s", account_id, symbol
            )
            raise HTTPException(status_code=500, detail="Database error") from exc

        if not rows:
            raise HTTPException(status_code=404, detail="No executions found for criteria")

        metrics = _compare_expected_realised(rows)
        if metrics.trade_count == 0:
            raise HTTPException(status_code=404, detail="No executions found for criteria")

        try:
            _persist_report(
                session,
                account_id=account_id,
                symbol=symbol,
                expected_cost=metrics.expected_cost,
                realized_cost=metrics.realized_cost,
                slippage_bps=metrics.slippage_bps,
            )
            session.commit()
        except SQLAlchemyError as exc:  # pragma: no cover - defensive guard
            session.rollback()
            LOGGER.exception(
                "Failed to persist TCA report for account=%s symbol=%s", account_id, symbol
            )
            raise HTTPException(status_code=500, detail="Database error") from exc

    response = _build_report_response(
        account_id=account_id,
        symbol=symbol,
        target_date=target_date,
        metrics=metrics,
    )
    _audit_access(
        request,
        actor_account,
        action="tca.report",
        entity=f"{account_id}:{symbol}:{target_date.isoformat()}",
        metadata={
            "account_id": account_id,
            "symbol": symbol,
            "date": target_date.isoformat(),
            "trade_count": metrics.trade_count,
        },
    )
    return response


def generate_daily_reports(target_date: date | None = None) -> list[TCAReportModel]:
    """Produce daily TCA reports for all account/symbol pairs with executions."""

    target = target_date or (datetime.now(UTC) - timedelta(days=1)).date()
    start, end = _daterange_bounds(target)
    reports: list[TCAReportModel] = []
    seen_pairs: set[tuple[str, str]] = set()

    with SessionLocal() as session:
        try:
            pairs_result = session.execute(
                text(
                    """
                    SELECT DISTINCT
                        o.account_id,
                        o.market AS symbol
                    FROM orders o
                    JOIN fills f ON f.order_id = o.order_id
                    WHERE f.fill_time >= :start_ts
                      AND f.fill_time < :end_ts
                    """
                ),
                {"start_ts": start, "end_ts": end},
            )
        except SQLAlchemyError as exc:  # pragma: no cover - defensive guard
            LOGGER.exception("Failed to enumerate executions for TCA report job")
            raise

        for account_id, raw_symbol in pairs_result:
            normalized_symbol = normalize_spot_symbol(raw_symbol)
            if not normalized_symbol or not is_spot_symbol(normalized_symbol):
                LOGGER.warning(
                    "Skipping non-spot instrument during TCA report generation",
                    extra={"symbol": raw_symbol, "account_id": account_id},
                )
                continue

            pair_key = (account_id, normalized_symbol)
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            try:
                rows = _fetch_execution_rows(
                    session,
                    account_id=account_id,
                    symbol=normalized_symbol,
                    start=start,
                    end=end,
                )
            except SQLAlchemyError as exc:  # pragma: no cover - defensive guard
                LOGGER.exception(
                    "Failed to fetch execution rows for account=%s symbol=%s",
                    account_id,
                    normalized_symbol,
                )
                continue

            if not rows:
                continue

            metrics = _compare_expected_realised(rows)
            if metrics.trade_count == 0:
                continue

            _persist_report(
                session,
                account_id=account_id,
                symbol=normalized_symbol,
                expected_cost=metrics.expected_cost,
                realized_cost=metrics.realized_cost,
                slippage_bps=metrics.slippage_bps,
            )
            reports.append(
                _build_report_response(
                    account_id=account_id,
                    symbol=normalized_symbol,
                    target_date=target,
                    metrics=metrics,
                )
            )

        session.commit()

    return reports


__all__ = ["app", "generate_daily_reports"]

def _require_spot_symbol(symbol: object) -> str:
    """Normalise *symbol* and ensure it references a supported USD spot market."""

    return require_spot_http(symbol, logger=LOGGER)

