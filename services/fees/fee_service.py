"""FastAPI service exposing the venue fee schedule and volume metrics."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterator, List, Sequence, Tuple

from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from services.common.security import require_admin_account
from services.common.spot import require_spot_http
from services.fees.fee_optimizer import FeeOptimizer
from services.fees.models import AccountFill, AccountVolume30d, Base, FeeTier

POLICY_SERVICE_URL = os.getenv("POLICY_SERVICE_URL", "http://policy-service")
POLICY_VOLUME_SIGNAL_PATH = os.getenv(
    "POLICY_VOLUME_SIGNAL_PATH", "/policy/opportunistic-volume"
)
POLICY_VOLUME_SIGNAL_TIMEOUT = float(os.getenv("POLICY_VOLUME_SIGNAL_TIMEOUT", "0.5"))
NEXT_TIER_ALERT_THRESHOLD = Decimal("0.95")


def _decimal_setting(env_var: str, default: str) -> Decimal:
    """Parse a decimal value from the environment with a safe fallback."""

    try:
        return Decimal(os.getenv(env_var, default))
    except Exception:
        return Decimal(default)


_DEFAULT_FEE_ALPHA = Decimal("0.2")
FEE_ESTIMATE_SMOOTHING_ALPHA = _decimal_setting("FEE_ESTIMATE_SMOOTHING_ALPHA", "0.2")
if FEE_ESTIMATE_SMOOTHING_ALPHA <= 0 or FEE_ESTIMATE_SMOOTHING_ALPHA > Decimal("1"):
    FEE_ESTIMATE_SMOOTHING_ALPHA = _DEFAULT_FEE_ALPHA

FEE_DRIFT_ALERT_THRESHOLD_BPS = _decimal_setting("FEE_DRIFT_ALERT_THRESHOLD_BPS", "5")
if FEE_DRIFT_ALERT_THRESHOLD_BPS < 0:
    FEE_DRIFT_ALERT_THRESHOLD_BPS = Decimal("5")

FEE_DRIFT_ALERT_THRESHOLD_RATIO = _decimal_setting("FEE_DRIFT_ALERT_THRESHOLD_RATIO", "0.25")
if FEE_DRIFT_ALERT_THRESHOLD_RATIO < 0:
    FEE_DRIFT_ALERT_THRESHOLD_RATIO = Decimal("0.25")

FEE_ESTIMATE_QUANTIZE = Decimal("0.0001")

_DATABASE_URL_ENV = "FEES_DATABASE_URL"


def _database_url() -> str:
    url = os.getenv(_DATABASE_URL_ENV) or os.getenv("TIMESCALE_DSN")
    if not url:
        raise RuntimeError(
            "FEES_DATABASE_URL or TIMESCALE_DSN must be configured with a PostgreSQL/Timescale DSN"
        )

    normalized = url.strip()
    if normalized.startswith("postgres://"):
        normalized = "postgresql://" + normalized.split("://", 1)[1]

    if normalized.startswith("postgresql://"):
        normalized = normalized.replace("postgresql://", "postgresql+psycopg2://", 1)
    elif normalized.startswith("postgresql+psycopg://"):
        normalized = normalized.replace("postgresql+psycopg://", "postgresql+psycopg2://", 1)
    elif normalized.startswith("postgresql+psycopg2://"):
        pass
    else:
        raise RuntimeError(
            "Fee service requires a PostgreSQL/Timescale DSN via FEES_DATABASE_URL or TIMESCALE_DSN"
        )

    return normalized


def _engine_options() -> dict[str, Any]:
    options: dict[str, Any] = {
        "future": True,
        "pool_pre_ping": True,
        "pool_size": int(os.getenv("FEES_DB_POOL_SIZE", "10")),
        "max_overflow": int(os.getenv("FEES_DB_MAX_OVERFLOW", "20")),
        "pool_timeout": int(os.getenv("FEES_DB_POOL_TIMEOUT", "30")),
        "pool_recycle": int(os.getenv("FEES_DB_POOL_RECYCLE", "1800")),
    }

    sslmode = os.getenv("FEES_DB_SSLMODE", "require").strip()
    connect_args: dict[str, Any] = {}
    if sslmode:
        connect_args["sslmode"] = sslmode
    options["connect_args"] = connect_args

    return options


DATABASE_URL = _database_url()
ENGINE: Engine = create_engine(DATABASE_URL, **_engine_options())
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)


app = FastAPI(title="Fee Schedule Service")


logger = logging.getLogger(__name__)


DEFAULT_KRAKEN_SCHEDULE: Sequence[dict[str, Decimal | str]] = (
    {"tier_id": "tier_0", "threshold": Decimal("0"), "maker": Decimal("16"), "taker": Decimal("26")},
    {"tier_id": "tier_1", "threshold": Decimal("10000"), "maker": Decimal("14"), "taker": Decimal("24")},
    {"tier_id": "tier_2", "threshold": Decimal("50000"), "maker": Decimal("12"), "taker": Decimal("22")},
    {"tier_id": "tier_3", "threshold": Decimal("100000"), "maker": Decimal("10"), "taker": Decimal("20")},
    {"tier_id": "tier_4", "threshold": Decimal("250000"), "maker": Decimal("8"), "taker": Decimal("18")},
    {"tier_id": "tier_5", "threshold": Decimal("500000"), "maker": Decimal("6"), "taker": Decimal("16")},
    {"tier_id": "tier_6", "threshold": Decimal("1000000"), "maker": Decimal("4"), "taker": Decimal("14")},
    {"tier_id": "tier_7", "threshold": Decimal("2500000"), "maker": Decimal("2"), "taker": Decimal("12")},
    {"tier_id": "tier_8", "threshold": Decimal("5000000"), "maker": Decimal("0"), "taker": Decimal("10")},
    {"tier_id": "tier_9", "threshold": Decimal("10000000"), "maker": Decimal("0"), "taker": Decimal("8")},
    {"tier_id": "tier_10", "threshold": Decimal("25000000"), "maker": Decimal("0"), "taker": Decimal("6")},
    {"tier_id": "tier_11", "threshold": Decimal("100000000"), "maker": Decimal("0"), "taker": Decimal("4")},
)


def _seed_schedule(session: Session) -> None:
    existing = session.execute(select(func.count()).select_from(FeeTier)).scalar_one()
    if existing:
        return

    effective_from = datetime(2020, 1, 1, tzinfo=timezone.utc)
    for tier in DEFAULT_KRAKEN_SCHEDULE:
        session.add(
            FeeTier(
                tier_id=tier["tier_id"],
                notional_threshold_usd=tier["threshold"],
                maker_bps=tier["maker"],
                taker_bps=tier["taker"],
                effective_from=effective_from,
            )
        )
    session.commit()


@app.on_event("startup")
def _on_startup() -> None:
    Base.metadata.create_all(bind=ENGINE)
    with SessionLocal() as session:
        _seed_schedule(session)


def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


class EffectiveFeeResponse(BaseModel):
    bps: float = Field(..., description="Fee rate expressed in basis points")
    usd: float = Field(..., description="Fee amount in USD")
    tier_id: str = Field(..., description="Identifier of the matched fee tier")
    basis_ts: datetime = Field(..., description="Timestamp of the volume basis for the tier decision")


class FeeTierSchema(BaseModel):
    tier_id: str
    notional_threshold_usd: float
    maker_bps: float
    taker_bps: float
    effective_from: datetime


class Volume30dResponse(BaseModel):
    notional_usd_30d: float
    updated_at: datetime


class AccountSummaryResponse(BaseModel):
    account_id: str = Field(..., description="Account identifier")
    tier: str = Field(..., description="Matched Kraken fee tier")
    volume_usd: float = Field(..., ge=0.0, description="Rolling 30-day USD volume")
    effective_fee_bps: float = Field(..., ge=0.0, description="Realized effective fee in basis points")
    basis_ts: datetime = Field(..., description="Timestamp anchoring the rolling window")


class FeeEstimateResponse(BaseModel):
    account_id: str = Field(..., description="Account identifier")
    symbol: str = Field(..., description="Trading pair symbol for the estimate")
    side: str = Field(..., description="Order side being evaluated")
    order_type: str = Field(..., description="Order type driving liquidity inference")
    tier: str = Field(..., description="Matched Kraken fee tier")
    volume_usd_30d: float = Field(..., ge=0.0, description="Rolling 30-day USD volume basis")
    maker_fee_bps_estimate: float = Field(
        ..., ge=0.0, description="Maker fee estimate expressed in basis points"
    )
    taker_fee_bps_estimate: float = Field(
        ..., ge=0.0, description="Taker fee estimate expressed in basis points"
    )
    inferred_liquidity: str | None = Field(
        None, description="Liquidity classification inferred from the order type"
    )
    fee_bps_estimate: float = Field(
        ..., ge=0.0, description="Fee estimate aligned with inferred liquidity"
    )
    basis_ts: datetime = Field(..., description="Timestamp anchoring the rolling window")


class NextTierStatusResponse(BaseModel):
    current_tier: str = Field(..., description="Identifier of the active fee tier")
    next_tier: str | None = Field(
        None, description="Identifier of the next fee tier if one exists"
    )
    progress_pct: float = Field(
        ..., description="Progress towards the next tier expressed as a percentage"
    )


def _fee_amount(notional: Decimal, bps: Decimal) -> Decimal:
    raw_fee = (notional * bps) / Decimal("10000")
    return raw_fee.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


def _to_decimal(value: Decimal | float | int | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


optimizer = FeeOptimizer(
    alert_threshold=NEXT_TIER_ALERT_THRESHOLD,
    policy_service_url=POLICY_SERVICE_URL,
    policy_path=POLICY_VOLUME_SIGNAL_PATH,
    policy_timeout=POLICY_VOLUME_SIGNAL_TIMEOUT,
)


def _ordered_tiers(session: Session) -> list[FeeTier]:
    """Return the configured fee tiers ordered by threshold."""

    return optimizer.ordered_tiers(session)


def _account_volume_snapshot(
    session: Session, account_id: str, as_of: datetime | None = None
) -> tuple[Decimal, datetime, AccountVolume30d | None]:
    """Return the stored rolling volume snapshot, falling back to raw fills."""

    record = session.get(AccountVolume30d, account_id)
    if record is not None and record.updated_at is not None:
        return _to_decimal(record.notional_usd_30d or 0), record.updated_at, record

    volume, basis_ts = _rolling_volume(session, account_id, as_of)
    return volume, basis_ts, record


def _infer_liquidity_hint(order_type: str) -> str | None:
    """Best-effort inference of liquidity from the order type."""

    normalized = order_type.lower()
    if normalized in {"limit", "post_only", "maker"}:
        return "maker"
    if normalized in {"market", "ioc", "fok", "taker"}:
        return "taker"
    return None


def _update_fee_estimate(
    record: AccountVolume30d,
    liquidity: str,
    actual_bps: Decimal,
    timestamp: datetime,
) -> Decimal:
    """Update the exponentially-weighted moving average fee estimate."""

    field = "maker_fee_bps_estimate" if liquidity == "maker" else "taker_fee_bps_estimate"
    existing_value = getattr(record, field)
    if existing_value is not None:
        current = Decimal(existing_value)
        weight_existing = Decimal("1") - FEE_ESTIMATE_SMOOTHING_ALPHA
        updated = (current * weight_existing) + (
            actual_bps * FEE_ESTIMATE_SMOOTHING_ALPHA
        )
    else:
        updated = actual_bps

    updated = updated.quantize(FEE_ESTIMATE_QUANTIZE, rounding=ROUND_HALF_UP)
    setattr(record, field, updated)
    record.fee_estimate_updated_at = timestamp
    return updated


def _maybe_alert_fee_drift(
    account_id: str,
    liquidity: str | None,
    notional: Decimal,
    actual_bps: Decimal | None,
    expected_bps: Decimal | None,
    actual_fee: Decimal | None,
    expected_fee_usd_hint: Decimal | None,
    expected_source: str | None,
) -> None:
    """Emit a warning when realized fees deviate materially from expectations."""

    if liquidity is None or actual_bps is None or expected_bps is None:
        return

    drift_bps = actual_bps - expected_bps
    drift_ratio: Decimal | None = None
    if expected_bps != 0:
        drift_ratio = drift_bps / expected_bps

    if (
        abs(drift_bps) < FEE_DRIFT_ALERT_THRESHOLD_BPS
        and (drift_ratio is None or abs(drift_ratio) < FEE_DRIFT_ALERT_THRESHOLD_RATIO)
    ):
        return

    expected_fee_usd = (
        expected_fee_usd_hint
        if expected_fee_usd_hint is not None
        else _fee_amount(notional, expected_bps)
    )

    logger.warning(
        "fee_estimate_drift",
        extra={
            "account_id": account_id,
            "liquidity": liquidity,
            "notional_usd": float(notional),
            "actual_fee_bps": float(actual_bps),
            "expected_fee_bps": float(expected_bps),
            "drift_bps": float(drift_bps),
            "drift_ratio": float(drift_ratio) if drift_ratio is not None else None,
            "actual_fee_usd": float(actual_fee) if actual_fee is not None else None,
            "expected_fee_usd": float(expected_fee_usd),
            "expected_source": expected_source,
        },
    )


def _rolling_window(as_of: datetime | None = None) -> Tuple[datetime, datetime]:
    now = as_of or datetime.now(timezone.utc)
    start = now - timedelta(days=30)
    return start, now


def _rolling_volume(
    session: Session, account_id: str, as_of: datetime | None = None
) -> Tuple[Decimal, datetime]:
    window_start, window_end = _rolling_window(as_of)
    stmt = (
        select(
            func.coalesce(func.sum(AccountFill.notional_usd), Decimal("0")),
            func.max(AccountFill.fill_ts),
        )
        .where(AccountFill.account_id == account_id)
        .where(AccountFill.fill_ts >= window_start)
        .where(AccountFill.fill_ts <= window_end)
    )
    total_notional, basis_ts = session.execute(stmt).one()
    volume = _to_decimal(total_notional)
    basis = basis_ts or window_end
    return volume, basis


def _realized_fee_bps(session: Session, account_id: str, as_of: datetime | None = None) -> Decimal:
    window_start, window_end = _rolling_window(as_of)
    stmt = (
        select(
            func.coalesce(func.sum(AccountFill.notional_usd), Decimal("0")),
            func.coalesce(func.sum(AccountFill.actual_fee_usd), Decimal("0")),
            func.coalesce(func.sum(AccountFill.estimated_fee_usd), Decimal("0")),
        )
        .where(AccountFill.account_id == account_id)
        .where(AccountFill.fill_ts >= window_start)
        .where(AccountFill.fill_ts <= window_end)
    )
    total_notional, actual_fee_total, estimated_fee_total = session.execute(stmt).one()
    notional = _to_decimal(total_notional)
    if notional <= 0:
        return Decimal("0")
    actual_total = _to_decimal(actual_fee_total)
    if actual_total > 0:
        return (actual_total / notional) * Decimal("10000")
    estimated_total = _to_decimal(estimated_fee_total)
    if estimated_total > 0:
        return (estimated_total / notional) * Decimal("10000")
    return Decimal("0")


@app.get("/fees/effective", response_model=EffectiveFeeResponse)
def get_effective_fee(
    pair: str = Query(..., description="Trading pair symbol", min_length=3, max_length=32),
    liquidity: str = Query(..., description="Requested liquidity side", pattern=r"(?i)^(maker|taker)$"),
    notional: float = Query(..., gt=0.0, description="Order notional in USD"),
    session: Session = Depends(get_session),
    account_id: str = Depends(require_admin_account),
) -> EffectiveFeeResponse:
    del pair  # the current schedule is global and does not vary by pair

    normalized_liquidity = liquidity.lower()
    tiers = _ordered_tiers(session)
    if not tiers:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fee schedule is not configured")

    rolling_volume, basis_ts, _ = _account_volume_snapshot(session, account_id)

    try:
        tier = optimizer.determine_tier(tiers, rolling_volume)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    bps_value = Decimal(tier.maker_bps if normalized_liquidity == "maker" else tier.taker_bps)
    notional_decimal = Decimal(str(notional))
    fee_usd = _fee_amount(notional_decimal, bps_value)

    return EffectiveFeeResponse(
        bps=float(bps_value),
        usd=float(fee_usd),
        tier_id=tier.tier_id,
        basis_ts=basis_ts,
    )


@app.get("/fees/tiers", response_model=List[FeeTierSchema])
def get_fee_tiers(
    session: Session = Depends(get_session),
    _: str = Depends(require_admin_account),
) -> List[FeeTierSchema]:
    tiers = _ordered_tiers(session)
    if not tiers:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fee schedule is not configured")

    return [
        FeeTierSchema(
            tier_id=tier.tier_id,
            notional_threshold_usd=float(tier.notional_threshold_usd or 0),
            maker_bps=float(tier.maker_bps),
            taker_bps=float(tier.taker_bps),
            effective_from=tier.effective_from,
        )
        for tier in tiers
    ]


@app.get("/fees/volume30d", response_model=Volume30dResponse)
def get_volume_30d(
    session: Session = Depends(get_session),
    account_id: str = Depends(require_admin_account),
) -> Volume30dResponse:
    volume, basis_ts, _ = _account_volume_snapshot(session, account_id)
    return Volume30dResponse(
        notional_usd_30d=float(volume),
        updated_at=basis_ts,
    )


@app.get("/fees/estimate", response_model=FeeEstimateResponse)
def get_fee_estimate(
    account_id: str = Query(..., min_length=1, max_length=64, description="Account identifier"),
    symbol: str = Query(..., min_length=3, max_length=32, description="Trading pair symbol"),
    side: str = Query(..., pattern=r"(?i)^(buy|sell)$", description="Order side"),
    order_type: str = Query(
        ..., min_length=3, max_length=32, description="Order type (e.g. limit, market, post_only)"
    ),
    session: Session = Depends(get_session),
    _: str = Depends(require_admin_account),
) -> FeeEstimateResponse:
    symbol_key = require_spot_http(symbol)

    tiers = _ordered_tiers(session)
    if not tiers:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fee schedule is not configured")

    volume, basis_ts, record = _account_volume_snapshot(session, account_id)

    try:
        tier = optimizer.determine_tier(tiers, volume)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    maker_bps = Decimal(tier.maker_bps)
    taker_bps = Decimal(tier.taker_bps)

    if record is not None and record.maker_fee_bps_estimate is not None:
        maker_bps = Decimal(record.maker_fee_bps_estimate)
    if record is not None and record.taker_fee_bps_estimate is not None:
        taker_bps = Decimal(record.taker_fee_bps_estimate)

    liquidity_hint = _infer_liquidity_hint(order_type)
    selected_bps = maker_bps if liquidity_hint == "maker" else taker_bps

    return FeeEstimateResponse(
        account_id=account_id,
        symbol=symbol_key,
        side=side.lower(),
        order_type=order_type.lower(),
        tier=tier.tier_id,
        volume_usd_30d=float(volume),
        maker_fee_bps_estimate=float(maker_bps),
        taker_fee_bps_estimate=float(taker_bps),
        inferred_liquidity=liquidity_hint,
        fee_bps_estimate=float(selected_bps),
        basis_ts=basis_ts,
    )


@app.get("/fees/account_summary", response_model=AccountSummaryResponse)
def get_account_summary(
    account_id: str = Query(..., min_length=1, max_length=64, description="Account identifier"),
    session: Session = Depends(get_session),
    _: str = Depends(require_admin_account),
) -> AccountSummaryResponse:
    tiers = _ordered_tiers(session)
    if not tiers:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fee schedule is not configured")

    volume, basis_ts, _ = _account_volume_snapshot(session, account_id)
    try:
        tier = optimizer.determine_tier(tiers, volume)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    effective_fee = _realized_fee_bps(session, account_id, basis_ts)
    return AccountSummaryResponse(
        account_id=account_id,
        tier=tier.tier_id,
        volume_usd=float(volume),
        effective_fee_bps=float(effective_fee),
        basis_ts=basis_ts,
    )


@app.get("/fees/next_tier_status", response_model=NextTierStatusResponse)
def get_next_tier_status(
    session: Session = Depends(get_session),
    account_id: str = Depends(require_admin_account),
) -> NextTierStatusResponse:
    try:
        status = optimizer.status_for_account(session, account_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return NextTierStatusResponse(
        current_tier=status.current_tier.tier_id,
        next_tier=status.next_tier.tier_id if status.next_tier is not None else None,
        progress_pct=float(status.progress_pct),
    )


def update_account_volume_30d(
    session: Session,
    account_id: str,
    fill_notional_usd: float,
    fill_time: datetime | None = None,
    liquidity: str | None = None,
    estimated_fee_bps: float | None = None,
    actual_fee_usd: float | None = None,
) -> AccountVolume30d:
    """Update the rolling 30-day volume using a newly observed fill."""

    timestamp = fill_time or datetime.now(timezone.utc)
    notional_delta = _to_decimal(fill_notional_usd)

    if notional_delta < 0:
        raise ValueError("fill_notional_usd must be non-negative")

    normalized_liquidity = liquidity.lower() if liquidity is not None else None

    estimated_bps = Decimal(str(estimated_fee_bps)) if estimated_fee_bps is not None else None
    estimated_fee_usd = (
        _fee_amount(notional_delta, estimated_bps) if estimated_bps is not None else None
    )
    actual_fee = Decimal(str(actual_fee_usd)) if actual_fee_usd is not None else None

    session.add(
        AccountFill(
            account_id=account_id,
            liquidity=normalized_liquidity,
            notional_usd=notional_delta,
            estimated_fee_bps=estimated_bps,
            estimated_fee_usd=estimated_fee_usd,
            actual_fee_usd=actual_fee,
            fill_ts=timestamp,
        )
    )
    session.flush()

    prune_before = timestamp - timedelta(days=60)
    session.execute(
        AccountFill.__table__
        .delete()
        .where(AccountFill.__table__.c.account_id == account_id)
        .where(AccountFill.__table__.c.fill_ts < prune_before)
    )

    rolling_volume, basis_ts = _rolling_volume(session, account_id, timestamp)

    record = session.get(AccountVolume30d, account_id)
    existing_maker_estimate = (
        Decimal(record.maker_fee_bps_estimate)
        if record is not None and record.maker_fee_bps_estimate is not None
        else None
    )
    existing_taker_estimate = (
        Decimal(record.taker_fee_bps_estimate)
        if record is not None and record.taker_fee_bps_estimate is not None
        else None
    )
    if record is None:
        record = AccountVolume30d(
            account_id=account_id,
            notional_usd_30d=rolling_volume,
            updated_at=basis_ts,
        )
        session.add(record)
    else:
        record.notional_usd_30d = rolling_volume
        record.updated_at = basis_ts

    actual_bps: Decimal | None = None
    if actual_fee is not None:
        actual_bps = (
            (actual_fee / notional_delta) * Decimal("10000")
            if notional_delta > 0
            else Decimal("0")
        )

    expected_bps: Decimal | None = None
    expected_source: str | None = None
    if estimated_bps is not None:
        expected_bps = estimated_bps
        expected_source = "request"
    elif normalized_liquidity == "maker" and existing_maker_estimate is not None:
        expected_bps = existing_maker_estimate
        expected_source = "historical"
    elif normalized_liquidity == "taker" and existing_taker_estimate is not None:
        expected_bps = existing_taker_estimate
        expected_source = "historical"

    if actual_bps is not None and normalized_liquidity in {"maker", "taker"}:
        _update_fee_estimate(record, normalized_liquidity, actual_bps, timestamp)

    session.commit()
    session.refresh(record)

    if actual_bps is not None:
        expected_fee_usd = None
        if expected_bps is not None:
            expected_fee_usd = (
                estimated_fee_usd
                if estimated_fee_usd is not None and expected_source == "request"
                else _fee_amount(notional_delta, expected_bps)
            )

        log_extra: dict[str, float | str | None] = {
            "account_id": account_id,
            "liquidity": normalized_liquidity,
            "notional_usd": float(notional_delta),
            "actual_fee_bps": float(actual_bps),
        }
        if actual_fee is not None:
            log_extra["actual_fee_usd"] = float(actual_fee)
        if expected_bps is not None:
            log_extra["estimated_fee_bps"] = float(expected_bps)
            log_extra["estimated_fee_source"] = expected_source
            log_extra["discrepancy_bps"] = float(actual_bps - expected_bps)
            if expected_fee_usd is not None:
                log_extra["estimated_fee_usd"] = float(expected_fee_usd)
                if actual_fee is not None:
                    log_extra["discrepancy_usd"] = float(actual_fee - expected_fee_usd)
        elif estimated_fee_usd is not None:
            log_extra["estimated_fee_usd"] = float(estimated_fee_usd)

        logger.info("fee_reconciliation", extra=log_extra)

        _maybe_alert_fee_drift(
            account_id,
            normalized_liquidity,
            notional_delta,
            actual_bps,
            expected_bps,
            actual_fee,
            expected_fee_usd,
            expected_source,
        )

    optimizer.monitor_account(
        session,
        account_id,
        Decimal(record.notional_usd_30d or 0),
        record.updated_at or timestamp,
    )
    return record


__all__ = ["app", "update_account_volume_30d"]

