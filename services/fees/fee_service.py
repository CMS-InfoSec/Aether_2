"""FastAPI service exposing the venue fee schedule and volume metrics."""
from __future__ import annotations

import logging
import os
from collections.abc import Generator, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from types import SimpleNamespace
from typing import Any, Callable, TypeVar, TypedDict, cast

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

from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

from sqlalchemy import create_engine, func, select
from typing import TYPE_CHECKING

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

if TYPE_CHECKING:  # pragma: no cover - type checking only
    from sqlalchemy.sql.schema import Table
else:  # pragma: no cover - lightweight environments may lack sqlalchemy.sql
    Table = Any  # type: ignore[misc,assignment]

from metrics import setup_metrics

from services.common.security import require_admin_account
from services.common.spot import require_spot_http
from services.fees.fee_optimizer import FeeOptimizer
from services.fees.models import AccountFill, AccountVolume30d, Base, FeeTier
from shared.pydantic_compat import BaseModel, Field

try:  # pragma: no cover - optional dependencies may be unavailable
    from services.common.adapters import (  # type: ignore[import]
        RedisFeastAdapter as _RedisFeastAdapter,
        TimescaleAdapter as _TimescaleAdapter,
    )
except Exception:  # pragma: no cover - fall back to lightweight shims
    _RedisFeastAdapter = None  # type: ignore[assignment]
    _TimescaleAdapter = None  # type: ignore[assignment]

POLICY_SERVICE_URL = os.getenv("POLICY_SERVICE_URL", "http://policy-service")
POLICY_VOLUME_SIGNAL_PATH = os.getenv(
    "POLICY_VOLUME_SIGNAL_PATH", "/policy/opportunistic-volume"
)
POLICY_VOLUME_SIGNAL_TIMEOUT = float(os.getenv("POLICY_VOLUME_SIGNAL_TIMEOUT", "0.5"))
NEXT_TIER_ALERT_THRESHOLD = Decimal("0.95")


class _FallbackRedisFeastAdapter:
    """Dependency-light stand-in for the Redis/Feast adapter."""

    def __init__(self, account_id: str, *args: Any, **kwargs: Any) -> None:
        self.account_id = account_id

    def fee_tiers(self, pair: str) -> list[dict[str, Any]]:  # pragma: no cover - simple fallback
        del pair
        return []

    def fee_override(self, instrument: str) -> dict[str, Any]:  # pragma: no cover - simple fallback
        del instrument
        return {"currency": "USD", "maker": Decimal("0"), "taker": Decimal("0")}


class _FallbackTimescaleAdapter:
    """File-backed substitute when the Timescale adapter is unavailable."""

    def __init__(self, account_id: str, *args: Any, **kwargs: Any) -> None:
        self.account_id = account_id

    def rolling_volume(self, pair: str) -> dict[str, Any]:  # pragma: no cover - simple fallback
        del pair
        return {"notional": Decimal("0"), "basis_ts": datetime.now(timezone.utc)}


RedisFeastAdapter = _RedisFeastAdapter or _FallbackRedisFeastAdapter
TimescaleAdapter = _TimescaleAdapter or _FallbackTimescaleAdapter


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


def _coerce_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    """Best-effort conversion of arbitrary values to :class:`Decimal`."""

    if isinstance(value, Decimal):
        return value
    if value is None:
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _coerce_datetime(value: Any, fallback: datetime) -> datetime:
    """Convert JSON-friendly timestamps to :class:`datetime`."""

    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        candidate = value.strip()
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            return fallback
    return fallback


def _select_tier(
    tiers: Sequence[Mapping[str, Any]], projected_volume: Decimal
) -> dict[str, Any] | None:
    """Return the fee tier matching the projected 30-day volume."""

    selected: dict[str, Any] | None = None
    for entry in sorted(
        (dict(tier) for tier in tiers),
        key=lambda tier: _coerce_decimal(tier.get("volume_threshold")),
    ):
        threshold = _coerce_decimal(entry.get("volume_threshold"))
        if selected is None:
            selected = entry
        if projected_volume >= threshold:
            selected = entry
        else:
            break
    return selected


def _resolve_rate(detail_bps: Decimal, override_value: Any) -> Decimal:
    """Determine the applied fee rate after considering overrides."""

    override_bps = _coerce_decimal(override_value, Decimal("0"))
    if detail_bps > 0 and override_bps > 0:
        return min(detail_bps, override_bps)
    if detail_bps > 0:
        return detail_bps
    if override_bps > 0:
        return override_bps
    return Decimal("0")


def _resolve_override(adapter: Any, instrument: str) -> dict[str, Any]:
    """Fetch fee overrides, falling back to the default instrument when absent."""

    def _fetch(name: str) -> dict[str, Any]:
        try:
            return dict(adapter.fee_override(name) or {})
        except Exception:
            return {}

    primary = _fetch(instrument)
    if any(primary.get(key) for key in ("maker", "taker", "currency")):
        return primary
    fallback = _fetch("default")
    if fallback and not primary:
        return fallback
    merged: dict[str, Any] = {}
    merged.update(fallback)
    merged.update(primary)
    return merged


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
SessionLocal = sessionmaker(
    bind=ENGINE,
    class_=Session,
    autoflush=False,
    expire_on_commit=False,
    future=True,
)


app = FastAPI(title="Fee Schedule Service")
setup_metrics(app, service_name="fee-service")


logger = logging.getLogger(__name__)

RouteFn = TypeVar("RouteFn", bound=Callable[..., Any])


def _app_get(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around :meth:`FastAPI.get` for strict type checking."""

    return cast(Callable[[RouteFn], RouteFn], app.get(*args, **kwargs))


def _app_on_event(event: str) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around :meth:`FastAPI.on_event` for strict type checking."""

    return cast(Callable[[RouteFn], RouteFn], app.on_event(event))


class _ScheduleEntry(TypedDict):
    tier_id: str
    threshold: Decimal
    maker: Decimal
    taker: Decimal


DEFAULT_KRAKEN_SCHEDULE: tuple[_ScheduleEntry, ...] = (
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

    # Lightweight SQLAlchemy stubs used in import tests provide placeholder models that
    # do not accept keyword arguments. Skip seeding in that environment so the module
    # remains importable without the full ORM stack.
    if getattr(FeeTier, "__init__", object.__init__) is object.__init__:
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


try:
    ACCOUNT_FILL_TABLE: Table = AccountFill.__table__  # type: ignore[assignment]
except AttributeError:
    class _StubColumn:
        def __init__(self, name: str) -> None:
            self.name = name

    class _StubDelete:
        def __init__(self) -> None:
            self.c = SimpleNamespace(
                account_id=_StubColumn("account_id"),
                fill_ts=_StubColumn("fill_ts"),
            )

        def delete(self) -> "_StubDelete":  # pragma: no cover - lightweight fallback
            return self

        def where(self, *_args: Any, **_kwargs: Any) -> "_StubDelete":  # pragma: no cover
            return self

    ACCOUNT_FILL_TABLE = _StubDelete()  # type: ignore[assignment]


@_app_on_event("startup")
def _on_startup() -> None:
    Base.metadata.create_all(bind=ENGINE)
    with SessionLocal() as session:
        _seed_schedule(session)


def get_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


class FeeDetailPayload(BaseModel):
    tier_id: str = Field(..., description="Identifier of the tier providing the rate")
    bps: float = Field(..., description="Fee expressed in basis points")
    usd: float = Field(..., description="Fee amount in USD for the requested notional")
    basis_ts: datetime = Field(..., description="Timestamp anchoring the tier decision")


class FeeBreakdownPayload(BaseModel):
    currency: str = Field(..., description="Fee currency")
    maker: float = Field(..., description="Applied maker fee in basis points")
    taker: float = Field(..., description="Applied taker fee in basis points")
    maker_detail: FeeDetailPayload
    taker_detail: FeeDetailPayload


class EffectiveFeePayload(BaseModel):
    account_id: str = Field(..., description="Account identifier for the request")
    effective_from: datetime = Field(..., description="Timestamp anchoring the fee schedule")
    fee: FeeBreakdownPayload


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

    record = cast(AccountVolume30d | None, session.get(AccountVolume30d, account_id))
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


def _rolling_window(as_of: datetime | None = None) -> tuple[datetime, datetime]:
    now = as_of or datetime.now(timezone.utc)
    start = now - timedelta(days=30)
    return start, now


def _rolling_volume(
    session: Session, account_id: str, as_of: datetime | None = None
) -> tuple[Decimal, datetime]:
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


@_app_get("/fees/effective", response_model=EffectiveFeePayload)
def get_effective_fee(
    pair: str = Query(..., description="Trading pair symbol", min_length=3, max_length=32),
    liquidity: str = Query(..., description="Requested liquidity side", pattern=r"(?i)^(maker|taker)$"),
    notional: float = Query(..., gt=0.0, description="Order notional in USD"),
    session: Session = Depends(get_session),
    account_id: str = Depends(require_admin_account),
) -> EffectiveFeePayload:
    canonical_pair = require_spot_http(pair, param="pair", logger=logger)
    logger.debug(
        "Calculating effective fee", extra={"pair": canonical_pair, "account_id": account_id}
    )

    request_notional = _coerce_decimal(notional, Decimal("0"))

    try:
        redis_adapter = RedisFeastAdapter(account_id=account_id)
    except Exception:  # pragma: no cover - fallback to lightweight adapter
        redis_adapter = _FallbackRedisFeastAdapter(account_id)

    try:
        timescale_adapter = TimescaleAdapter(account_id=account_id)
    except Exception:  # pragma: no cover - fallback to lightweight adapter
        timescale_adapter = _FallbackTimescaleAdapter(account_id)

    try:
        tier_rows = list(redis_adapter.fee_tiers(canonical_pair) or [])
    except Exception:
        tier_rows = []

    override = _resolve_override(redis_adapter, canonical_pair)

    try:
        volume_snapshot = dict(timescale_adapter.rolling_volume(canonical_pair) or {})
    except Exception:
        volume_snapshot = {}

    current_volume = _coerce_decimal(volume_snapshot.get("notional"), Decimal("0"))
    basis_ts = _coerce_datetime(volume_snapshot.get("basis_ts"), datetime.now(timezone.utc))
    projected_volume = current_volume + request_notional

    selected_tier = _select_tier(tier_rows, projected_volume)
    if selected_tier is None:
        selected_tier = {
            "tier_id": override.get("tier_id", "default"),
            "maker_bps": override.get("maker"),
            "taker_bps": override.get("taker"),
            "basis_ts": basis_ts,
            "currency": override.get("currency", "USD"),
        }

    maker_detail_bps = _coerce_decimal(
        selected_tier.get("maker_bps"), _coerce_decimal(override.get("maker"))
    )
    taker_detail_bps = _coerce_decimal(
        selected_tier.get("taker_bps"), _coerce_decimal(override.get("taker"))
    )

    maker_rate = _resolve_rate(maker_detail_bps, override.get("maker"))
    taker_rate = _resolve_rate(taker_detail_bps, override.get("taker"))

    currency = str(
        override.get("currency")
        or selected_tier.get("currency")
        or "USD"
    )

    tier_basis = _coerce_datetime(selected_tier.get("basis_ts"), basis_ts)
    tier_id = str(selected_tier.get("tier_id") or override.get("tier_id") or "default")

    maker_fee_usd = _fee_amount(request_notional, maker_detail_bps)
    taker_fee_usd = _fee_amount(request_notional, taker_detail_bps)

    maker_detail = FeeDetailPayload(
        tier_id=tier_id,
        bps=float(maker_detail_bps),
        usd=float(maker_fee_usd),
        basis_ts=tier_basis,
    )
    taker_detail = FeeDetailPayload(
        tier_id=tier_id,
        bps=float(taker_detail_bps),
        usd=float(taker_fee_usd),
        basis_ts=tier_basis,
    )

    fee_payload = FeeBreakdownPayload(
        currency=currency,
        maker=float(maker_rate),
        taker=float(taker_rate),
        maker_detail=maker_detail,
        taker_detail=taker_detail,
    )

    return EffectiveFeePayload(
        account_id=account_id,
        effective_from=tier_basis,
        fee=fee_payload,
    )


@_app_get("/fees/tiers", response_model=list[FeeTierSchema])
def get_fee_tiers(
    session: Session = Depends(get_session),
    _admin_account: str = Depends(require_admin_account),
) -> list[FeeTierSchema]:
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


@_app_get("/fees/volume30d", response_model=Volume30dResponse)
def get_volume_30d(
    session: Session = Depends(get_session),
    account_id: str = Depends(require_admin_account),
) -> Volume30dResponse:
    volume, basis_ts, _ = _account_volume_snapshot(session, account_id)
    return Volume30dResponse(
        notional_usd_30d=float(volume),
        updated_at=basis_ts,
    )


@_app_get("/fees/estimate", response_model=FeeEstimateResponse)
def get_fee_estimate(
    account_id: str = Query(..., min_length=1, max_length=64, description="Account identifier"),
    symbol: str = Query(..., min_length=3, max_length=32, description="Trading pair symbol"),
    side: str = Query(..., pattern=r"(?i)^(buy|sell)$", description="Order side"),
    order_type: str = Query(
        ..., min_length=3, max_length=32, description="Order type (e.g. limit, market, post_only)"
    ),
    session: Session = Depends(get_session),
    _admin_account: str = Depends(require_admin_account),
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


@_app_get("/fees/account_summary", response_model=AccountSummaryResponse)
def get_account_summary(
    account_id: str = Query(..., min_length=1, max_length=64, description="Account identifier"),
    session: Session = Depends(get_session),
    _admin_account: str = Depends(require_admin_account),
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


@_app_get("/fees/next_tier_status", response_model=NextTierStatusResponse)
def get_next_tier_status(
    session: Session = Depends(get_session),
    account_id: str = Depends(require_admin_account),
) -> NextTierStatusResponse:
    try:
        tier_status = optimizer.status_for_account(session, account_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return NextTierStatusResponse(
        current_tier=tier_status.current_tier.tier_id,
        next_tier=tier_status.next_tier.tier_id if tier_status.next_tier is not None else None,
        progress_pct=float(tier_status.progress_pct),
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
        ACCOUNT_FILL_TABLE.delete()
        .where(ACCOUNT_FILL_TABLE.c.account_id == account_id)
        .where(ACCOUNT_FILL_TABLE.c.fill_ts < prune_before)
    )

    rolling_volume, basis_ts = _rolling_volume(session, account_id, timestamp)

    record = cast(AccountVolume30d | None, session.get(AccountVolume30d, account_id))
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

