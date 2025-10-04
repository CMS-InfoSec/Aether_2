"""FastAPI service exposing capital flow management endpoints.

This module provides lightweight APIs for recording deposits and withdrawals
against trading accounts while maintaining a running NAV baseline per account.
All persistence happens in a managed PostgreSQL/TimescaleDB cluster referenced
via the ``CAPITAL_FLOW_DATABASE_URL`` environment variable. The service fails
fast when the DSN is missing or incorrectly configured so that deployments do
not silently fall back to local storage.
"""

from __future__ import annotations

import enum
import os
from dataclasses import dataclass
from datetime import datetime, timezone

from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from typing import Generator, Optional


from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator
from sqlalchemy import Column, DateTime, Integer, Numeric, String, create_engine, select, func
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from sqlalchemy.pool import StaticPool
from sqlalchemy.types import TypeDecorator


from services.common.security import require_admin_account


_DECIMAL_PRECISION = 38
_DECIMAL_SCALE = 18

ZERO = Decimal("0")
_QUANT = Decimal("1").scaleb(-_DECIMAL_SCALE)


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(_QUANT, rounding=ROUND_HALF_EVEN)


class PreciseDecimal(TypeDecorator):
    """Type decorator storing high-precision decimals losslessly on SQLite."""

    impl = Numeric
    cache_ok = True

    def __init__(self, precision: int, scale: int, **kwargs):
        super().__init__(**kwargs)
        self.precision = precision
        self.scale = scale
        self._numeric = Numeric(precision, scale, asdecimal=True)

    @property
    def python_type(self) -> type[Decimal]:  # pragma: no cover - SQLAlchemy hook
        return Decimal

    def load_dialect_impl(self, dialect):
        if dialect.name == "sqlite":
            return dialect.type_descriptor(String(self.precision + self.scale + 2))
        return dialect.type_descriptor(self._numeric)

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if not isinstance(value, Decimal):
            value = Decimal(str(value))
        value = _quantize(value)
        if dialect.name == "sqlite":
            return format(value, f".{self.scale}f")
        return value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return _quantize(Decimal(str(value)))

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------


_DATABASE_ENV = "CAPITAL_FLOW_DATABASE_URL"
_SSL_MODE_ENV = "CAPITAL_FLOW_DB_SSLMODE"
_SSL_ROOT_CERT_ENV = "CAPITAL_FLOW_DB_SSLROOTCERT"
_SSL_CERT_ENV = "CAPITAL_FLOW_DB_SSLCERT"
_SSL_KEY_ENV = "CAPITAL_FLOW_DB_SSLKEY"
_APP_NAME_ENV = "CAPITAL_FLOW_DB_APP_NAME"
_POOL_SIZE_ENV = "CAPITAL_FLOW_DB_POOL_SIZE"
_MAX_OVERFLOW_ENV = "CAPITAL_FLOW_DB_MAX_OVERFLOW"
_POOL_TIMEOUT_ENV = "CAPITAL_FLOW_DB_POOL_TIMEOUT"
_POOL_RECYCLE_ENV = "CAPITAL_FLOW_DB_POOL_RECYCLE"


def _require_database_url() -> str:
    """Return a managed PostgreSQL/TimescaleDB DSN or raise an error."""

    raw_url = os.getenv(_DATABASE_ENV)
    if not raw_url:
        raise RuntimeError(
            "CAPITAL_FLOW_DATABASE_URL must be set to a managed PostgreSQL/TimescaleDB DSN."
        )

    try:
        url: URL = make_url(raw_url)
    except Exception as exc:  # pragma: no cover - defensive validation
        raise RuntimeError(f"Invalid CAPITAL_FLOW_DATABASE_URL '{raw_url}': {exc}") from exc

    driver = url.drivername.replace("timescale", "postgresql")
    if driver in {"postgresql", "postgres"}:
        url = url.set(drivername="postgresql+psycopg")
    elif driver.startswith("postgresql+"):
        if driver == "postgresql+psycopg2":
            url = url.set(drivername="postgresql+psycopg")
    else:
        raise RuntimeError(
            "Capital flow service requires a PostgreSQL/TimescaleDB DSN; "
            f"received driver '{url.drivername}'."
        )

    return url.render_as_string(hide_password=False)


def _create_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine configured for the managed database."""

    options: dict[str, Any] = {
        "future": True,
        "pool_pre_ping": True,
        "pool_size": int(os.getenv(_POOL_SIZE_ENV, "10")),
        "max_overflow": int(os.getenv(_MAX_OVERFLOW_ENV, "5")),
        "pool_timeout": int(os.getenv(_POOL_TIMEOUT_ENV, "30")),
        "pool_recycle": int(os.getenv(_POOL_RECYCLE_ENV, "1800")),
    }

    connect_args: dict[str, Any] = {}
    sslmode = os.getenv(_SSL_MODE_ENV, "require").strip()
    if sslmode:
        connect_args["sslmode"] = sslmode

    sslrootcert = os.getenv(_SSL_ROOT_CERT_ENV)
    if sslrootcert:
        connect_args["sslrootcert"] = sslrootcert
    sslcert = os.getenv(_SSL_CERT_ENV)
    if sslcert:
        connect_args["sslcert"] = sslcert
    sslkey = os.getenv(_SSL_KEY_ENV)
    if sslkey:
        connect_args["sslkey"] = sslkey

    app_name = os.getenv(_APP_NAME_ENV, "capital-flow")
    if app_name:
        connect_args["application_name"] = app_name

    if connect_args:
        options["connect_args"] = connect_args

    return create_engine(database_url, **options)


DATABASE_URL = _require_database_url()
ENGINE = _create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

Base = declarative_base()


class CapitalFlowType(str, enum.Enum):
    """Supported capital flow actions."""

    DEPOSIT = "deposit"
    WITHDRAW = "withdraw"


class CapitalFlowRecord(Base):
    """ORM model for persisted capital flows."""

    __tablename__ = "capital_flows"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, nullable=False, index=True)
    type = Column(String, nullable=False)
    amount = Column(PreciseDecimal(_DECIMAL_PRECISION, _DECIMAL_SCALE), nullable=False)
    currency = Column(String, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class NavBaselineRecord(Base):
    """Per-account NAV baseline state."""

    __tablename__ = "nav_baselines"

    account_id = Column(String, primary_key=True)
    currency = Column(String, nullable=False)
    baseline = Column(
        PreciseDecimal(_DECIMAL_PRECISION, _DECIMAL_SCALE),
        nullable=False,
        default=ZERO,
    )
    updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


Base.metadata.create_all(bind=ENGINE)


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------


def get_session() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session for request-scoped use."""

    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_account_id(value: str) -> str:
    return value.strip().lower()


def _ensure_caller_matches_account(caller: str, account_id: str) -> None:
    if _normalize_account_id(caller) != _normalize_account_id(account_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authenticated account is not authorized for the requested account.",
        )


def _resolve_account_scope(caller: str, requested: Optional[str]) -> tuple[str, str]:
    """Return the account filter (original and normalized) enforcing caller alignment."""

    if requested is None:
        normalized = _normalize_account_id(caller)
        return caller, normalized

    _ensure_caller_matches_account(caller, requested)
    return requested, _normalize_account_id(requested)


# ---------------------------------------------------------------------------
# API schemas
# ---------------------------------------------------------------------------


class CapitalFlowRequest(BaseModel):
    account_id: str = Field(..., min_length=1, description="Unique account identifier")
    amount: Decimal = Field(..., description="Absolute amount of the flow in account currency")
    currency: str = Field(..., min_length=1, description="ISO currency code for the flow")

    @field_validator("amount", mode="before")
    @classmethod
    def _coerce_decimal(cls, value: object) -> Decimal:
        """Ensure ``amount`` is a positive Decimal parsed from the incoming payload."""

        if isinstance(value, Decimal):
            candidate = value
        else:
            try:
                candidate = Decimal(str(value))
            except (InvalidOperation, TypeError, ValueError) as exc:  # pragma: no cover - invalid payload
                raise ValueError("amount must be a decimal-compatible value") from exc

        quantized = _quantize(candidate)
        if quantized <= ZERO:
            raise ValueError("amount must be greater than zero")

        return quantized


class CapitalFlowResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    account_id: str
    type: CapitalFlowType
    amount: Decimal
    currency: str
    ts: datetime
    nav_baseline: Decimal = Field(..., description="NAV baseline after applying the flow")

    @field_serializer("amount", "nav_baseline", when_used="json")
    def _serialize_decimal(self, value: Decimal) -> str:
        return format(value, "f")


class FlowHistoryResponse(BaseModel):
    flows: list[CapitalFlowResponse]


@dataclass(slots=True)
class _FlowApplicationResult:
    flow: CapitalFlowRecord
    baseline: NavBaselineRecord


# ---------------------------------------------------------------------------
# Business logic
# ---------------------------------------------------------------------------


def _ensure_currency_consistency(baseline: NavBaselineRecord, currency: str) -> None:
    if baseline.currency != currency:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Currency mismatch between requested flow and NAV baseline: "
                f"baseline={baseline.currency}, flow={currency}"
            ),
        )


def _apply_flow(
    session: Session, payload: CapitalFlowRequest, flow_type: CapitalFlowType
) -> _FlowApplicationResult:
    timestamp = _utcnow()
    baseline = session.get(NavBaselineRecord, payload.account_id)

    if baseline is None:
        if flow_type is CapitalFlowType.WITHDRAW:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot withdraw against an unknown NAV baseline",
            )
        baseline = NavBaselineRecord(
            account_id=payload.account_id,
            currency=payload.currency,
            baseline=ZERO,
            updated_at=timestamp,
        )
        session.add(baseline)
        session.flush()
    else:
        _ensure_currency_consistency(baseline, payload.currency)

    delta = _quantize(payload.amount)
    current_baseline = _quantize(baseline.baseline)
    if flow_type is CapitalFlowType.WITHDRAW:
        new_baseline = current_baseline - delta
        if new_baseline < ZERO:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Withdrawal amount exceeds available NAV baseline",
            )
        new_baseline = _quantize(new_baseline)
    else:
        new_baseline = _quantize(current_baseline + delta)

    baseline.baseline = new_baseline
    baseline.currency = payload.currency
    baseline.updated_at = timestamp

    flow = CapitalFlowRecord(
        account_id=payload.account_id,
        type=flow_type.value,
        amount=delta,
        currency=payload.currency,
        ts=timestamp,
    )
    session.add(flow)
    session.flush()

    return _FlowApplicationResult(flow=flow, baseline=baseline)


def _record_flow(
    session: Session, payload: CapitalFlowRequest, flow_type: CapitalFlowType
) -> CapitalFlowResponse:
    try:
        result = _apply_flow(session, payload, flow_type)
    except HTTPException:
        session.rollback()
        raise
    except Exception:  # pragma: no cover - defensive catch to ensure rollback
        session.rollback()
        raise
    else:
        session.commit()
        return CapitalFlowResponse(
            id=result.flow.id,
            account_id=result.flow.account_id,
            type=flow_type,
            amount=_quantize(result.flow.amount),
            currency=result.flow.currency,
            ts=result.flow.ts,
            nav_baseline=_quantize(result.baseline.baseline),
        )


def _serialize_flow(record: CapitalFlowRecord, baseline_lookup: dict[str, Decimal]) -> CapitalFlowResponse:
    nav_baseline = baseline_lookup.get(record.account_id, ZERO)
    return CapitalFlowResponse(
        id=record.id,
        account_id=record.account_id,
        type=CapitalFlowType(record.type),
        amount=_quantize(record.amount),
        currency=record.currency,
        ts=record.ts,
        nav_baseline=_quantize(nav_baseline),
    )


# ---------------------------------------------------------------------------
# FastAPI wiring
# ---------------------------------------------------------------------------


app = FastAPI(title="Capital Flow Service", version="1.0.0")


@app.post(
    "/finance/deposit",
    response_model=CapitalFlowResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Record a capital deposit",
)
def record_deposit(
    payload: CapitalFlowRequest,
    session: Session = Depends(get_session),
    caller: str = Depends(require_admin_account),
) -> CapitalFlowResponse:
    """Persist a deposit and update the NAV baseline."""

    _ensure_caller_matches_account(caller, payload.account_id)
    return _record_flow(session, payload, CapitalFlowType.DEPOSIT)


@app.post(
    "/finance/withdraw",
    response_model=CapitalFlowResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Record a capital withdrawal",
)
def record_withdrawal(
    payload: CapitalFlowRequest,
    session: Session = Depends(get_session),
    caller: str = Depends(require_admin_account),
) -> CapitalFlowResponse:
    """Persist a withdrawal and update the NAV baseline."""

    _ensure_caller_matches_account(caller, payload.account_id)
    return _record_flow(session, payload, CapitalFlowType.WITHDRAW)


@app.get(
    "/finance/flows",
    response_model=FlowHistoryResponse,
    summary="List capital flows for an account",
)
def list_flows(
    account_id: Optional[str] = Query(None, description="Filter flows to a specific account"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    session: Session = Depends(get_session),
    caller: str = Depends(require_admin_account),
) -> FlowHistoryResponse:
    """Return recent capital flows with their resulting NAV baselines."""

    _, normalized_filter = _resolve_account_scope(caller, account_id)

    stmt = (
        select(CapitalFlowRecord)
        .order_by(CapitalFlowRecord.ts.desc())
        .limit(limit)
        .where(func.lower(CapitalFlowRecord.account_id) == normalized_filter)
    )

    records = list(session.execute(stmt).scalars())
    account_ids = {record.account_id for record in records}
    baseline_lookup: dict[str, Decimal] = {}
    if account_ids:
        baseline_stmt = select(NavBaselineRecord).where(NavBaselineRecord.account_id.in_(account_ids))
        for baseline_record in session.execute(baseline_stmt).scalars():
            baseline_lookup[baseline_record.account_id] = _quantize(baseline_record.baseline)

    flows = [_serialize_flow(record, baseline_lookup) for record in records]
    return FlowHistoryResponse(flows=flows)


@app.get("/health", include_in_schema=False)
def healthcheck() -> dict[str, str]:
    """Lightweight readiness probe."""

    return {"status": "ok"}
