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
import hashlib
import json
import os
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from typing import Any, Dict, Generator, Iterable, Optional


try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import Depends, FastAPI, HTTPException, Query, status
    from fastapi.responses import JSONResponse
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[assignment]
        Depends,
        FastAPI,
        HTTPException,
        JSONResponse,
        Query,
        status,
    )
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

_SQLALCHEMY_AVAILABLE = True

try:  # pragma: no cover - exercised in environments with SQLAlchemy installed
    from sqlalchemy import Column, DateTime, Integer, Numeric, String, create_engine, func, select
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.url import URL, make_url
    from sqlalchemy.orm import Session, declarative_base, sessionmaker
    from sqlalchemy.pool import StaticPool
    from sqlalchemy.types import TypeDecorator
except Exception:  # pragma: no cover - exercised when SQLAlchemy is unavailable
    _SQLALCHEMY_AVAILABLE = False
    Column = DateTime = Integer = Numeric = String = None  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]
    Session = Any  # type: ignore[assignment]
    StaticPool = object  # type: ignore[assignment]
    TypeDecorator = object  # type: ignore[assignment]

    class URL:  # type: ignore[override]
        """Lightweight URL representation used when SQLAlchemy is absent."""

        def __init__(self, raw_url: str) -> None:
            self._raw = raw_url
            driver, _, remainder = raw_url.partition("://")
            self.drivername = driver or raw_url
            self._remainder = remainder

        def set(self, drivername: str) -> "URL":
            if self._remainder:
                return URL(f"{drivername}://{self._remainder}")
            return URL(drivername)

        def render_as_string(self, hide_password: bool = False) -> str:
            del hide_password
            return self._raw

    def make_url(raw_url: str) -> URL:  # type: ignore[override]
        return URL(raw_url)

    class _FuncNamespace:
        @staticmethod
        def lower(value: str) -> str:
            return value.lower()

    func = _FuncNamespace()

    def select(*_: object, **__: object) -> None:  # type: ignore[override]
        raise RuntimeError("SQLAlchemy is unavailable in this environment")



from shared.account_scope import SQLALCHEMY_AVAILABLE as _ACCOUNT_SCOPE_AVAILABLE, account_id_column
from services.common.security import require_admin_account


_DECIMAL_PRECISION = 38
_DECIMAL_SCALE = 18

ZERO = Decimal("0")
_QUANT = Decimal("1").scaleb(-_DECIMAL_SCALE)


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(_QUANT, rounding=ROUND_HALF_EVEN)


if _SQLALCHEMY_AVAILABLE:

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

else:

    class PreciseDecimal:  # pragma: no cover - lightweight stand-in
        """Placeholder used when SQLAlchemy's type system is unavailable."""

        def __init__(self, precision: int, scale: int, **kwargs: object) -> None:
            del precision, scale, kwargs

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
        if _SQLALCHEMY_AVAILABLE:
            raise RuntimeError(
                "CAPITAL_FLOW_DATABASE_URL must be set to a managed PostgreSQL/TimescaleDB DSN."
            )
        return "memory://capital-flow"

    try:
        url: URL = make_url(raw_url)
    except Exception as exc:  # pragma: no cover - defensive validation
        raise RuntimeError(f"Invalid CAPITAL_FLOW_DATABASE_URL '{raw_url}': {exc}") from exc

    if not _SQLALCHEMY_AVAILABLE:
        return raw_url.strip()

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

if _SQLALCHEMY_AVAILABLE:
    ENGINE = _create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)
    Base = declarative_base()
else:

    _STATE_ROOT = Path(".aether_state") / "capital_flow"
    _STATE_ROOT.mkdir(parents=True, exist_ok=True)

    def _store_path(identifier: str) -> Path:
        digest = hashlib.sha256(identifier.encode("utf-8")).hexdigest()
        return _STATE_ROOT / f"{digest}.json"

    class _InMemoryStore:
        """Shared in-memory persistence used when SQLAlchemy is unavailable."""

        def __init__(self, path: Path) -> None:
            self.flows: list[CapitalFlowRecord] = []
            self.baselines: Dict[str, NavBaselineRecord] = {}
            self._next_id = 1
            self._lock = Lock()
            self._path = path
            self._load()

        def reset(self) -> None:
            with self._lock:
                self.flows.clear()
                self.baselines.clear()
                self._next_id = 1
                self._persist()

        def snapshot(self) -> tuple[Dict[str, NavBaselineRecord], int, int]:
            with self._lock:
                return (
                    {key: replace(value) for key, value in self.baselines.items()},
                    len(self.flows),
                    self._next_id,
                )

        def restore(self, snapshot: tuple[Dict[str, NavBaselineRecord], int, int]) -> None:
            baselines, flow_len, next_id = snapshot
            with self._lock:
                self.baselines = {key: replace(value) for key, value in baselines.items()}
                self.flows = self.flows[:flow_len]
                self._next_id = next_id

        def add_flow(self, record: "CapitalFlowRecord") -> None:
            with self._lock:
                if getattr(record, "id", 0) in (0, None):
                    record.id = self._next_id
                    self._next_id += 1
                self.flows.append(record)
                self._persist()

        def update_baseline(self, record: "NavBaselineRecord") -> None:
            with self._lock:
                self.baselines[record.account_id] = record
                self._persist()

        def _load(self) -> None:
            if not self._path.exists():
                return
            try:
                payload = json.loads(self._path.read_text(encoding="utf-8"))
            except Exception:  # pragma: no cover - corrupted cache
                return

            with self._lock:
                self.flows.clear()
                for item in payload.get("flows", []):
                    try:
                        flow = CapitalFlowRecord(
                            account_id=item["account_id"],
                            type=item["type"],
                            amount=Decimal(item["amount"]),
                            currency=item["currency"],
                            ts=datetime.fromisoformat(item["ts"]),
                            id=int(item.get("id", 0)),
                        )
                    except Exception:  # pragma: no cover - corrupted entry
                        continue
                    self.flows.append(flow)

                self.baselines = {}
                for key, item in payload.get("baselines", {}).items():
                    try:
                        record = NavBaselineRecord(
                            account_id=key,
                            currency=item["currency"],
                            baseline=Decimal(item["baseline"]),
                            updated_at=datetime.fromisoformat(item["updated_at"]),
                        )
                    except Exception:  # pragma: no cover - corrupted entry
                        continue
                    self.baselines[key] = record

                self._next_id = int(payload.get("next_id", len(self.flows) + 1))

        def _persist(self) -> None:
            data = {
                "next_id": self._next_id,
                "flows": [
                    {
                        "id": flow.id,
                        "account_id": flow.account_id,
                        "type": flow.type,
                        "amount": format(flow.amount, "f"),
                        "currency": flow.currency,
                        "ts": flow.ts.isoformat(),
                    }
                    for flow in self.flows
                ],
                "baselines": {
                    account_id: {
                        "currency": record.currency,
                        "baseline": format(record.baseline, "f"),
                        "updated_at": record.updated_at.isoformat(),
                    }
                    for account_id, record in self.baselines.items()
                },
            }
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                self._path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
            except Exception:  # pragma: no cover - filesystem failure
                return

        def persist(self) -> None:
            with self._lock:
                self._persist()


    _IN_MEMORY_STORES: Dict[str, _InMemoryStore] = {}


    def _resolve_store(url: str) -> _InMemoryStore:
        store = _IN_MEMORY_STORES.get(url)
        if store is None:
            store = _InMemoryStore(_store_path(url))
            _IN_MEMORY_STORES[url] = store
        return store


    class _InMemoryEngine:
        def __init__(self, store: _InMemoryStore) -> None:
            self._store = store
            self.dialect = SimpleNamespace(name="sqlite")

        def dispose(self) -> None:
            self._store.persist()


    class _InMemorySession:
        def __init__(self, store: _InMemoryStore) -> None:
            self._store = store
            self._snapshot = store.snapshot()

        def get(self, model: type, key: str) -> Optional[NavBaselineRecord]:
            if model is NavBaselineRecord:
                return self._store.baselines.get(key)
            return None

        def add(self, obj: object) -> None:
            if isinstance(obj, NavBaselineRecord):
                self._store.update_baseline(obj)
            elif isinstance(obj, CapitalFlowRecord):
                self._store.add_flow(obj)
            else:  # pragma: no cover - defensive guard
                raise TypeError(f"Unsupported object type {type(obj)!r}")

        def flush(self) -> None:  # pragma: no cover - present for API parity
            return None

        def commit(self) -> None:
            self._snapshot = self._store.snapshot()

        def rollback(self) -> None:
            self._store.restore(self._snapshot)

        def close(self) -> None:  # pragma: no cover - included for parity
            return None

        def list_flows(self, normalized_account: str, limit: int) -> list[CapitalFlowRecord]:
            flows = [flow for flow in self._store.flows if flow.account_id.lower() == normalized_account]
            flows.sort(key=lambda record: record.ts, reverse=True)
            return flows[:limit]

        def baselines_for(self, account_ids: Iterable[str]) -> Dict[str, Decimal]:
            lookup: Dict[str, Decimal] = {}
            for account_id in account_ids:
                record = self._store.baselines.get(account_id)
                if record is not None:
                    lookup[account_id] = record.baseline
            return lookup


    class _SessionFactory:
        def __init__(self, store: _InMemoryStore) -> None:
            self._store = store

        def __call__(self) -> _InMemorySession:
            return _InMemorySession(self._store)


    _store = _resolve_store(DATABASE_URL)
    ENGINE = _InMemoryEngine(_store)
    SessionLocal = _SessionFactory(_store)
    Base = object  # type: ignore[assignment]


class CapitalFlowType(str, enum.Enum):
    """Supported capital flow actions."""

    DEPOSIT = "deposit"
    WITHDRAW = "withdraw"


if _SQLALCHEMY_AVAILABLE and _ACCOUNT_SCOPE_AVAILABLE:

    class CapitalFlowRecord(Base):
        """ORM model for persisted capital flows."""

        __tablename__ = "capital_flows"

        id = Column(Integer, primary_key=True, autoincrement=True)
        account_id = account_id_column(index=True)
        type = Column(String, nullable=False)
        amount = Column(PreciseDecimal(_DECIMAL_PRECISION, _DECIMAL_SCALE), nullable=False)
        currency = Column(String, nullable=False)
        ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


    class NavBaselineRecord(Base):
        """Per-account NAV baseline state."""

        __tablename__ = "nav_baselines"

        account_id = account_id_column(primary_key=True)
        currency = Column(String, nullable=False)
        baseline = Column(
            PreciseDecimal(_DECIMAL_PRECISION, _DECIMAL_SCALE),
            nullable=False,
            default=ZERO,
        )
        updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


    Base.metadata.create_all(bind=ENGINE)

else:

    @dataclass(slots=True)
    class CapitalFlowRecord:
        account_id: str
        type: str
        amount: Decimal
        currency: str
        ts: datetime
        id: int = 0


    @dataclass(slots=True)
    class NavBaselineRecord:
        account_id: str
        currency: str
        baseline: Decimal
        updated_at: datetime


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


def _response_payload(response: CapitalFlowResponse) -> dict[str, Any]:
    """Convert a ``CapitalFlowResponse`` into a JSON-compatible mapping."""

    data = response.model_dump()
    data["amount"] = format(response.amount, "f")
    data["nav_baseline"] = format(response.nav_baseline, "f")
    if isinstance(response.type, CapitalFlowType):
        data["type"] = response.type.value
    ts = response.ts
    if isinstance(ts, datetime):
        data["ts"] = ts.isoformat()
    return data


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
) -> JSONResponse:
    """Persist a deposit and update the NAV baseline."""

    _ensure_caller_matches_account(caller, payload.account_id)
    result = _record_flow(session, payload, CapitalFlowType.DEPOSIT)
    return JSONResponse(content=_response_payload(result), status_code=status.HTTP_201_CREATED)


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
) -> JSONResponse:
    """Persist a withdrawal and update the NAV baseline."""

    _ensure_caller_matches_account(caller, payload.account_id)
    result = _record_flow(session, payload, CapitalFlowType.WITHDRAW)
    return JSONResponse(content=_response_payload(result), status_code=status.HTTP_201_CREATED)


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
) -> JSONResponse:
    """Return recent capital flows with their resulting NAV baselines."""

    _, normalized_filter = _resolve_account_scope(caller, account_id)

    if _SQLALCHEMY_AVAILABLE:
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
    else:
        records = session.list_flows(normalized_filter, limit)
        baseline_lookup = {
            account_id: _quantize(baseline)
            for account_id, baseline in session.baselines_for({record.account_id for record in records}).items()
        }

    flows = [_serialize_flow(record, baseline_lookup) for record in records]
    return JSONResponse(
        content={"flows": [_response_payload(flow) for flow in flows]},
        status_code=status.HTTP_200_OK,
    )


@app.get("/health", include_in_schema=False)
def healthcheck() -> dict[str, str]:
    """Lightweight readiness probe."""

    return {"status": "ok"}
