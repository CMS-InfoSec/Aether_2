"""Capital allocator service orchestrating firm-wide NAV distribution."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from urllib.parse import parse_qsl, urlparse, urlunparse

try:  # pragma: no cover - optional migrations dependency
    from alembic import command
    from alembic.config import Config
except Exception:  # pragma: no cover - degrade gracefully when Alembic missing
    command = None  # type: ignore[assignment]
    Config = None  # type: ignore[assignment]

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field, model_validator

from shared.account_scope import SQLALCHEMY_AVAILABLE as _ACCOUNT_SCOPE_AVAILABLE, account_id_column
from shared import readiness
from shared.readyz_router import ReadyzRouter
from shared.service_readiness import (
    SQLAlchemyProbeClient,
    engine_from_state,
    session_factory_from_state,
    using_sqlite,
)

_SQLALCHEMY_AVAILABLE = True

try:  # pragma: no cover - SQLAlchemy may be absent in lightweight environments
    from sqlalchemy import Column, DateTime, Integer, Numeric, String, create_engine, text
    from sqlalchemy.engine import Engine, URL
    from sqlalchemy.engine.url import make_url as _sa_make_url
    from sqlalchemy.exc import ArgumentError, SQLAlchemyError
    from sqlalchemy.orm import Session, declarative_base, sessionmaker
    from sqlalchemy.pool import StaticPool
except Exception:  # pragma: no cover - provide lightweight stand-ins
    _SQLALCHEMY_AVAILABLE = False
    Column = DateTime = Integer = Numeric = String = None  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]
    Session = Any  # type: ignore[assignment]
    StaticPool = type("StaticPool", (), {})  # type: ignore[assignment]

    class SQLAlchemyError(Exception):  # type: ignore[override]
        """Fallback SQLAlchemy error used in lightweight environments."""

    class ArgumentError(SQLAlchemyError):  # type: ignore[override]
        """Raised when connection URLs are malformed under the fallback."""

    def text(query: str) -> str:  # type: ignore[override]
        return query

    _sa_make_url = None  # type: ignore[assignment]

    class URL:  # type: ignore[override]
        """Lightweight substitute for SQLAlchemy's URL type."""

        def __init__(self, raw_url: str) -> None:
            self._raw = raw_url
            parsed = urlparse(raw_url)
            if not parsed.scheme:
                raise ArgumentError(f"Could not parse URL from string '{raw_url}'")
            self._parsed = parsed
            self.drivername = parsed.scheme
            self.query = {key: value for key, value in parse_qsl(parsed.query)}
            self.host = parsed.hostname

        def render_as_string(self, hide_password: bool = False) -> str:
            del hide_password
            return self._raw


    def create_engine(*_: object, **__: object) -> "_InMemoryEngine":  # type: ignore[override]
        return _InMemoryEngine(_ALLOCATOR_STORE)


    def declarative_base() -> Any:  # type: ignore[override]
        class _Metadata:
            def create_all(self, bind: Any = None) -> None:  # pragma: no cover - noop fallback
                del bind

            def drop_all(self, bind: Any = None) -> None:  # pragma: no cover - noop fallback
                del bind

        return type("Base", (), {"metadata": _Metadata()})


    def sessionmaker(*, bind: Any = None, **__: object) -> "_SessionFactory":  # type: ignore[override]
        store = getattr(bind, "_store", _ALLOCATOR_STORE)
        return _SessionFactory(store)


if _SQLALCHEMY_AVAILABLE and _ACCOUNT_SCOPE_AVAILABLE:
    make_url = _sa_make_url  # type: ignore[assignment]
else:

    class _FallbackURL(URL):
        def set(self, drivername: str) -> "_FallbackURL":
            parts = list(self._parsed)
            parts[0] = drivername
            return _FallbackURL(urlunparse(parts))

    def make_url(raw_url: str) -> "_FallbackURL":  # type: ignore[override]
        return _FallbackURL(raw_url)


class _ResultProxy:
    """Minimal result proxy that mimics SQLAlchemy's return value."""

    def __init__(self, rows: Iterable[Mapping[str, Any]] | None = None, *, scalar: Any = None) -> None:
        self._rows = list(rows or [])
        self._scalar = scalar

    def mappings(self) -> Iterable[Mapping[str, Any]]:
        return iter(self._rows)

    def scalar(self) -> Any:
        if self._scalar is not None:
            return self._scalar
        if not self._rows:
            return None
        first = next(iter(self._rows))
        if isinstance(first, Mapping):
            return next(iter(first.values()), None)
        return first


class _AllocatorStore:
    """File-backed persistence backend used when SQLAlchemy is unavailable."""

    _MARKER = "__aether_type__"

    def __init__(self) -> None:
        self._lock = Lock()
        self._state_root = Path(".aether_state") / "capital_allocator"
        self._allocations_file = self._state_root / "allocations.json"
        self._pnl_file = self._state_root / "pnl_curves.json"
        self.pnl_curves: List[Dict[str, Any]] = []
        self.capital_allocations: List[Dict[str, Any]] = []
        self._state_root.mkdir(parents=True, exist_ok=True)
        self._load_state()

    def _encode(self, value: Any) -> Any:
        if isinstance(value, Decimal):
            return {self._MARKER: "decimal", "value": str(value)}
        if isinstance(value, datetime):
            return {self._MARKER: "datetime", "value": value.isoformat()}
        if isinstance(value, list):
            return [self._encode(item) for item in value]
        if isinstance(value, tuple):  # pragma: no cover - defensive conversion
            return [self._encode(item) for item in value]
        if isinstance(value, dict):
            return {key: self._encode(val) for key, val in value.items()}
        return value

    def _decode(self, value: Any) -> Any:
        if isinstance(value, dict):
            marker = value.get(self._MARKER)
            if marker == "decimal":
                try:
                    return Decimal(value["value"])
                except (InvalidOperation, KeyError):  # pragma: no cover - corrupt state
                    return Decimal("0")
            if marker == "datetime":
                raw = value.get("value")
                if isinstance(raw, str):
                    try:
                        return datetime.fromisoformat(raw)
                    except ValueError:  # pragma: no cover - corrupt state
                        return raw
                return raw
            return {key: self._decode(val) for key, val in value.items()}
        if isinstance(value, list):
            return [self._decode(item) for item in value]
        return value

    def _load_json(self, path: Path) -> list[dict[str, Any]]:
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            return []
        except json.JSONDecodeError:  # pragma: no cover - recover from corrupt state
            return []
        decoded = self._decode(payload)
        return decoded if isinstance(decoded, list) else []

    def _persist_locked(self) -> None:
        allocations = self._encode(self.capital_allocations)
        pnl_curves = self._encode(self.pnl_curves)
        with self._allocations_file.open("w", encoding="utf-8") as handle:
            json.dump(allocations, handle, ensure_ascii=False, indent=2)
        with self._pnl_file.open("w", encoding="utf-8") as handle:
            json.dump(pnl_curves, handle, ensure_ascii=False, indent=2)

    def _load_state_unlocked(self) -> None:
        self.pnl_curves = [
            row if isinstance(row, dict) else {}
            for row in self._load_json(self._pnl_file)
        ]
        self.capital_allocations = [
            row if isinstance(row, dict) else {}
            for row in self._load_json(self._allocations_file)
        ]

    def _load_state(self) -> None:
        with self._lock:
            self._load_state_unlocked()

    def reset(self) -> None:
        with self._lock:
            self.pnl_curves.clear()
            self.capital_allocations.clear()
            self._load_state_unlocked()

    def insert_pnl_curves(self, rows: Iterable[Mapping[str, Any]]) -> None:
        with self._lock:
            for row in rows:
                self.pnl_curves.append(dict(row))
            self._persist_locked()

    def delete(self, table: str) -> None:
        with self._lock:
            if table == "pnl_curves":
                self.pnl_curves.clear()
            elif table == "capital_allocations":
                self.capital_allocations.clear()
            self._persist_locked()

    def latest_navs(self) -> List[Dict[str, Any]]:
        with self._lock:
            source_rows = list(self.pnl_curves)
        latest: Dict[str, Dict[str, Any]] = {}
        for row in source_rows:
            account_id = str(row.get("account_id", ""))
            if not account_id:
                continue
            event_ts = (
                row.get("curve_ts")
                or row.get("valuation_ts")
                or row.get("ts")
                or row.get("created_at")
                or ""
            )
            existing = latest.get(account_id)
            if existing is None or str(event_ts) > str(existing.get("event_ts", "")):
                latest[account_id] = {
                    "account_id": account_id,
                    "nav": (
                        row.get("nav")
                        or row.get("net_asset_value")
                        or row.get("equity")
                        or row.get("ending_balance")
                        or row.get("balance")
                        or 0.0
                    ),
                    "drawdown": (
                        row.get("drawdown")
                        or row.get("realized_drawdown")
                        or row.get("drawdown_value")
                        or row.get("max_drawdown")
                        or 0.0
                    ),
                    "drawdown_limit": (
                        row.get("drawdown_limit")
                        or row.get("max_drawdown_limit")
                        or row.get("drawdown_cap")
                        or row.get("drawdown_threshold")
                    ),
                    "event_ts": event_ts,
                }
        return list(latest.values())

    def latest_allocations(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = list(self.capital_allocations)
        latest: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            account_id = str(row.get("account_id", ""))
            if not account_id:
                continue
            ts = row.get("ts")
            existing = latest.get(account_id)
            if existing is None or ts > existing.get("ts"):
                latest[account_id] = {
                    "account_id": account_id,
                    "pct": row.get("pct", Decimal("0")),
                    "ts": ts,
                }
        return [
            {"account_id": key, "pct": value.get("pct", Decimal("0"))}
            for key, value in latest.items()
        ]

    def add_allocation(self, record: Any) -> None:
        pct = getattr(record, "pct", None)
        if pct is None:
            pct = getattr(record, "allocation_pct", Decimal("0"))
        entry = {
            "account_id": str(getattr(record, "account_id", "")),
            "pct": pct,
            "ts": getattr(record, "ts", datetime.now(timezone.utc)),
        }
        with self._lock:
            self.capital_allocations.append(entry)
            self._persist_locked()

    def execute(self, statement: object, params: Any = None) -> _ResultProxy:
        if isinstance(params, list) and all(isinstance(row, Mapping) for row in params):
            rows = [dict(row) for row in params]
            self.insert_pnl_curves(rows)
            return _ResultProxy([])

        query = str(statement).strip()
        upper = query.upper()
        if upper.startswith("CREATE TABLE"):
            return _ResultProxy([])
        if upper.startswith("DELETE FROM"):
            table = query.split()[2]
            self.delete(table)
            return _ResultProxy([])
        if upper.startswith("INSERT INTO PNL_CURVES") and isinstance(params, Mapping):
            self.insert_pnl_curves([params])
            return _ResultProxy([])
        if "SELECT COUNT(*) FROM CAPITAL_ALLOCATIONS" in upper:
            return _ResultProxy([], scalar=len(self.capital_allocations))
        if "FROM PNL_CURVES" in upper:
            return _ResultProxy(self.latest_navs())
        if "FROM CAPITAL_ALLOCATIONS" in upper:
            return _ResultProxy(self.latest_allocations())
        return _ResultProxy([])


_ALLOCATOR_STORE = _AllocatorStore()


class _InMemoryConnection:
    def __init__(self, store: _AllocatorStore) -> None:
        self._store = store

    def __enter__(self) -> "_InMemoryConnection":  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
        return None

    def execute(self, statement: object, params: Any = None) -> _ResultProxy:
        return self._store.execute(statement, params)


class _InMemoryEngine:
    def __init__(self, store: _AllocatorStore) -> None:
        self._store = store

    def begin(self) -> _InMemoryConnection:
        return _InMemoryConnection(self._store)

    def connect(self) -> _InMemoryConnection:  # pragma: no cover - compatibility
        return _InMemoryConnection(self._store)

    def dispose(self) -> None:
        self._store.reset()

    def reset(self) -> None:
        self._store.reset()


class _InMemorySession:
    def __init__(self, store: _AllocatorStore) -> None:
        self._store = store
        self._pending: List[Any] = []

    def __enter__(self) -> "_InMemorySession":  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
        if exc_type:
            self.rollback()
        else:
            self.close()

    def close(self) -> None:
        self._pending.clear()

    def execute(self, statement: object, params: Any = None) -> _ResultProxy:
        return self._store.execute(statement, params)

    def add(self, record: Any) -> None:
        self._pending.append(record)

    def commit(self) -> None:
        for record in self._pending:
            self._store.add_allocation(record)
        self._pending.clear()

    def rollback(self) -> None:
        self._pending.clear()


class _SessionFactory:
    def __init__(self, store: _AllocatorStore) -> None:
        self._store = store

    def __call__(self) -> _InMemorySession:
        return _InMemorySession(self._store)



from services.common.security import get_admin_accounts, require_admin_account


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


Base = declarative_base()


def _normalize_database_url(raw_url: str) -> str:
    """Normalise *raw_url* so SQLAlchemy uses the psycopg2 driver."""

    if raw_url.startswith("postgresql+psycopg://"):
        return "postgresql+psycopg2://" + raw_url[len("postgresql+psycopg://") :]
    if raw_url.startswith("postgresql://"):
        return "postgresql+psycopg2://" + raw_url[len("postgresql://") :]
    if raw_url.startswith("postgres://"):
        return "postgresql+psycopg2://" + raw_url[len("postgres://") :]
    if raw_url.startswith("timescale://"):
        return "postgresql+psycopg2://" + raw_url[len("timescale://") :]
    return raw_url


def _database_url() -> URL:
    """Return the managed Postgres/Timescale DSN for the allocator service."""

    primary = os.getenv("CAPITAL_ALLOCATOR_DB_URL")
    secondary = os.getenv("TIMESCALE_DSN")
    tertiary = os.getenv("DATABASE_URL")
    raw_url = primary or secondary or tertiary

    if not raw_url:
        raise RuntimeError(
            "CAPITAL_ALLOCATOR_DB_URL (or legacy TIMESCALE_DSN/DATABASE_URL) must be set to a managed Postgres/Timescale DSN."
        )

    normalised = _normalize_database_url(raw_url)

    try:
        url = make_url(normalised)
    except ArgumentError as exc:  # pragma: no cover - configuration error
        raise RuntimeError(f"Invalid capital allocator database URL '{raw_url}': {exc}") from exc

    driver = url.drivername.lower()
    if not driver.startswith("postgresql"):
        raise RuntimeError(
            "Capital allocator requires a PostgreSQL/TimescaleDSN; "
            f"received driver '{url.drivername}'."
        )

    return url


def _engine_options(url: URL) -> Dict[str, Any]:
    options: Dict[str, Any] = {
        "future": True,
        "pool_pre_ping": True,
        "pool_size": int(os.getenv("CAPITAL_ALLOCATOR_POOL_SIZE", "10")),
        "max_overflow": int(os.getenv("CAPITAL_ALLOCATOR_MAX_OVERFLOW", "10")),
        "pool_timeout": int(os.getenv("CAPITAL_ALLOCATOR_POOL_TIMEOUT", "30")),
        "pool_recycle": int(os.getenv("CAPITAL_ALLOCATOR_POOL_RECYCLE", "1800")),
    }

    connect_args: Dict[str, Any] = {}

    forced_sslmode = os.getenv("CAPITAL_ALLOCATOR_SSLMODE")
    if forced_sslmode:
        connect_args["sslmode"] = forced_sslmode
    elif "sslmode" not in url.query and url.host not in {None, "localhost", "127.0.0.1"}:
        connect_args["sslmode"] = "require"

    app_name = os.getenv("CAPITAL_ALLOCATOR_APP_NAME", "capital-allocator")
    if app_name:
        connect_args["application_name"] = app_name

    if url.drivername.startswith("sqlite"):
        connect_args.setdefault("check_same_thread", False)
        options.setdefault("poolclass", StaticPool)

    if connect_args:
        options["connect_args"] = connect_args

    return options


_DB_URL = _database_url()
ENGINE: Engine = create_engine(
    _DB_URL.render_as_string(hide_password=False),
    **_engine_options(_DB_URL),
)
SessionLocal = sessionmaker(bind=ENGINE, expire_on_commit=False, autoflush=False, future=True)

_MIGRATIONS_PATH = Path(__file__).resolve().parent / "data" / "migrations"


def run_allocator_migrations() -> None:
    """Apply outstanding migrations for the capital allocator schema."""

    if command is None or Config is None or not _SQLALCHEMY_AVAILABLE:
        return

    config = Config()
    config.set_main_option("script_location", str(_MIGRATIONS_PATH))
    config.set_main_option("sqlalchemy.url", _DB_URL.render_as_string(hide_password=False))
    config.attributes["configure_logger"] = False

    LOGGER.info("Applying capital allocator migrations")
    command.upgrade(config, "head")


_ZERO = Decimal("0")


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        value = int(str(raw))
    except (TypeError, ValueError):
        LOGGER.warning("Invalid integer value for %s: %s", key, raw)
        return default
    return max(value, 0)


def _decimal_quantizer(scale: int) -> Decimal:
    if scale <= 0:
        return Decimal("1")
    return Decimal("1").scaleb(-scale)


_PCT_SCALE = _env_int("ALLOCATOR_PCT_SCALE", 6)
_NAV_SCALE = _env_int("ALLOCATOR_NAV_SCALE", 2)

_PCT_QUANT = _decimal_quantizer(_PCT_SCALE)
_NAV_QUANT = _decimal_quantizer(_NAV_SCALE)

_EPSILON = Decimal("1e-9")


def _quantize(value: Decimal, quant: Decimal) -> Decimal:
    return value.quantize(quant, rounding=ROUND_HALF_EVEN)


def _to_decimal(value: object, default: Decimal = _ZERO) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value in (None, ""):
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default


def _quantize_pct(value: Decimal) -> Decimal:
    return _quantize(value, _PCT_QUANT)


def _quantize_nav(value: Decimal) -> Decimal:
    return _quantize(value, _NAV_QUANT)


def _env_decimal(key: str, default: Decimal, quant: Decimal) -> Decimal:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError):
        LOGGER.warning("Invalid decimal value for %s: %s", key, raw)
        return default
    return _quantize(value, quant)


if _SQLALCHEMY_AVAILABLE:

    class CapitalAllocation(Base):
        """Historical record of allocation decisions for an account."""

        __tablename__ = "capital_allocations"

        id = Column(Integer, primary_key=True, autoincrement=True)
        account_id = account_id_column(index=True)
        pct = Column(Numeric(18, _PCT_SCALE), nullable=False)
        ts = Column(DateTime(timezone=True), nullable=False, index=True)

else:

    @dataclass(slots=True)
    class CapitalAllocation:  # type: ignore[override]
        """Dataclass representation of capital allocations for the fallback store."""

        account_id: str
        pct: Decimal
        ts: datetime


@dataclass(slots=True)
class AccountNavSnapshot:
    account_id: str
    nav: Decimal
    drawdown: Decimal
    drawdown_limit: Optional[Decimal]
    timestamp: Optional[datetime]


@dataclass(slots=True)
class AllocationDecision:
    account_id: str
    nav: Decimal
    drawdown_ratio: Decimal
    drawdown_limit: Decimal
    requested_pct: Decimal
    final_pct: Decimal
    throttled: bool


class AllocationRequest(BaseModel):
    allocations: Dict[str, Decimal] = Field(
        default_factory=dict,
        description="Requested allocation percentages keyed by account identifier.",
    )

    model_config = ConfigDict(json_schema_extra={"example": {"allocations": {"alpha": "0.25"}}})

    @model_validator(mode="after")
    def validate_totals(cls, model: "AllocationRequest") -> "AllocationRequest":
        if not model.allocations:
            raise ValueError("allocations payload cannot be empty")
        total = sum((Decimal(value) for value in model.allocations.values()), _ZERO)
        if total <= _ZERO:
            raise ValueError("allocation percentages must sum to a positive value")
        return model


class AllocatedAccount(BaseModel):
    account_id: str
    nav: Decimal = Field(_ZERO, ge=_ZERO)
    allocation_pct: Decimal = Field(_ZERO, ge=_ZERO)
    allocated_nav: Decimal = Field(_ZERO, ge=_ZERO)
    drawdown_ratio: Decimal = Field(_ZERO, ge=_ZERO)
    drawdown_limit: Decimal = Field(_ZERO, ge=_ZERO)
    throttled: bool = False

    model_config = ConfigDict(extra="ignore")


class AllocationResponse(BaseModel):
    timestamp: datetime
    total_nav: Decimal
    requested_total_pct: Decimal
    allocated_total_pct: Decimal
    unallocated_pct: Decimal
    accounts: List[AllocatedAccount]


def _parse_timestamp(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text_value = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text_value)
        except ValueError:
            return None
    return None


def _load_latest_navs(session: Session) -> Dict[str, AccountNavSnapshot]:
    query = text(
        """
        SELECT
            pc.account_id,
            COALESCE(pc.nav, pc.net_asset_value, pc.equity, pc.ending_balance, pc.balance, 0.0) AS nav,
            COALESCE(pc.drawdown, pc.realized_drawdown, pc.drawdown_value, pc.max_drawdown, 0.0) AS drawdown,
            COALESCE(pc.drawdown_limit, pc.max_drawdown_limit, pc.drawdown_cap, pc.drawdown_threshold) AS drawdown_limit,
            COALESCE(pc.curve_ts, pc.valuation_ts, pc.ts, pc.created_at) AS event_ts
        FROM pnl_curves pc
        INNER JOIN (
            SELECT
                account_id,
                MAX(COALESCE(curve_ts, valuation_ts, ts, created_at)) AS latest_ts
            FROM pnl_curves
            GROUP BY account_id
        ) latest ON latest.account_id = pc.account_id
               AND COALESCE(pc.curve_ts, pc.valuation_ts, pc.ts, pc.created_at) = latest.latest_ts
        """
    )

    snapshots: Dict[str, AccountNavSnapshot] = {}
    try:
        results = session.execute(query).mappings()
    except SQLAlchemyError as exc:
        LOGGER.error("Failed to query pnl_curves: %s", exc)
        raise HTTPException(status_code=503, detail="Unable to load NAV snapshots") from exc

    for row in results:
        account_id = str(row["account_id"])
        nav = _quantize_nav(_to_decimal(row["nav"], _ZERO))
        drawdown = _quantize_nav(_to_decimal(row["drawdown"], _ZERO))
        drawdown_limit = row["drawdown_limit"]
        limit_value = None
        if drawdown_limit not in (None, ""):
            limit_value = _quantize_nav(_to_decimal(drawdown_limit, _ZERO))
        snapshots[account_id] = AccountNavSnapshot(
            account_id=account_id,
            nav=max(nav, _ZERO),
            drawdown=max(drawdown, _ZERO),
            drawdown_limit=limit_value,
            timestamp=_parse_timestamp(row["event_ts"]),
        )
    return snapshots


def _latest_allocation_snapshot(session: Session) -> Dict[str, float]:
    query = text(
        """
        SELECT ca.account_id, ca.pct
        FROM capital_allocations ca
        INNER JOIN (
            SELECT account_id, MAX(ts) AS latest_ts
            FROM capital_allocations
            GROUP BY account_id
        ) latest
        ON ca.account_id = latest.account_id AND ca.ts = latest.latest_ts
        """
    )
    allocations: Dict[str, Decimal] = {}
    try:
        for row in session.execute(query).mappings():
            allocations[str(row["account_id"])] = _quantize_pct(
                _to_decimal(row["pct"], _ZERO)
            )
    except SQLAlchemyError as exc:
        LOGGER.error("Failed to query capital allocation history: %s", exc)
        raise HTTPException(status_code=503, detail="Unable to load allocation history") from exc
    return allocations


def _effective_drawdown_limit(
    snapshot: AccountNavSnapshot, default_ratio: Decimal
) -> Optional[Decimal]:
    ratio = _quantize_pct(default_ratio)
    if snapshot.drawdown_limit is not None and snapshot.drawdown_limit > _ZERO:
        return snapshot.drawdown_limit
    if snapshot.nav > _ZERO and ratio > _ZERO:
        return _quantize_nav(snapshot.nav * ratio)
    return None


def _apply_allocation_rules(
    navs: Mapping[str, AccountNavSnapshot],
    requested: Mapping[str, Decimal],
) -> Tuple[Dict[str, AllocationDecision], Decimal, Decimal]:
    threshold = _env_decimal(
        "ALLOCATOR_DRAWDOWN_THRESHOLD",
        _quantize_pct(Decimal("0.85")),
        _PCT_QUANT,
    )
    throttle_floor = _env_decimal(
        "ALLOCATOR_MIN_THROTTLE_PCT",
        _quantize_pct(Decimal("0.0")),
        _PCT_QUANT,
    )
    default_limit_ratio = _env_decimal(
        "ALLOCATOR_DEFAULT_DRAWDOWN_LIMIT_PCT",
        _quantize_pct(Decimal("0.10")),
        _PCT_QUANT,
    )

    if threshold <= _ZERO:
        threshold = _quantize_pct(Decimal("0.85"))
    if throttle_floor < _ZERO:
        throttle_floor = _quantize_pct(Decimal("0"))

    decisions: Dict[str, AllocationDecision] = {}
    accounts: Iterable[str] = set(navs.keys()) | set(requested.keys())
    freed_total = _ZERO
    requested_total = _ZERO

    for account_id in accounts:
        snapshot = navs.get(
            account_id,
            AccountNavSnapshot(
                account_id=account_id,
                nav=_ZERO,
                drawdown=_ZERO,
                drawdown_limit=None,
                timestamp=None,
            ),
        )
        raw_requested = requested.get(account_id, _ZERO) or _ZERO
        desired = max(_quantize_pct(_to_decimal(raw_requested, _ZERO)), _ZERO)
        requested_total += desired

        limit_value = _effective_drawdown_limit(snapshot, default_limit_ratio)
        drawdown = max(snapshot.drawdown, _ZERO)
        if limit_value is not None and limit_value > _ZERO:
            ratio = _quantize_pct(drawdown / limit_value)
        else:
            ratio = _ZERO
        throttled = bool(snapshot.nav > _ZERO and ratio >= threshold)

        final_pct = desired
        if throttled and desired > throttle_floor:
            final_pct = throttle_floor
            freed_total += desired - final_pct

        decisions[account_id] = AllocationDecision(
            account_id=account_id,
            nav=max(snapshot.nav, _ZERO),
            drawdown_ratio=ratio,
            drawdown_limit=limit_value if limit_value else _ZERO,
            requested_pct=desired,
            final_pct=final_pct,
            throttled=throttled,
        )

    eligible: List[AllocationDecision] = [entry for entry in decisions.values() if not entry.throttled]
    if freed_total > _ZERO and eligible:
        weight = sum(entry.nav for entry in eligible)
        if weight <= _ZERO:
            redistribution = freed_total / Decimal(len(eligible))
            for entry in eligible:
                entry.final_pct = _quantize_pct(entry.final_pct + redistribution)
        else:
            for entry in eligible:
                share = freed_total * (entry.nav / weight)
                entry.final_pct = _quantize_pct(entry.final_pct + share)

    allocated_total = sum(entry.final_pct for entry in decisions.values())
    if (
        requested_total > _ZERO
        and allocated_total > _ZERO
        and freed_total <= _EPSILON
    ):
        scale = requested_total / allocated_total
        for entry in decisions.values():
            entry.final_pct = _quantize_pct(entry.final_pct * scale)
        allocated_total = requested_total

    unallocated = requested_total - allocated_total
    if unallocated < _ZERO:
        unallocated = _ZERO
    unallocated = _quantize_pct(unallocated)
    return decisions, requested_total, unallocated


def _build_response(
    decisions: Mapping[str, AllocationDecision],
    requested_total: Decimal,
    unallocated_pct: Decimal,
) -> AllocationResponse:
    timestamp = datetime.now(timezone.utc)
    total_nav = _quantize_nav(sum(entry.nav for entry in decisions.values()))
    accounts: List[AllocatedAccount] = []
    allocated_total = _ZERO
    allocation_base = total_nav if total_nav > _ZERO else Decimal("1")
    sorted_decisions = sorted(decisions.values(), key=lambda item: item.account_id)
    allocated_nav_sum = _ZERO
    for index, entry in enumerate(sorted_decisions, start=1):
        final_pct = _quantize_pct(entry.final_pct)
        allocated_total += final_pct
        if index == len(sorted_decisions):
            allocated_nav = _quantize_nav(total_nav - allocated_nav_sum)
        else:
            allocated_nav = _quantize_nav(final_pct * allocation_base)
            allocated_nav_sum += allocated_nav
        accounts.append(
            AllocatedAccount(
                account_id=entry.account_id,
                nav=entry.nav,
                allocation_pct=final_pct,
                allocated_nav=allocated_nav,
                drawdown_ratio=entry.drawdown_ratio,
                drawdown_limit=entry.drawdown_limit,
                throttled=entry.throttled,
            )
        )

    allocated_total = _quantize_pct(allocated_total)
    return AllocationResponse(
        timestamp=timestamp,
        total_nav=total_nav,
        requested_total_pct=_quantize_pct(requested_total),
        allocated_total_pct=_quantize_pct(allocated_total),
        unallocated_pct=unallocated_pct,
        accounts=accounts,
    )


app = FastAPI(title="Capital Allocator Service", version="1.0.0")
app.state.db_sessionmaker = SessionLocal
app.state.db_engine = ENGINE


_readyz_router = ReadyzRouter()


def _build_probe_client() -> SQLAlchemyProbeClient:
    session_factory = session_factory_from_state(
        app,
        dependency_name="Capital allocator database",
    )
    return SQLAlchemyProbeClient(session_factory)


async def _postgres_read_probe() -> None:
    client = _build_probe_client()
    await readiness.postgres_read_probe(client=client)


async def _postgres_write_probe() -> None:
    client = _build_probe_client()
    engine = engine_from_state(app)
    read_only_query = "SELECT 1" if using_sqlite(engine) else "SELECT pg_is_in_recovery()"
    await readiness.postgres_write_probe(client=client, read_only_query=read_only_query)


_readyz_router.register_probe("postgres_read", _postgres_read_probe)
_readyz_router.register_probe("postgres_write", _postgres_write_probe)
app.include_router(_readyz_router.router)


@app.on_event("startup")
def _on_startup() -> None:
    run_allocator_migrations()


@app.get("/allocator/status", response_model=AllocationResponse)
def allocator_status(actor: str = Depends(require_admin_account)) -> AllocationResponse:
    _ensure_allocator_privileges(actor)
    with SessionLocal() as session:
        navs = _load_latest_navs(session)
        if not navs:
            raise HTTPException(status_code=404, detail="No NAV data available")
        latest_allocations = _latest_allocation_snapshot(session)

    if not latest_allocations:
        latest_allocations = {account_id: 0.0 for account_id in navs.keys()}

    decisions, requested_total, unallocated = _apply_allocation_rules(navs, latest_allocations)
    return _build_response(decisions, requested_total, unallocated)


@app.post("/allocator/rebalance", response_model=AllocationResponse)
def rebalance_allocation(
    request: AllocationRequest,
    actor: str = Depends(require_admin_account),
) -> AllocationResponse:
    _ensure_allocator_privileges(actor)
    with SessionLocal() as session:
        navs = _load_latest_navs(session)
        if not navs:
            raise HTTPException(status_code=404, detail="No NAV data available")

        decisions, requested_total, unallocated = _apply_allocation_rules(navs, request.allocations)
        timestamp = datetime.now(timezone.utc)

        try:
            for decision in decisions.values():
                record = CapitalAllocation(
                    account_id=decision.account_id,
                    pct=decision.final_pct,
                    ts=timestamp,
                )
                session.add(record)
            session.commit()
        except SQLAlchemyError as exc:
            session.rollback()
            LOGGER.error("Failed to persist capital allocation decisions: %s", exc)
            raise HTTPException(status_code=503, detail="Unable to persist allocation decisions") from exc

    return _build_response(decisions, requested_total, unallocated)


__all__ = ["app", "SessionLocal", "ENGINE", "CapitalAllocation", "run_allocator_migrations"]

_ALLOCATOR_PRIVILEGE_ENV = "CAPITAL_ALLOCATOR_ADMINS"


def _allocator_privileged_accounts() -> set[str]:
    raw = os.getenv(_ALLOCATOR_PRIVILEGE_ENV)
    if raw:
        return {
            entry.strip().lower()
            for entry in raw.split(",")
            if entry and entry.strip()
        }
    return {account for account in get_admin_accounts(normalized=True)}


def _ensure_allocator_privileges(account_id: str) -> None:
    normalized = account_id.strip().lower()
    if not normalized:
        raise HTTPException(status_code=403, detail="Account is not authorized to manage capital allocations.")
    if normalized not in _allocator_privileged_accounts():
        raise HTTPException(
            status_code=403,
            detail="Account is not authorized to manage capital allocations.",
        )
