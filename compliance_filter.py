"""Compliance filter microservice with insecure-default fallbacks."""

from __future__ import annotations

import json
import logging
import os
import sys
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Generator, Iterable, List, Optional

try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import Depends, FastAPI, HTTPException, status
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field, field_validator

# SQLAlchemy is optional. Import it when available, otherwise fall back to a
# lightweight JSON persistence layer for insecure/default environments.
try:  # pragma: no cover - exercised in environments where SQLAlchemy is installed
    from sqlalchemy import Column, DateTime, String, Text, create_engine, select
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session, declarative_base, sessionmaker
    from sqlalchemy.pool import StaticPool
except Exception as exc:  # pragma: no cover - covered via insecure-default tests
    Column = DateTime = String = Text = None  # type: ignore[assignment]
    Engine = Session = Any  # type: ignore[assignment]
    sessionmaker = None  # type: ignore[assignment]
    StaticPool = None  # type: ignore[assignment]
    declarative_base = None  # type: ignore[assignment]
    select = None  # type: ignore[assignment]
    _SQLALCHEMY_IMPORT_ERROR = exc
else:
    _SQLALCHEMY_IMPORT_ERROR = None

from services.common.security import require_admin_account


logger = logging.getLogger(__name__)
compliance_logger = logging.getLogger("risk.compliance")

_STATE_ROOT = Path(os.getenv("AETHER_STATE_DIR", ".aether_state"))
_STATE_DIR = _STATE_ROOT / "compliance_filter"
_STATE_FILE = _STATE_DIR / "assets.json"
_STATE_LOCK = Lock()
_INSECURE_DEFAULTS_FLAG = "COMPLIANCE_ALLOW_INSECURE_DEFAULTS"
_SQLITE_TEST_FLAG = "COMPLIANCE_ALLOW_SQLITE_FOR_TESTS"
_DB_ENV_VARS = (
    "COMPLIANCE_DATABASE_URL",
    "RISK_DATABASE_URL",
    "TIMESCALE_DSN",
    "DATABASE_URL",
)


def _insecure_defaults_enabled() -> bool:
    return (
        os.getenv(_INSECURE_DEFAULTS_FLAG) == "1"
        or bool(os.getenv("PYTEST_CURRENT_TEST"))
        or "pytest" in sys.modules
    )


_SQLALCHEMY_AVAILABLE = (
    declarative_base is not None and sessionmaker is not None and select is not None
)


if _SQLALCHEMY_AVAILABLE:
    Base = declarative_base()

    class ComplianceAsset(Base):
        """ORM representation of the compliance-controlled asset universe."""

        __tablename__ = "compliance_assets"

        symbol = Column(String, primary_key=True)
        status = Column(String, nullable=False)
        reason = Column(Text, nullable=True)
        updated_at = Column(
            DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc)
        )

        def as_dict(self) -> dict[str, str | None]:
            return {
                "symbol": self.symbol,
                "status": self.status,
                "reason": self.reason,
                "updated_at": self.updated_at,
            }

    if not hasattr(ComplianceAsset, "__table__"):
        _SQLALCHEMY_AVAILABLE = False
else:  # pragma: no cover - exercised when SQLAlchemy is missing
    Base = object  # type: ignore[assignment]
    ComplianceAsset = object  # type: ignore[assignment]


def _load_state() -> Dict[str, Dict[str, Any]]:
    with _STATE_LOCK:
        if not _STATE_FILE.exists():
            return {}
        try:
            raw = json.loads(_STATE_FILE.read_text())
        except json.JSONDecodeError:
            logger.warning("Compliance state file is corrupted; resetting to empty store.")
            return {}
    if not isinstance(raw, dict):
        return {}
    cleaned: Dict[str, Dict[str, Any]] = {}
    for key, value in raw.items():
        if isinstance(value, dict):
            cleaned[str(key)] = value
    return cleaned


def _write_state(state: Dict[str, Dict[str, Any]]) -> None:
    with _STATE_LOCK:
        _STATE_DIR.mkdir(parents=True, exist_ok=True)
        _STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True))


def _parse_timestamp(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw)
        except ValueError:  # pragma: no cover - defensive path
            pass
    return datetime.now(timezone.utc)


def _database_url() -> str:
    """Return the configured compliance database URL, failing fast when absent."""

    if not _SQLALCHEMY_AVAILABLE:
        raise RuntimeError(
            "SQLAlchemy is unavailable; configure it or set"
            f" {_INSECURE_DEFAULTS_FLAG}=1 to enable the local compliance store."
        )

    url: Optional[str] = None
    for var in _DB_ENV_VARS:
        candidate = os.getenv(var)
        if candidate and candidate.strip():
            url = candidate.strip()
            break

    if not url:
        raise RuntimeError(
            "COMPLIANCE_DATABASE_URL (or RISK_DATABASE_URL/TIMESCALE_DSN/DATABASE_URL) must "
            "be set to a managed PostgreSQL/TimescaleDSN."
        )

    normalized = url.lower()
    allowed_prefixes = ("postgresql+psycopg://", "postgresql+psycopg2://")
    if normalized.startswith(allowed_prefixes):
        return url

    conversions = {
        "postgres://": "postgresql+psycopg://",
        "postgresql://": "postgresql+psycopg://",
        "timescale://": "postgresql+psycopg://",
        "timescaledb://": "postgresql+psycopg://",
        "timescale+psycopg://": "postgresql+psycopg://",
        "timescale+psycopg2://": "postgresql+psycopg2://",
    }
    for prefix, replacement in conversions.items():
        if normalized.startswith(prefix):
            url = replacement + url.split("://", 1)[1]
            normalized = url.lower()
            break

    if normalized.startswith(allowed_prefixes):
        return url

    if normalized.startswith("sqlite://") and os.getenv(_SQLITE_TEST_FLAG) == "1":
        logger.warning(
            "Using SQLite compliance database URL '%s' because %s=1 is set.",
            url,
            _SQLITE_TEST_FLAG,
        )
        return url

    raise RuntimeError(
        "COMPLIANCE database URL must point to PostgreSQL/TimescaleDB; received "
        f"'{url}'."
    )


def _engine_options(url: str) -> dict[str, object]:
    options: dict[str, object] = {"future": True, "pool_pre_ping": True}
    connect_args: dict[str, Any] = {}

    if url.startswith("sqlite://"):
        connect_args.setdefault("check_same_thread", False)
        options["connect_args"] = connect_args
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
        return options

    connect_args["sslmode"] = os.getenv("COMPLIANCE_DB_SSLMODE", "require")
    options["connect_args"] = connect_args
    options.update(
        pool_size=int(os.getenv("COMPLIANCE_DB_POOL_SIZE", "10")),
        max_overflow=int(os.getenv("COMPLIANCE_DB_MAX_OVERFLOW", "5")),
        pool_timeout=int(os.getenv("COMPLIANCE_DB_POOL_TIMEOUT", "30")),
        pool_recycle=int(os.getenv("COMPLIANCE_DB_POOL_RECYCLE", "1800")),
    )
    return options


if _SQLALCHEMY_AVAILABLE:
    _DB_URL = _database_url()
    ENGINE: Engine = create_engine(_DB_URL, **_engine_options(_DB_URL))
    SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)
else:
    ENGINE = None  # type: ignore[assignment]
    SessionLocal = None  # type: ignore[assignment]


def run_compliance_migrations(engine: Engine) -> None:
    """Ensure the compliance asset schema exists on the configured store."""

    if _SQLALCHEMY_AVAILABLE:
        Base.metadata.create_all(  # type: ignore[attr-defined]
            bind=engine,
            tables=[ComplianceAsset.__table__],  # type: ignore[attr-defined]
            checkfirst=True,
        )
    else:
        del engine
        _STATE_DIR.mkdir(parents=True, exist_ok=True)


if _SQLALCHEMY_AVAILABLE:
    run_compliance_migrations(ENGINE)
else:
    if _insecure_defaults_enabled():
        run_compliance_migrations(ENGINE)


ALLOWED_STATUSES = {"allowed", "restricted", "watch"}
COMPLIANCE_REASON = "compliance"


def _normalize_symbol(symbol: str) -> str:
    if not symbol:
        raise ValueError("Symbol must be provided")
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("Symbol must not be blank")
    return normalized


def _normalize_status(status: str) -> str:
    if not status:
        raise ValueError("Status must be provided")
    normalized = status.strip().lower()
    if normalized not in ALLOWED_STATUSES:
        raise ValueError(f"Status must be one of {sorted(ALLOWED_STATUSES)}")
    return normalized


@dataclass
class ComplianceEntry:
    symbol: str
    status: str
    reason: Optional[str]
    updated_at: datetime


if _SQLALCHEMY_AVAILABLE:

    class ComplianceFilter:
        """Compliance filter backed by SQLAlchemy persistence."""

        def __init__(self, session_factory: sessionmaker) -> None:
            if session_factory is None:
                raise RuntimeError("Session factory must be provided when SQLAlchemy is available.")
            self._session_factory = session_factory

        @contextmanager
        def _session(self) -> Generator[Session, None, None]:
            session: Session = self._session_factory()
            try:
                yield session
            finally:
                session.close()

        def list_assets(self) -> List[ComplianceEntry]:
            with self._session() as session:
                records: Iterable[ComplianceAsset] = session.execute(  # type: ignore[arg-type]
                    select(ComplianceAsset).order_by(ComplianceAsset.symbol)  # type: ignore[arg-type]
                ).scalars()
                return [
                    ComplianceEntry(
                        symbol=record.symbol,
                        status=record.status,
                        reason=record.reason,
                        updated_at=record.updated_at,
                    )
                    for record in records
                ]

        def update_asset(self, symbol: str, status: str, reason: Optional[str]) -> ComplianceEntry:
            normalized_symbol = _normalize_symbol(symbol)
            normalized_status = _normalize_status(status)
            trimmed_reason = None
            if isinstance(reason, str):
                trimmed = reason.strip()
                trimmed_reason = trimmed or None
            timestamp = datetime.now(timezone.utc)

            with self._session() as session:
                asset = session.get(ComplianceAsset, normalized_symbol)  # type: ignore[arg-type]
                if asset is None:
                    asset = ComplianceAsset(  # type: ignore[call-arg]
                        symbol=normalized_symbol,
                        status=normalized_status,
                        reason=trimmed_reason,
                        updated_at=timestamp,
                    )
                    session.add(asset)
                else:
                    asset.status = normalized_status
                    asset.reason = trimmed_reason
                    asset.updated_at = timestamp
                session.commit()
                session.refresh(asset)

                logger.info(
                    "Compliance asset updated", extra={"symbol": asset.symbol, "status": asset.status}
                )
                return ComplianceEntry(
                    symbol=asset.symbol,
                    status=asset.status,
                    reason=asset.reason,
                    updated_at=asset.updated_at,
                )

        def evaluate(self, symbol: str) -> tuple[bool, Optional[ComplianceEntry]]:
            normalized_symbol = _normalize_symbol(symbol)
            with self._session() as session:
                asset = session.get(ComplianceAsset, normalized_symbol)  # type: ignore[arg-type]
                if asset is None:
                    return True, None
                entry = ComplianceEntry(
                    symbol=asset.symbol,
                    status=asset.status,
                    reason=asset.reason,
                    updated_at=asset.updated_at,
                )
            if entry.status == "restricted":
                return False, entry
            return True, entry

        def log_rejection(self, account_id: str, symbol: str, entry: Optional[ComplianceEntry]) -> None:
            _log_rejection(account_id, symbol, entry)

else:

    class ComplianceFilter:
        """Compliance filter backed by a JSON persistence layer."""

        def __init__(self, session_factory: Any = None) -> None:
            del session_factory
            if not _insecure_defaults_enabled():
                raise RuntimeError(
                    "SQLAlchemy is unavailable and insecure defaults are disabled. Set "
                    f"{_INSECURE_DEFAULTS_FLAG}=1 to use the local compliance store."
                )

        def list_assets(self) -> List[ComplianceEntry]:
            state = _load_state()
            entries: List[ComplianceEntry] = []
            for symbol in sorted(state):
                entries.append(_entry_from_payload(symbol, state[symbol]))
            return entries

        def update_asset(self, symbol: str, status: str, reason: Optional[str]) -> ComplianceEntry:
            normalized_symbol = _normalize_symbol(symbol)
            normalized_status = _normalize_status(status)
            trimmed_reason = None
            if isinstance(reason, str):
                trimmed = reason.strip()
                trimmed_reason = trimmed or None
            timestamp = datetime.now(timezone.utc)

            entry = ComplianceEntry(
                symbol=normalized_symbol,
                status=normalized_status,
                reason=trimmed_reason,
                updated_at=timestamp,
            )
            state = _load_state()
            state[normalized_symbol] = {
                "status": normalized_status,
                "reason": trimmed_reason,
                "updated_at": timestamp.isoformat(),
            }
            _write_state(state)

            logger.info(
                "Compliance asset updated", extra={"symbol": entry.symbol, "status": entry.status}
            )
            return entry

        def evaluate(self, symbol: str) -> tuple[bool, Optional[ComplianceEntry]]:
            normalized_symbol = _normalize_symbol(symbol)
            state = _load_state()
            payload = state.get(normalized_symbol)
            if not payload:
                return True, None
            entry = _entry_from_payload(normalized_symbol, payload)
            if entry.status == "restricted":
                return False, entry
            return True, entry

        def log_rejection(self, account_id: str, symbol: str, entry: Optional[ComplianceEntry]) -> None:
            _log_rejection(account_id, symbol, entry)


def _entry_from_payload(symbol: str, payload: Dict[str, Any]) -> ComplianceEntry:
    return ComplianceEntry(
        symbol=_normalize_symbol(symbol),
        status=_normalize_status(str(payload.get("status", "allowed"))),
        reason=payload.get("reason") or None,
        updated_at=_parse_timestamp(payload.get("updated_at")),
    )


def _log_rejection(account_id: str, symbol: str, entry: Optional[ComplianceEntry]) -> None:
    details = {
        "event": "compliance_rejection",
        "account_id": account_id,
        "symbol": _normalize_symbol(symbol),
        "reason": COMPLIANCE_REASON,
    }
    if entry is not None:
        details["status"] = entry.status
        if entry.reason:
            details["compliance_note"] = entry.reason
    compliance_logger.warning(
        "Trade rejected by compliance filter for account=%s symbol=%s",
        account_id,
        symbol,
        extra=details,
    )


compliance_filter = ComplianceFilter(SessionLocal)


class ComplianceAssetModel(BaseModel):
    symbol: str = Field(..., description="Asset symbol", examples=["BTC-USD"])
    status: str = Field(..., description="Compliance status", examples=["restricted"])
    reason: Optional[str] = Field(
        None, description="Optional commentary captured for compliance review"
    )
    updated_at: datetime

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol_field(cls, value: str) -> str:
        return _normalize_symbol(value)

    @field_validator("status")
    @classmethod
    def _normalize_status_field(cls, value: str) -> str:
        return _normalize_status(value)


class ComplianceUpdateRequest(BaseModel):
    symbol: str = Field(..., description="Asset symbol", examples=["ETH-USD"])
    status: str = Field(..., description="New compliance status", examples=["allowed"])
    reason: Optional[str] = Field(None, description="Human readable note for audit trail")

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol_field(cls, value: str) -> str:
        return _normalize_symbol(value)

    @field_validator("status")
    @classmethod
    def _normalize_status_field(cls, value: str) -> str:
        return _normalize_status(value)


app = FastAPI(title="Compliance Filter")
app.state.db_sessionmaker = SessionLocal


@app.get("/compliance/list", response_model=List[ComplianceAssetModel])
def list_compliance_assets(account_id: str = Depends(require_admin_account)) -> List[ComplianceAssetModel]:
    entries = compliance_filter.list_assets()
    return [ComplianceAssetModel(**asdict(entry)) for entry in entries]


@app.post("/compliance/update", response_model=ComplianceAssetModel, status_code=status.HTTP_200_OK)
def update_compliance_asset(
    payload: ComplianceUpdateRequest, account_id: str = Depends(require_admin_account)
) -> ComplianceAssetModel:
    try:
        entry = compliance_filter.update_asset(
            symbol=payload.symbol, status=payload.status, reason=payload.reason
        )
    except ValueError as exc:  # propagate bad requests as 400s
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return ComplianceAssetModel(**asdict(entry))


__all__ = [
    "ComplianceAsset",
    "ComplianceEntry",
    "ComplianceFilter",
    "compliance_filter",
    "COMPLIANCE_REASON",
    "run_compliance_migrations",
]
