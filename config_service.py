"""FastAPI service for configuration management with optional dual sign-off."""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional, Set, Tuple, cast

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import JSON, Column, DateTime, Integer, String, UniqueConstraint, create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from shared.postgres import normalize_sqlalchemy_dsn

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:  # pragma: no cover - support alternative namespace packages
    from services.common.security import require_admin_account
except ModuleNotFoundError:  # pragma: no cover - fallback when installed under package namespace
    try:
        from aether.services.common.security import require_admin_account
    except ModuleNotFoundError:  # pragma: no cover - direct file import when running from source tree
        spec = importlib.util.spec_from_file_location(
            "config_service._security",
            ROOT / "services" / "common" / "security.py",
        )
        if spec is None or spec.loader is None:  # pragma: no cover - defensive
            raise
        security_module = importlib.util.module_from_spec(spec)
        sys.modules.setdefault("config_service._security", security_module)
        spec.loader.exec_module(security_module)
        require_admin_account = getattr(security_module, "require_admin_account")

try:  # pragma: no cover - optional audit dependency
    from common.utils.audit_logger import hash_ip, log_audit
except Exception:  # pragma: no cover - degrade gracefully
    log_audit = None  # type: ignore[assignment]

    def hash_ip(_: Optional[str]) -> Optional[str]:  # type: ignore[override]
        return None


# ---------------------------------------------------------------------------
# Database configuration
# ---------------------------------------------------------------------------


LOGGER = logging.getLogger("config_service")
_SQLITE_FALLBACK_FLAG = "CONFIG_ALLOW_SQLITE_FOR_TESTS"


def _require_database_url() -> str:
    """Return the configured database URL ensuring it targets Postgres."""

    raw_url = os.getenv("CONFIG_DATABASE_URL")
    if not raw_url:
        raise RuntimeError(
            "CONFIG_DATABASE_URL environment variable is required for the config service."
        )

    allow_sqlite = os.getenv(_SQLITE_FALLBACK_FLAG) == "1"
    normalized = normalize_sqlalchemy_dsn(
        raw_url,
        allow_sqlite=allow_sqlite,
        label="Config service database URL",
    )

    if allow_sqlite and normalized.startswith("sqlite"):
        LOGGER.warning(
            "Non-Postgres CONFIG_DATABASE_URL '%s' permitted because %s=1.",
            raw_url,
            _SQLITE_FALLBACK_FLAG,
        )

    return normalized


def _create_engine(database_url: str):
    connect_args: Dict[str, Any] = {}
    engine_kwargs: Dict[str, Any] = {
        "future": True,
        "pool_pre_ping": True,
    }

    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        engine_kwargs["connect_args"] = connect_args
        if ":memory:" in database_url:
            engine_kwargs["poolclass"] = StaticPool
    else:
        connect_args["sslmode"] = os.getenv("CONFIG_DB_SSLMODE", "require")
        engine_kwargs["connect_args"] = connect_args
        engine_kwargs.update(
            pool_size=int(os.getenv("CONFIG_DB_POOL_SIZE", "10")),
            max_overflow=int(os.getenv("CONFIG_DB_MAX_OVERFLOW", "5")),
            pool_timeout=int(os.getenv("CONFIG_DB_POOL_TIMEOUT", "30")),
            pool_recycle=int(os.getenv("CONFIG_DB_POOL_RECYCLE", "1800")),
        )

    return create_engine(database_url, **engine_kwargs)


Base = declarative_base()


def _get_config_engine(application: FastAPI) -> Engine:
    engine = getattr(application.state, "db_engine", None)
    if engine is None:
        raise RuntimeError(
            "Config service database engine is not initialised. "
            "Ensure the FastAPI application startup has completed successfully."
        )
    return cast(Engine, engine)


def _get_session_factory(application: FastAPI) -> sessionmaker:
    session_factory = getattr(application.state, "db_sessionmaker", None)
    if session_factory is None:
        raise RuntimeError(
            "Config service session factory is unavailable. "
            "Ensure the FastAPI application startup has completed successfully."
        )
    return cast(sessionmaker, session_factory)


def _initialise_database(application: FastAPI) -> None:
    if hasattr(application.state, "db_engine") and hasattr(application.state, "db_sessionmaker"):
        return

    database_url = _require_database_url()
    engine = _create_engine(database_url)
    session_factory = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        future=True,
    )

    application.state.db_engine = engine
    application.state.db_sessionmaker = session_factory

    Base.metadata.create_all(bind=engine)


class ConfigVersion(Base):
    """ORM model representing committed configuration versions."""

    __tablename__ = "config_versions"

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False, default="global")
    key = Column(String, nullable=False)
    value_json = Column(JSON, nullable=False)
    version = Column(Integer, nullable=False)
    approvers = Column(JSON, nullable=False, default=list)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint("account_id", "key", "version", name="uq_config_version"),
    )

# ---------------------------------------------------------------------------
# Guarded key management
# ---------------------------------------------------------------------------


GUARDED_KEYS: Set[str] = {
    "risk.max_notional",
    "trading.kill_switch",
}


@dataclass
class PendingGuardedChange:
    """Represents the interim state for a guarded configuration change."""

    value: Any
    author: str
    created_at: datetime


PendingKey = Tuple[str, str]
_pending_guarded: Dict[PendingKey, PendingGuardedChange] = {}


def set_guarded_keys(keys: Iterable[str]) -> None:
    """Override the guarded keys collection (primarily for testing)."""

    GUARDED_KEYS.clear()
    GUARDED_KEYS.update(keys)


def guarded_keys() -> Set[str]:
    return set(GUARDED_KEYS)


def reset_state(application: Optional[FastAPI] = None) -> None:
    """Reset in-memory and database state (used in tests)."""

    engine = _get_config_engine(application or app)
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    _pending_guarded.clear()


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def get_session(request: Request) -> Generator[Session, None, None]:
    session_factory = _get_session_factory(request.app)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def _next_version(session: Session, *, account_id: str, key: str) -> int:
    stmt = select(func.max(ConfigVersion.version)).where(
        ConfigVersion.account_id == account_id, ConfigVersion.key == key
    )
    max_version: Optional[int] = session.execute(stmt).scalar()
    return (max_version or 0) + 1


def _latest_config_record(
    session: Session,
    *,
    account_id: str,
    key: str,
) -> Optional[ConfigVersion]:
    stmt = (
        select(ConfigVersion)
        .where(ConfigVersion.account_id == account_id, ConfigVersion.key == key)
        .order_by(ConfigVersion.version.desc())
        .limit(1)
    )
    return session.execute(stmt).scalars().first()


def _serialize_config(record: ConfigVersion) -> "ConfigEntry":
    return ConfigEntry(
        id=record.id,
        account_id=record.account_id,
        key=record.key,
        value=record.value_json,
        version=record.version,
        approvers=list(record.approvers or []),
        ts=record.ts,
    )


def _audit_snapshot(record: Optional[ConfigVersion]) -> Dict[str, Any]:
    if record is None:
        return {}

    return {
        "account_id": record.account_id,
        "key": record.key,
        "value": record.value_json,
        "version": record.version,
        "approvers": list(record.approvers or []),
        "ts": record.ts.isoformat(),
    }


# ---------------------------------------------------------------------------
# API schemas
# ---------------------------------------------------------------------------


class ConfigUpdateRequest(BaseModel):
    key: str = Field(..., description="Configuration key to update")
    value: Any = Field(..., description="JSON-serialisable value for the configuration key")
    author: str = Field(..., description="User requesting the change")


class ConfigEntry(BaseModel):
    id: int
    account_id: str
    key: str
    value: Any
    version: int
    approvers: List[str]
    ts: datetime


class ConfigUpdateResponse(BaseModel):
    status: str = Field(..., pattern="^(pending|applied)$")
    account_id: str
    key: str
    value: Any
    approvers: List[str]
    version: Optional[int]
    ts: Optional[datetime]
    required_approvals: int


# ---------------------------------------------------------------------------
# FastAPI application setup
# ---------------------------------------------------------------------------


app = FastAPI(title="Config Service")


@app.on_event("startup")
def _on_startup() -> None:
    try:
        _initialise_database(app)
    except RuntimeError as exc:  # pragma: no cover - defensive logging path
        LOGGER.error("Failed to initialise config service database: %s", exc)
        raise


def _pending_key(account_id: str, key: str) -> PendingKey:
    return account_id, key


def _commit_version(
    session: Session,
    *,
    account_id: str,
    key: str,
    value: Any,
    approvers: List[str],
) -> ConfigVersion:
    version = _next_version(session, account_id=account_id, key=key)
    record = ConfigVersion(
        account_id=account_id,
        key=key,
        value_json=value,
        version=version,
        approvers=list(approvers),
        ts=datetime.now(timezone.utc),
    )
    session.add(record)
    session.commit()
    session.refresh(record)
    return record


@app.get("/config/current", response_model=Dict[str, ConfigEntry])
def get_current_config(
    account_id: str = Query("global", description="Account identifier"),
    _admin_account: str = Depends(require_admin_account),
    session: Session = Depends(get_session),
) -> Dict[str, ConfigEntry]:
    stmt = (
        select(ConfigVersion)
        .where(ConfigVersion.account_id == account_id)
        .order_by(ConfigVersion.key.asc(), ConfigVersion.version.desc())
    )
    results = session.execute(stmt).scalars().all()
    latest: Dict[str, ConfigEntry] = {}
    for record in results:
        if record.key not in latest:
            latest[record.key] = _serialize_config(record)
    return latest


@app.post("/config/update", response_model=ConfigUpdateResponse)
def update_config(
    payload: ConfigUpdateRequest,
    request: Request,
    account_id: str = Query("global", description="Account identifier"),
    admin_account: str = Depends(require_admin_account),
    session: Session = Depends(get_session),
):
    key = payload.key
    pending_identifier = _pending_key(account_id, key)
    required_approvals = 2 if key in GUARDED_KEYS else 1

    before_record = _latest_config_record(session, account_id=account_id, key=key)
    before_snapshot = _audit_snapshot(before_record)
    entity = f"{account_id}:{key}"
    ip_hash = hash_ip(request.client.host if request.client else None)

    if key in GUARDED_KEYS:
        pending = _pending_guarded.get(pending_identifier)
        if pending is None:
            created_at = datetime.now(timezone.utc)
            _pending_guarded[pending_identifier] = PendingGuardedChange(
                value=payload.value,
                author=admin_account,
                created_at=created_at,
            )
            response = ConfigUpdateResponse(
                status="pending",
                account_id=account_id,
                key=key,
                value=payload.value,
                approvers=[admin_account],
                version=None,
                ts=None,
                required_approvals=required_approvals,
            )

            if log_audit is not None:
                try:
                    after_snapshot = {
                        "account_id": account_id,
                        "key": key,
                        "value": payload.value,
                        "status": "pending",
                        "requested_by": admin_account,
                        "requested_at": created_at.isoformat(),
                        "required_approvals": required_approvals,
                    }
                    log_audit(
                        actor=admin_account,
                        action="config.change.requested",
                        entity=entity,
                        before=before_snapshot,
                        after=after_snapshot,
                        ip_hash=ip_hash,
                    )
                except Exception:  # pragma: no cover - defensive best effort
                    LOGGER.exception(
                        "Failed to record audit log for pending config change %s", entity
                    )
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.model_dump())

        if pending.author == admin_account:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="second_author_required")
        if pending.value != payload.value:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="value_mismatch")

        record = _commit_version(
            session,
            account_id=account_id,
            key=key,
            value=payload.value,
            approvers=[pending.author, admin_account],
        )
        _pending_guarded.pop(pending_identifier, None)
        response = ConfigUpdateResponse(
            status="applied",
            account_id=record.account_id,
            key=record.key,
            value=record.value_json,
            approvers=list(record.approvers or []),
            version=record.version,
            ts=record.ts,
            required_approvals=required_approvals,
        )

        if log_audit is not None:
            try:
                log_audit(
                    actor=admin_account,
                    action="config.change.approved",
                    entity=entity,
                    before=before_snapshot,
                    after=_audit_snapshot(record),
                    ip_hash=ip_hash,
                )
            except Exception:  # pragma: no cover - defensive best effort
                LOGGER.exception("Failed to record audit log for config approval %s", entity)

        return response

    record = _commit_version(
        session,
        account_id=account_id,
        key=key,
        value=payload.value,
        approvers=[admin_account],
    )
    response = ConfigUpdateResponse(
        status="applied",
        account_id=record.account_id,
        key=record.key,
        value=record.value_json,
        approvers=list(record.approvers or []),
        version=record.version,
        ts=record.ts,
        required_approvals=required_approvals,
    )

    if log_audit is not None:
        try:
            log_audit(
                actor=admin_account,
                action="config.change.applied",
                entity=entity,
                before=before_snapshot,
                after=_audit_snapshot(record),
                ip_hash=ip_hash,
            )
        except Exception:  # pragma: no cover - defensive best effort
            LOGGER.exception("Failed to record audit log for config change %s", entity)

    return response


@app.get("/config/history", response_model=List[ConfigEntry])
def get_config_history(
    key: str = Query(..., description="Configuration key"),
    account_id: str = Query("global", description="Account identifier"),
    session: Session = Depends(get_session),
) -> List[ConfigEntry]:
    stmt = (
        select(ConfigVersion)
        .where(ConfigVersion.account_id == account_id, ConfigVersion.key == key)
        .order_by(ConfigVersion.version.asc())
    )
    records = session.execute(stmt).scalars().all()
    entries = [_serialize_config(record) for record in records]
    return entries


__all__ = [
    "app",
    "ConfigVersion",
    "ConfigUpdateRequest",
    "ConfigEntry",
    "ConfigUpdateResponse",
    "get_session",
    "reset_state",
    "set_guarded_keys",
    "guarded_keys",
]

