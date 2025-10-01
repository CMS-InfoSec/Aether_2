"""FastAPI service for configuration management with optional dual sign-off."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Iterable, List, Optional, Set, Tuple

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import JSON, Column, DateTime, Integer, String, UniqueConstraint, create_engine, func, select
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool


# ---------------------------------------------------------------------------
# Database configuration
# ---------------------------------------------------------------------------


DEFAULT_DATABASE_URL = "sqlite+pysqlite:////tmp/config.db"


def _create_engine(database_url: str):
    connect_args: Dict[str, Any] = {}
    engine_kwargs: Dict[str, Any] = {"future": True}
    if database_url.startswith("sqlite"):  # pragma: no cover - defensive branch
        connect_args["check_same_thread"] = False
        engine_kwargs["connect_args"] = connect_args
        if ":memory:" in database_url:
            engine_kwargs["poolclass"] = StaticPool
    return create_engine(database_url, **engine_kwargs)


DATABASE_URL = os.getenv("CONFIG_DATABASE_URL", DEFAULT_DATABASE_URL)
engine = _create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)

Base = declarative_base()


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


Base.metadata.create_all(bind=engine)


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


def reset_state() -> None:
    """Reset in-memory and database state (used in tests)."""

    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    _pending_guarded.clear()


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def get_session() -> Generator[Session, None, None]:
    session = SessionLocal()
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
    account_id: str = Query("global", description="Account identifier"),
    session: Session = Depends(get_session),
):
    key = payload.key
    pending_identifier = _pending_key(account_id, key)
    required_approvals = 2 if key in GUARDED_KEYS else 1

    if key in GUARDED_KEYS:
        pending = _pending_guarded.get(pending_identifier)
        if pending is None:
            _pending_guarded[pending_identifier] = PendingGuardedChange(
                value=payload.value,
                author=payload.author,
                created_at=datetime.now(timezone.utc),
            )
            response = ConfigUpdateResponse(
                status="pending",
                account_id=account_id,
                key=key,
                value=payload.value,
                approvers=[payload.author],
                version=None,
                ts=None,
                required_approvals=required_approvals,
            )
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response.model_dump())

        if pending.author == payload.author:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="second_author_required")
        if pending.value != payload.value:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="value_mismatch")

        record = _commit_version(
            session,
            account_id=account_id,
            key=key,
            value=payload.value,
            approvers=[pending.author, payload.author],
        )
        _pending_guarded.pop(pending_identifier, None)
        return ConfigUpdateResponse(
            status="applied",
            account_id=record.account_id,
            key=record.key,
            value=record.value_json,
            approvers=list(record.approvers or []),
            version=record.version,
            ts=record.ts,
            required_approvals=required_approvals,
        )

    record = _commit_version(
        session,
        account_id=account_id,
        key=key,
        value=payload.value,
        approvers=[payload.author],
    )
    return ConfigUpdateResponse(
        status="applied",
        account_id=record.account_id,
        key=record.key,
        value=record.value_json,
        approvers=list(record.approvers or []),
        version=record.version,
        ts=record.ts,
        required_approvals=required_approvals,
    )


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

