"""Account management FastAPI service.

This module exposes endpoints for creating and managing trading accounts.
It provides orchestration around account lifecycle events including Kraken
API credential rotation, simulation toggles, default configuration
bootstrap, and governance logging.  The implementation favours explicit
SQLAlchemy usage so the service can run in isolation with either a
PostgreSQL or SQLite backend which keeps the unit tests lightweight.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Path as FastAPIPath, status

if FastAPIPath is Path:  # pragma: no cover - fallback for stub environments
    from services.common.fastapi_stub import Path as FastAPIPath  # type: ignore[assignment]

from metrics import setup_metrics
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    JSON,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

import yaml

from services.account_crypto import (
    decrypt_value as _decrypt,
    encrypt_value as _encrypt,
    fernet_key as _fernet_key,
)
from services.k8s_sync_service import sync_account_secret
from services.kraken_test_service import test_kraken_connection
from shared.accounts_config import resolve_accounts_database_url

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------


def _database_url() -> str:
    return resolve_accounts_database_url()


def _engine_options(url: str) -> dict[str, Any]:
    options: dict[str, Any] = {"future": True, "pool_pre_ping": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


DATABASE_URL = _database_url()
ENGINE: Engine = create_engine(DATABASE_URL, **_engine_options(DATABASE_URL))
SessionLocal = sessionmaker(
    bind=ENGINE,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)


Base = declarative_base(metadata=MetaData())


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Account(Base):
    __tablename__ = "accounts"

    account_id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    name = Column(String, nullable=False)
    owner_user_id = Column(PGUUID(as_uuid=True), nullable=False)
    base_currency = Column(String, nullable=False, default="USD")
    sim_mode = Column(Boolean, nullable=False, default=False)
    hedge_auto = Column(Boolean, nullable=False, default=True)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at = Column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )


class KrakenKey(Base):
    __tablename__ = "kraken_keys"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    account_id = Column(
        PGUUID(as_uuid=True),
        ForeignKey("accounts.account_id", ondelete="CASCADE"),
        nullable=False,
    )
    encrypted_api_key = Column(Text, nullable=False)
    encrypted_api_secret = Column(Text, nullable=False)
    last_rotated_at = Column(DateTime(timezone=True), nullable=False, default=_utcnow)


class AccountConfig(Base):
    __tablename__ = "account_configs"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    account_id = Column(
        PGUUID(as_uuid=True),
        ForeignKey("accounts.account_id", ondelete="CASCADE"),
        nullable=False,
    )
    config_type = Column(String, nullable=False)
    payload = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at = Column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    account_id = Column(PGUUID(as_uuid=True), nullable=False)
    user_id = Column(PGUUID(as_uuid=True), nullable=False)
    action = Column(String, nullable=False)
    details = Column(JSON, nullable=False, default=dict)
    timestamp = Column(DateTime(timezone=True), nullable=False, default=_utcnow)


# Ensure tables exist for environments without migrations (tests).
Base.metadata.create_all(ENGINE)


# ---------------------------------------------------------------------------
# Security helpers
# ---------------------------------------------------------------------------


class Role(str):
    ADMIN = "admin"
    DIRECTOR = "director"


@dataclass
class CurrentUser:
    user_id: UUID
    role: str
    account_ids: set[UUID]

    def _normalized_role(self) -> str:
        return self.role.strip().lower()

    def has_management_rights(self) -> bool:
        return self._normalized_role() in {Role.ADMIN, Role.DIRECTOR}

    def can_access(self, account_id: UUID) -> bool:
        if self._normalized_role() == Role.ADMIN:
            return True
        return account_id in self.account_ids


def _parse_uuid_list(raw: str | None) -> set[UUID]:
    if not raw:
        return set()
    uuids: set[UUID] = set()
    for part in raw.split(","):
        cleaned = part.strip()
        if not cleaned:
            continue
        uuids.add(UUID(cleaned))
    return uuids


from fastapi import Header


def get_current_user(
    user_id: str = Header(..., alias="X-User-Id"),
    role: str = Header(Role.DIRECTOR, alias="X-User-Role"),
    account_ids: str = Header("", alias="X-Account-Ids"),
) -> CurrentUser:
    """Dependency used primarily during tests to inject user context.

    In production the expectation is that the ASGI stack or API gateway will
    populate ``TEST_*`` environment variables prior to calling into the
    application.  The default behaviour keeps the dependency simple for unit
    tests where the TestClient can override ``app.dependency_overrides`` with
    a callable returning :class:`CurrentUser`.
    """

    try:
        user_uuid = UUID(user_id)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid user") from exc

    role_value = (role or Role.DIRECTOR).strip().lower()
    allowed = _parse_uuid_list(account_ids)
    return CurrentUser(user_id=user_uuid, role=role_value, account_ids=allowed)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class AccountBase(BaseModel):
    name: str = Field(..., max_length=255)
    owner_user_id: UUID
    base_currency: str = Field("USD", max_length=12)
    sim_mode: bool = False
    hedge_auto: bool = True


class AccountCreateRequest(AccountBase):
    api_key: Optional[str] = Field(None, alias="initial_api_key")
    api_secret: Optional[str] = Field(None, alias="initial_api_secret")

    @field_validator("api_key", "api_secret", mode="before")
    @classmethod
    def _blank_to_none(cls, value: Optional[str]) -> Optional[str]:
        if isinstance(value, str) and not value.strip():
            return None
        return value


class AccountCreateResponse(BaseModel):
    account_id: UUID
    name: str
    owner_user_id: UUID
    base_currency: str
    sim_mode: bool
    hedge_auto: bool
    active: bool
    kraken_status: Optional[dict[str, Any]] = None


class AccountSummary(BaseModel):
    account_id: UUID
    name: str
    owner_user_id: UUID
    sim_mode: bool
    hedge_auto: bool
    active: bool


class AccountUpdateRequest(BaseModel):
    account_id: UUID
    name: Optional[str] = None
    owner_user_id: Optional[UUID] = None
    hedge_auto: Optional[bool] = None
    sim_mode: Optional[bool] = None


class AccountDeleteRequest(BaseModel):
    account_id: UUID
    hard_delete: bool = False


class ApiKeyUploadRequest(BaseModel):
    account_id: UUID
    api_key: str
    api_secret: str


class SimToggleRequest(BaseModel):
    account_id: UUID
    enabled: bool


class DefaultsRequest(BaseModel):
    account_id: UUID


class AuditEventResponse(BaseModel):
    id: UUID
    action: str
    details: dict[str, Any]
    timestamp: datetime


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def get_session() -> Iterable[Session]:  # pragma: no cover - FastAPI dependency
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _account_to_summary(account: Account) -> AccountSummary:
    return AccountSummary(
        account_id=account.account_id,
        name=account.name,
        owner_user_id=account.owner_user_id,
        sim_mode=account.sim_mode,
        hedge_auto=account.hedge_auto,
        active=account.active,
    )


def _load_defaults() -> dict[str, Any]:
    path = Path(__file__).resolve().parent.parent / "config" / "defaults.yaml"
    if not path.exists():
        return {
            "trading": {"max_positions": 10},
            "risk": {"max_drawdown_pct": 0.2},
            "training": {"retrain_interval_days": 7},
            "hedge": {"target_pct": 0.25},
        }
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("defaults.yaml must contain a mapping of config types")
    return data


def apply_default_configs(session: Session, account_id: UUID) -> dict[str, Any]:
    defaults = _load_defaults()
    for config_type, payload in defaults.items():
        stmt = select(AccountConfig).where(
            AccountConfig.account_id == account_id,
            AccountConfig.config_type == config_type,
        )
        record = session.execute(stmt).scalar_one_or_none()
        timestamp = _utcnow()
        if record is None:
            record = AccountConfig(
                account_id=account_id,
                config_type=config_type,
                payload=payload,
                created_at=timestamp,
                updated_at=timestamp,
            )
            session.add(record)
        else:
            record.payload = payload
            record.updated_at = timestamp
    session.flush()
    return defaults


def log_audit_event(
    session: Session,
    *,
    account_id: UUID,
    user_id: UUID,
    action: str,
    details: dict[str, Any] | None = None,
) -> AuditLog:
    record = AuditLog(
        account_id=account_id,
        user_id=user_id,
        action=action,
        details=details or {},
        timestamp=_utcnow(),
    )
    session.add(record)
    session.flush()
    LOGGER.info(
        "audit_event",  # pragma: no cover - logging only
        extra={
            "audit": {
                "account_id": str(account_id),
                "user_id": str(user_id),
                "action": action,
                "details": details or {},
            }
        },
    )
    return record


def _ensure_access(user: CurrentUser, account_id: UUID) -> None:
    if not user.can_access(account_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is not authorized for this account",
        )


def _store_api_keys(
    session: Session,
    *,
    account_id: UUID,
    user: CurrentUser,
    api_key: str,
    api_secret: str,
) -> KrakenKey:
    encrypted_key = _encrypt(api_key)
    encrypted_secret = _encrypt(api_secret)

    existing_stmt = select(KrakenKey).where(KrakenKey.account_id == account_id)
    existing = session.execute(existing_stmt).scalar_one_or_none()
    timestamp = _utcnow()
    if existing is None:
        record = KrakenKey(
            account_id=account_id,
            encrypted_api_key=encrypted_key,
            encrypted_api_secret=encrypted_secret,
            last_rotated_at=timestamp,
        )
        session.add(record)
    else:
        existing.encrypted_api_key = encrypted_key
        existing.encrypted_api_secret = encrypted_secret
        existing.last_rotated_at = timestamp
        record = existing
    session.flush()
    log_audit_event(
        session,
        account_id=account_id,
        user_id=user.user_id,
        action="kraken_keys_rotated",
        details={"rotated_at": timestamp.isoformat()},
    )
    sync_account_secret(str(account_id), api_key, api_secret)
    return record


def _kraken_status_from_keys(record: KrakenKey | None) -> Optional[dict[str, Any]]:
    if record is None:
        return None
    decrypted_key = _decrypt(record.encrypted_api_key)
    decrypted_secret = _decrypt(record.encrypted_api_secret)
    return test_kraken_connection(decrypted_key, decrypted_secret)


# ---------------------------------------------------------------------------
# FastAPI application and endpoints
# ---------------------------------------------------------------------------


app = FastAPI(title="Account Management Service")
setup_metrics(app, service_name="account-service")


@app.post("/accounts/create", response_model=AccountCreateResponse)
def create_account(
    payload: AccountCreateRequest,
    session: Session = Depends(get_session),
    user: CurrentUser = Depends(get_current_user),
) -> AccountCreateResponse:
    if not user.has_management_rights():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admin or director roles can create accounts",
        )

    account = Account(
        account_id=uuid4(),
        name=payload.name,
        owner_user_id=payload.owner_user_id,
        base_currency=payload.base_currency,
        sim_mode=payload.sim_mode,
        hedge_auto=payload.hedge_auto,
        created_at=_utcnow(),
        updated_at=_utcnow(),
    )
    session.add(account)
    session.flush()

    defaults = apply_default_configs(session, account.account_id)
    log_audit_event(
        session,
        account_id=account.account_id,
        user_id=user.user_id,
        action="account_created",
        details={"name": account.name, "defaults": list(defaults)},
    )

    kraken_status: Optional[dict[str, Any]] = None
    if payload.api_key and payload.api_secret:
        record = _store_api_keys(
            session,
            account_id=account.account_id,
            user=user,
            api_key=payload.api_key,
            api_secret=payload.api_secret,
        )
        kraken_status = _kraken_status_from_keys(record)

    session.commit()

    return AccountCreateResponse(
        account_id=account.account_id,
        name=account.name,
        owner_user_id=account.owner_user_id,
        base_currency=account.base_currency,
        sim_mode=account.sim_mode,
        hedge_auto=account.hedge_auto,
        active=account.active,
        kraken_status=kraken_status,
    )


@app.get("/accounts/list", response_model=list[AccountSummary])
def list_accounts(
    session: Session = Depends(get_session),
    user: CurrentUser = Depends(get_current_user),
) -> list[AccountSummary]:
    stmt = select(Account).where(Account.active.is_(True))
    records = session.execute(stmt).scalars().all()
    summaries: list[AccountSummary] = []
    for record in records:
        if user.role != Role.ADMIN and record.account_id not in user.account_ids:
            continue
        summaries.append(_account_to_summary(record))
    return summaries


@app.get("/accounts/{account_id}")
def get_account(
    account_id: UUID = FastAPIPath(...),
    session: Session = Depends(get_session),
    user: CurrentUser = Depends(get_current_user),
) -> dict[str, Any]:
    _ensure_access(user, account_id)
    account = session.get(Account, account_id)
    if account is None or not account.active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")

    configs_stmt = select(AccountConfig).where(AccountConfig.account_id == account_id)
    config_rows = session.execute(configs_stmt).scalars().all()
    configs = {row.config_type: row.payload for row in config_rows}

    return {
        "account": _account_to_summary(account).dict(),
        "base_currency": account.base_currency,
        "configs": configs,
    }


@app.post("/accounts/update", response_model=AccountSummary)
def update_account(
    payload: AccountUpdateRequest,
    session: Session = Depends(get_session),
    user: CurrentUser = Depends(get_current_user),
) -> AccountSummary:
    _ensure_access(user, payload.account_id)
    account = session.get(Account, payload.account_id)
    if account is None or not account.active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")

    if payload.name is not None:
        account.name = payload.name
    if payload.owner_user_id is not None:
        account.owner_user_id = payload.owner_user_id
    if payload.hedge_auto is not None:
        account.hedge_auto = payload.hedge_auto
    if payload.sim_mode is not None:
        account.sim_mode = payload.sim_mode
    account.updated_at = _utcnow()

    log_audit_event(
        session,
        account_id=account.account_id,
        user_id=user.user_id,
        action="account_updated",
        details=payload.dict(exclude_unset=True),
    )
    session.commit()
    return _account_to_summary(account)


@app.post("/accounts/delete")
def delete_account(
    payload: AccountDeleteRequest,
    session: Session = Depends(get_session),
    user: CurrentUser = Depends(get_current_user),
) -> dict[str, Any]:
    if not user.has_management_rights():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admin or director roles can delete accounts",
        )
    account = session.get(Account, payload.account_id)
    if account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")

    if payload.hard_delete:
        session.delete(account)
    else:
        account.active = False
        account.updated_at = _utcnow()

    log_audit_event(
        session,
        account_id=payload.account_id,
        user_id=user.user_id,
        action="account_deleted" if payload.hard_delete else "account_deactivated",
        details={"hard_delete": payload.hard_delete},
    )
    session.commit()
    return {"status": "ok"}


@app.post("/accounts/api/upload")
def upload_api_keys(
    payload: ApiKeyUploadRequest,
    session: Session = Depends(get_session),
    user: CurrentUser = Depends(get_current_user),
) -> dict[str, Any]:
    _ensure_access(user, payload.account_id)
    account = session.get(Account, payload.account_id)
    if account is None or not account.active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")

    record = _store_api_keys(
        session,
        account_id=payload.account_id,
        user=user,
        api_key=payload.api_key,
        api_secret=payload.api_secret,
    )
    status_payload = _kraken_status_from_keys(record)
    session.commit()
    return {"status": "ok", "kraken": status_payload}


@app.get("/accounts/api/test/{account_id}")
def api_test(
    account_id: UUID = FastAPIPath(...),
    session: Session = Depends(get_session),
    user: CurrentUser = Depends(get_current_user),
) -> dict[str, Any]:
    _ensure_access(user, account_id)
    stmt = select(KrakenKey).where(KrakenKey.account_id == account_id)
    record = session.execute(stmt).scalar_one_or_none()
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No API keys configured")
    status_payload = _kraken_status_from_keys(record)
    return status_payload or {"status": "unknown"}


@app.post("/accounts/sim/toggle", response_model=AccountSummary)
def toggle_simulation(
    payload: SimToggleRequest,
    session: Session = Depends(get_session),
    user: CurrentUser = Depends(get_current_user),
) -> AccountSummary:
    _ensure_access(user, payload.account_id)
    account = session.get(Account, payload.account_id)
    if account is None or not account.active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")
    account.sim_mode = payload.enabled
    account.updated_at = _utcnow()
    log_audit_event(
        session,
        account_id=payload.account_id,
        user_id=user.user_id,
        action="simulation_toggled",
        details={"enabled": payload.enabled},
    )
    session.commit()
    return _account_to_summary(account)


@app.post("/accounts/config/defaults")
def apply_defaults_endpoint(
    payload: DefaultsRequest,
    session: Session = Depends(get_session),
    user: CurrentUser = Depends(get_current_user),
) -> dict[str, Any]:
    _ensure_access(user, payload.account_id)
    defaults = apply_default_configs(session, payload.account_id)
    log_audit_event(
        session,
        account_id=payload.account_id,
        user_id=user.user_id,
        action="defaults_applied",
        details={"keys": list(defaults)},
    )
    session.commit()
    return {"status": "ok", "applied": defaults}


@app.get("/accounts/audit/{account_id}", response_model=list[AuditEventResponse])
def audit_log(
    account_id: UUID = FastAPIPath(...),
    session: Session = Depends(get_session),
    user: CurrentUser = Depends(get_current_user),
) -> list[AuditEventResponse]:
    _ensure_access(user, account_id)
    stmt = (
        select(AuditLog)
        .where(AuditLog.account_id == account_id)
        .order_by(AuditLog.timestamp.desc())
    )
    records = session.execute(stmt).scalars().all()
    return [
        AuditEventResponse(
            id=record.id,
            action=record.action,
            details=dict(record.details or {}),
            timestamp=record.timestamp,
        )
        for record in records
    ]


__all__ = [
    "app",
    "Account",
    "KrakenKey",
    "AccountConfig",
    "AuditLog",
    "apply_default_configs",
    "log_audit_event",
]

