"""Accounts service coordinating admin profiles and approval workflows."""
from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, List, Optional, Sequence, Tuple

from sqlalchemy import Boolean, DateTime, ForeignKey, JSON, String, UniqueConstraint, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql import func

from shared.accounts_config import resolve_accounts_database_url
from shared.audit import SensitiveActionRecorder


class Base(DeclarativeBase):
    """Base declarative class for the accounts service ORM models."""


def _database_url() -> str:
    return resolve_accounts_database_url()


def _engine_options(url: str) -> dict[str, object]:
    options: dict[str, object] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


_DB_URL = _database_url()
ENGINE: Engine = create_engine(_DB_URL, **_engine_options(_DB_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)


@dataclass
class AdminProfile:
    admin_id: str
    email: str
    display_name: str
    kraken_credentials_linked: bool = False
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class RiskConfigurationChange:
    request_id: str
    requested_by: str
    payload: dict
    approvals: List[str] = field(default_factory=list)
    executed: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def ready(self) -> bool:
        return len(self.approvals) >= 2 and not self.executed


class AdminProfileRecord(Base):
    """SQLAlchemy model backing persisted admin profiles."""

    __tablename__ = "admin_profiles"

    admin_id: Mapped[str] = mapped_column(String(length=64), primary_key=True)
    email: Mapped[str] = mapped_column(String(length=255), nullable=False)
    display_name: Mapped[str] = mapped_column(String(length=255), nullable=False)
    kraken_credentials_linked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), server_default=func.now()
    )


class RiskConfigurationChangeRecord(Base):
    """SQLAlchemy model for persisted risk configuration change requests."""

    __tablename__ = "risk_configuration_changes"

    request_id: Mapped[str] = mapped_column(String(length=64), primary_key=True)
    requested_by: Mapped[str] = mapped_column(String(length=64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON().with_variant(JSONB(none_as_null=True), "postgresql"), nullable=False)
    executed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), server_default=func.now()
    )

    approvals: Mapped[List["RiskConfigurationApprovalRecord"]] = relationship(
        back_populates="change",
        cascade="all, delete-orphan",
        order_by="RiskConfigurationApprovalRecord.approved_at",
    )


class RiskConfigurationApprovalRecord(Base):
    """SQLAlchemy model tracking individual approvals for risk changes."""

    __tablename__ = "risk_configuration_change_approvals"
    __table_args__ = (UniqueConstraint("request_id", "admin_id", name="uq_risk_change_approval"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    request_id: Mapped[str] = mapped_column(
        String(length=64), ForeignKey("risk_configuration_changes.request_id", ondelete="CASCADE"), nullable=False
    )
    admin_id: Mapped[str] = mapped_column(String(length=64), nullable=False)
    approved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), server_default=func.now()
    )

    change: Mapped[RiskConfigurationChangeRecord] = relationship(back_populates="approvals")


SessionFactory = Callable[[], Session]


def _profile_from_record(record: AdminProfileRecord) -> AdminProfile:
    return AdminProfile(
        admin_id=record.admin_id,
        email=record.email,
        display_name=record.display_name,
        kraken_credentials_linked=record.kraken_credentials_linked,
        last_updated=record.last_updated,
    )


def _change_from_record(record: RiskConfigurationChangeRecord, approvals: Sequence[str]) -> RiskConfigurationChange:
    return RiskConfigurationChange(
        request_id=record.request_id,
        requested_by=record.requested_by,
        payload=dict(record.payload or {}),
        approvals=list(approvals),
        executed=record.executed,
        created_at=record.created_at,
    )


class AccountsRepository:
    """Persistence abstraction for admin profiles and risk configuration changes."""

    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    def upsert_profile(self, profile: AdminProfile) -> Tuple[Optional[AdminProfile], AdminProfile]:
        session = self._session_factory()
        try:
            record = session.get(AdminProfileRecord, profile.admin_id)
            before = _profile_from_record(record) if record is not None else None
            timestamp = datetime.now(timezone.utc)
            if record is None:
                record = AdminProfileRecord(
                    admin_id=profile.admin_id,
                    email=profile.email,
                    display_name=profile.display_name,
                    kraken_credentials_linked=profile.kraken_credentials_linked,
                    last_updated=timestamp,
                )
                session.add(record)
            else:
                record.email = profile.email
                record.display_name = profile.display_name
                record.kraken_credentials_linked = profile.kraken_credentials_linked
                record.last_updated = timestamp
            session.commit()
            session.refresh(record)
            return before, _profile_from_record(record)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_profile(self, admin_id: str) -> Optional[AdminProfile]:
        session = self._session_factory()
        try:
            record = session.get(AdminProfileRecord, admin_id)
            if record is None:
                return None
            return _profile_from_record(record)
        finally:
            session.close()

    def set_kraken_credentials_status(self, admin_id: str, linked: bool) -> Tuple[AdminProfile, AdminProfile]:
        session = self._session_factory()
        try:
            record = session.get(AdminProfileRecord, admin_id)
            if record is None:
                raise KeyError("profile_missing")
            before = _profile_from_record(record)
            record.kraken_credentials_linked = linked
            record.last_updated = datetime.now(timezone.utc)
            session.commit()
            session.refresh(record)
            return before, _profile_from_record(record)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def request_risk_configuration_change(self, admin_id: str, payload: dict) -> RiskConfigurationChange:
        session = self._session_factory()
        try:
            request_id = str(uuid.uuid4())
            record = RiskConfigurationChangeRecord(
                request_id=request_id,
                requested_by=admin_id,
                payload=dict(payload),
                executed=False,
            )
            session.add(record)
            session.commit()
            session.refresh(record)
            return _change_from_record(record, approvals=[])
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def approve_risk_change(
        self, admin_id: str, request_id: str
    ) -> Tuple[RiskConfigurationChange, List[str], bool]:
        session = self._session_factory()
        try:
            stmt = select(RiskConfigurationChangeRecord).where(
                RiskConfigurationChangeRecord.request_id == request_id
            )
            if session.bind and session.bind.dialect.name not in {"sqlite"}:
                stmt = stmt.with_for_update()
            record = session.execute(stmt).scalar_one_or_none()
            if record is None:
                raise KeyError("request_missing")
            if record.executed:
                raise PermissionError("already_executed")

            approvals_before = (
                session.execute(
                    select(RiskConfigurationApprovalRecord.admin_id)
                    .where(RiskConfigurationApprovalRecord.request_id == request_id)
                    .order_by(RiskConfigurationApprovalRecord.approved_at)
                )
                .scalars()
                .all()
            )

            if admin_id in approvals_before:
                raise PermissionError("duplicate_approval")

            executed_now = not record.executed and len(approvals_before) + 1 >= 2

            approval = RiskConfigurationApprovalRecord(
                request_id=request_id,
                admin_id=admin_id,
            )
            session.add(approval)
            try:
                session.flush()
            except IntegrityError as exc:
                raise PermissionError("duplicate_approval") from exc

            if executed_now:
                record.executed = True

            session.commit()

            approvals_after = (
                session.execute(
                    select(RiskConfigurationApprovalRecord.admin_id)
                    .where(RiskConfigurationApprovalRecord.request_id == request_id)
                    .order_by(RiskConfigurationApprovalRecord.approved_at)
                )
                .scalars()
                .all()
            )
            refreshed = session.get(RiskConfigurationChangeRecord, request_id)
            assert refreshed is not None  # defensive, record must still exist
            return _change_from_record(refreshed, approvals_after), approvals_before, executed_now
        except PermissionError:
            session.rollback()
            raise
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


class AccountsService:
    """Handles admin profile state, credential status, and risk workflows."""

    def __init__(
        self,
        recorder: SensitiveActionRecorder,
        repository: Optional[AccountsRepository] = None,
    ) -> None:
        self._recorder = recorder
        self._repository = repository or AccountsRepository(SessionLocal)

    def upsert_profile(self, profile: AdminProfile) -> AdminProfile:
        before, updated = self._repository.upsert_profile(profile)
        self._recorder.record(
            action="profile_update",
            actor_id=profile.admin_id,
            before=_profile_snapshot(before),
            after=_profile_snapshot(updated),
        )
        return updated

    def get_profile(self, admin_id: str) -> Optional[AdminProfile]:
        return self._repository.get_profile(admin_id)

    def set_kraken_credentials_status(self, admin_id: str, linked: bool) -> AdminProfile:
        before, updated = self._repository.set_kraken_credentials_status(admin_id, linked)
        self._recorder.record(
            action="kraken_credentials_update",
            actor_id=admin_id,
            before=_profile_snapshot(before),
            after=_profile_snapshot(updated),
        )
        return updated

    def request_risk_configuration_change(self, admin_id: str, payload: dict) -> RiskConfigurationChange:
        change = self._repository.request_risk_configuration_change(admin_id, payload)
        self._recorder.record(
            action="risk_change_requested",
            actor_id=admin_id,
            before=None,
            after={"request_id": change.request_id, "payload": payload},
        )
        return change

    def approve_risk_change(self, admin_id: str, request_id: str) -> RiskConfigurationChange:
        change, approvals_before, executed_now = self._repository.approve_risk_change(admin_id, request_id)
        if executed_now:
            self._finalize_risk_change(change, approvals_before)
        return change

    def _finalize_risk_change(self, change: RiskConfigurationChange, approvals_before: Sequence[str]) -> None:
        self._recorder.record(
            action="risk_change_executed",
            actor_id=change.requested_by,
            before={"payload": change.payload, "approvals": list(approvals_before)},
            after={"payload": change.payload, "approvals": change.approvals},
        )

    def rotate_secret(self, admin_id: str, secret_name: str) -> None:
        self._recorder.record(
            action="secret_rotation",
            actor_id=admin_id,
            before={"secret": secret_name, "status": "active"},
            after={"secret": secret_name, "status": "rotated"},
        )

    def trigger_kill_switch(self, admin_id: str, reason: str) -> None:
        self._recorder.record(
            action="kill_switch",
            actor_id=admin_id,
            before={"reason": reason, "active": False},
            after={"reason": reason, "active": True},
        )


def _profile_snapshot(profile: Optional[AdminProfile]) -> Optional[dict]:
    if not profile:
        return None
    return {
        "admin_id": profile.admin_id,
        "email": profile.email,
        "display_name": profile.display_name,
        "kraken_credentials_linked": profile.kraken_credentials_linked,
        "last_updated": profile.last_updated.isoformat(),
    }


__all__ = [
    "AdminProfile",
    "RiskConfigurationChange",
    "AccountsService",
    "AccountsRepository",
    "Base",
    "ENGINE",
    "SessionLocal",
]
