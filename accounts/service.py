"""Accounts service coordinating admin profiles and approval workflows."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

_SQLALCHEMY_AVAILABLE = True

try:  # pragma: no cover - optional dependency in production
    from sqlalchemy import (
        Boolean,
        DateTime,
        ForeignKey,
        JSON,
        String,
        UniqueConstraint,
        create_engine,
        select,
    )
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import (
        DeclarativeBase,
        Mapped,
        Session,
        mapped_column,
        relationship,
        sessionmaker,
    )
    from sqlalchemy.pool import StaticPool
    from sqlalchemy.sql import func
except Exception:  # pragma: no cover - exercised in minimal environments
    Boolean = DateTime = ForeignKey = JSON = String = UniqueConstraint = object  # type: ignore[assignment]
    JSONB = object  # type: ignore[assignment]
    Engine = object  # type: ignore[assignment]
    IntegrityError = Exception  # type: ignore[assignment]
    DeclarativeBase = object  # type: ignore[assignment]
    Mapped = object  # type: ignore[assignment]
    Session = object  # type: ignore[assignment]
    mapped_column = relationship = sessionmaker = func = select = create_engine = None  # type: ignore[assignment]
    StaticPool = object  # type: ignore[assignment]
    _SQLALCHEMY_AVAILABLE = False

from shared.accounts_config import resolve_accounts_database_url
from shared.audit import SensitiveActionRecorder


logger = logging.getLogger(__name__)

_TEST_DSN_ENV = "AETHER_ACCOUNTS_TEST_DSN"
_IN_MEMORY_SQLITE_URL = "sqlite+pysqlite:///:memory:"


if _SQLALCHEMY_AVAILABLE:
    class Base(DeclarativeBase):
        """Base declarative class for the accounts service ORM models."""

else:
    class _Metadata:
        def create_all(self, *, bind: object | None = None) -> None:
            return None

    class Base:
        """Lightweight stand-in exposing SQLAlchemy metadata helpers."""

        metadata = _Metadata()

    SessionFactory = Callable[[], object]

    def _sqlite_path_from_url(url: str) -> str:
        parsed = urlparse(url)
        base, _, _driver = parsed.scheme.partition("+")
        if base and base != "sqlite":
            logger.warning(
                "SQLAlchemy unavailable; using in-memory SQLite fallback for accounts DSN %s",
                url,
            )
            return ":memory:"

        netloc_path = f"{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path
        if netloc_path.startswith("//"):
            netloc_path = netloc_path[1:]

        if netloc_path in {"", "/"}:
            return ":memory:"

        candidate = Path(netloc_path)
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate

        if str(candidate) != ":memory:":
            candidate.parent.mkdir(parents=True, exist_ok=True)

        return str(candidate)

    def _row_datetime(value: str | None) -> datetime:
        if not value:
            return datetime.fromtimestamp(0, tz=timezone.utc)
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return datetime.fromtimestamp(0, tz=timezone.utc)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _decode_payload(raw: str | None) -> dict:
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(data, dict):
            return data
        return {"value": data}

    class _SQLiteAccountsRepository:
        """SQLite-backed fallback used when SQLAlchemy is unavailable."""

        def __init__(self, url: str) -> None:
            self._path = _sqlite_path_from_url(url)
            self._lock = threading.RLock()
            self._memory_conn: sqlite3.Connection | None = None
            if self._path == ":memory:":
                self._memory_conn = sqlite3.connect(
                    ":memory:",
                    detect_types=sqlite3.PARSE_DECLTYPES,
                    check_same_thread=False,
                )
                self._memory_conn.row_factory = sqlite3.Row
            self._ensure_schema()

        def _connect(self) -> sqlite3.Connection:
            if self._memory_conn is not None:
                return self._memory_conn
            conn = sqlite3.connect(
                self._path,
                detect_types=sqlite3.PARSE_DECLTYPES,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            return conn

        def _close(self, conn: sqlite3.Connection) -> None:
            if conn.in_transaction:
                conn.rollback()
            if conn is self._memory_conn:
                return
            conn.close()

        def _ensure_schema(self) -> None:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS admin_profiles (
                        admin_id TEXT PRIMARY KEY,
                        email TEXT NOT NULL,
                        display_name TEXT NOT NULL,
                        kraken_credentials_linked INTEGER NOT NULL DEFAULT 0,
                        last_updated TEXT NOT NULL
                    )
                    """.strip()
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS risk_configuration_changes (
                        request_id TEXT PRIMARY KEY,
                        requested_by TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        executed INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL
                    )
                    """.strip()
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS risk_configuration_change_approvals (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        request_id TEXT NOT NULL,
                        admin_id TEXT NOT NULL,
                        approved_at TEXT NOT NULL,
                        UNIQUE(request_id, admin_id)
                    )
                    """.strip()
                )
                conn.commit()
            finally:
                self._close(conn)

        def _profile_from_row(self, row: sqlite3.Row) -> AdminProfile:
            return AdminProfile(
                admin_id=row["admin_id"],
                email=row["email"],
                display_name=row["display_name"],
                kraken_credentials_linked=bool(row["kraken_credentials_linked"]),
                last_updated=_row_datetime(row["last_updated"]),
            )

        def _change_from_row(self, row: sqlite3.Row, approvals: Sequence[str]) -> RiskConfigurationChange:
            return RiskConfigurationChange(
                request_id=row["request_id"],
                requested_by=row["requested_by"],
                payload=_decode_payload(row["payload"]),
                approvals=list(approvals),
                executed=bool(row["executed"]),
                created_at=_row_datetime(row["created_at"]),
            )

        def upsert_profile(self, profile: AdminProfile) -> Tuple[Optional[AdminProfile], AdminProfile]:
            timestamp = datetime.now(timezone.utc).isoformat()
            with self._lock:
                conn = self._connect()
                try:
                    cursor = conn.execute(
                        "SELECT * FROM admin_profiles WHERE admin_id = ?",
                        (profile.admin_id,),
                    )
                    row = cursor.fetchone()
                    before = self._profile_from_row(row) if row else None
                    if row:
                        conn.execute(
                            """
                            UPDATE admin_profiles
                            SET email = ?, display_name = ?, kraken_credentials_linked = ?, last_updated = ?
                            WHERE admin_id = ?
                            """.strip(),
                            (
                                profile.email,
                                profile.display_name,
                                1 if profile.kraken_credentials_linked else 0,
                                timestamp,
                                profile.admin_id,
                            ),
                        )
                    else:
                        conn.execute(
                            """
                            INSERT INTO admin_profiles (admin_id, email, display_name, kraken_credentials_linked, last_updated)
                            VALUES (?, ?, ?, ?, ?)
                            """.strip(),
                            (
                                profile.admin_id,
                                profile.email,
                                profile.display_name,
                                1 if profile.kraken_credentials_linked else 0,
                                timestamp,
                            ),
                        )
                    conn.commit()
                    refreshed = conn.execute(
                        "SELECT * FROM admin_profiles WHERE admin_id = ?",
                        (profile.admin_id,),
                    ).fetchone()
                    assert refreshed is not None
                    return before, self._profile_from_row(refreshed)
                finally:
                    self._close(conn)

        def get_profile(self, admin_id: str) -> Optional[AdminProfile]:
            with self._lock:
                conn = self._connect()
                try:
                    row = conn.execute(
                        "SELECT * FROM admin_profiles WHERE admin_id = ?",
                        (admin_id,),
                    ).fetchone()
                    if row is None:
                        return None
                    return self._profile_from_row(row)
                finally:
                    self._close(conn)

        def set_kraken_credentials_status(self, admin_id: str, linked: bool) -> Tuple[AdminProfile, AdminProfile]:
            timestamp = datetime.now(timezone.utc).isoformat()
            with self._lock:
                conn = self._connect()
                try:
                    row = conn.execute(
                        "SELECT * FROM admin_profiles WHERE admin_id = ?",
                        (admin_id,),
                    ).fetchone()
                    if row is None:
                        raise KeyError("profile_missing")
                    before = self._profile_from_row(row)
                    conn.execute(
                        """
                        UPDATE admin_profiles
                        SET kraken_credentials_linked = ?, last_updated = ?
                        WHERE admin_id = ?
                        """.strip(),
                        (1 if linked else 0, timestamp, admin_id),
                    )
                    conn.commit()
                    refreshed = conn.execute(
                        "SELECT * FROM admin_profiles WHERE admin_id = ?",
                        (admin_id,),
                    ).fetchone()
                    assert refreshed is not None
                    return before, self._profile_from_row(refreshed)
                finally:
                    self._close(conn)

        def request_risk_configuration_change(self, admin_id: str, payload: dict) -> RiskConfigurationChange:
            request_id = str(uuid.uuid4())
            created_at = datetime.now(timezone.utc).isoformat()
            payload_json = json.dumps(payload)
            with self._lock:
                conn = self._connect()
                try:
                    conn.execute(
                        """
                        INSERT INTO risk_configuration_changes (request_id, requested_by, payload, executed, created_at)
                        VALUES (?, ?, ?, 0, ?)
                        """.strip(),
                        (request_id, admin_id, payload_json, created_at),
                    )
                    conn.commit()
                    row = conn.execute(
                        "SELECT * FROM risk_configuration_changes WHERE request_id = ?",
                        (request_id,),
                    ).fetchone()
                    assert row is not None
                    return self._change_from_row(row, approvals=[])
                finally:
                    self._close(conn)

        def approve_risk_change(
            self, admin_id: str, request_id: str
        ) -> Tuple[RiskConfigurationChange, List[str], bool]:
            now = datetime.now(timezone.utc).isoformat()
            with self._lock:
                conn = self._connect()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    change_row = conn.execute(
                        "SELECT * FROM risk_configuration_changes WHERE request_id = ?",
                        (request_id,),
                    ).fetchone()
                    if change_row is None:
                        raise KeyError("request_missing")
                    if change_row["executed"]:
                        raise PermissionError("already_executed")

                    approvals_before = [
                        r["admin_id"]
                        for r in conn.execute(
                            """
                            SELECT admin_id FROM risk_configuration_change_approvals
                            WHERE request_id = ?
                            ORDER BY approved_at
                            """.strip(),
                            (request_id,),
                        ).fetchall()
                    ]

                    if admin_id in approvals_before:
                        raise PermissionError("duplicate_approval")

                    executed_now = not change_row["executed"] and len(approvals_before) + 1 >= 2

                    try:
                        conn.execute(
                            """
                            INSERT INTO risk_configuration_change_approvals (request_id, admin_id, approved_at)
                            VALUES (?, ?, ?)
                            """.strip(),
                            (request_id, admin_id, now),
                        )
                    except sqlite3.IntegrityError as exc:
                        raise PermissionError("duplicate_approval") from exc

                    if executed_now:
                        conn.execute(
                            "UPDATE risk_configuration_changes SET executed = 1 WHERE request_id = ?",
                            (request_id,),
                        )

                    conn.commit()

                    approvals_after = [
                        r["admin_id"]
                        for r in conn.execute(
                            """
                            SELECT admin_id FROM risk_configuration_change_approvals
                            WHERE request_id = ?
                            ORDER BY approved_at
                            """.strip(),
                            (request_id,),
                        ).fetchall()
                    ]
                    refreshed = conn.execute(
                        "SELECT * FROM risk_configuration_changes WHERE request_id = ?",
                        (request_id,),
                    ).fetchone()
                    assert refreshed is not None
                    return self._change_from_row(refreshed, approvals_after), approvals_before, executed_now
                except PermissionError:
                    conn.rollback()
                    raise
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    self._close(conn)


def _database_url() -> str:
    """Resolve the backing database URL, tolerating test environments."""

    try:
        return resolve_accounts_database_url()
    except RuntimeError as exc:
        fallback = os.environ.get(_TEST_DSN_ENV)
        if fallback:
            logger.warning(
                "Accounts database DSN missing; using %s for tests instead", _TEST_DSN_ENV
            )
            return fallback
        if "pytest" in sys.modules:
            logger.warning(
                "Accounts database DSN missing; using in-memory SQLite for tests: %s",
                exc,
            )
            return _IN_MEMORY_SQLITE_URL
        raise


def _engine_options(url: str) -> dict[str, object]:
    options: dict[str, object] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


_DB_URL = _database_url()

if _SQLALCHEMY_AVAILABLE:
    ENGINE: Engine = create_engine(_DB_URL, **_engine_options(_DB_URL))
    SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)
else:
    ENGINE = None  # type: ignore[assignment]
    SessionLocal = lambda: None  # type: ignore[assignment]


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


if _SQLALCHEMY_AVAILABLE:
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


if _SQLALCHEMY_AVAILABLE:
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


    class _SQLAlchemyAccountsRepository:
        """Persistence abstraction when SQLAlchemy is available."""

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


else:
    class _SQLAlchemyAccountsRepository:  # pragma: no cover - unused placeholder
        def __init__(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("SQLAlchemy repository not available without SQLAlchemy")


class AccountsRepository:
    """Persistence abstraction for admin profiles and risk configuration changes."""

    def __init__(self, session_factory: SessionFactory | None = None) -> None:
        if _SQLALCHEMY_AVAILABLE:
            if session_factory is None:
                session_factory = SessionLocal
            self._backend = _SQLAlchemyAccountsRepository(session_factory)  # type: ignore[arg-type]
        else:
            self._backend = _SQLiteAccountsRepository(_DB_URL)

    def upsert_profile(self, profile: AdminProfile) -> Tuple[Optional[AdminProfile], AdminProfile]:
        return self._backend.upsert_profile(profile)

    def get_profile(self, admin_id: str) -> Optional[AdminProfile]:
        return self._backend.get_profile(admin_id)

    def set_kraken_credentials_status(self, admin_id: str, linked: bool) -> Tuple[AdminProfile, AdminProfile]:
        return self._backend.set_kraken_credentials_status(admin_id, linked)

    def request_risk_configuration_change(self, admin_id: str, payload: dict) -> RiskConfigurationChange:
        return self._backend.request_risk_configuration_change(admin_id, payload)

    def approve_risk_change(
        self, admin_id: str, request_id: str
    ) -> Tuple[RiskConfigurationChange, List[str], bool]:
        return self._backend.approve_risk_change(admin_id, request_id)


class AccountsService:
    """Handles admin profile state, credential status, and risk workflows."""

    def __init__(
        self,
        recorder: SensitiveActionRecorder,
        repository: Optional[AccountsRepository] = None,
    ) -> None:
        self._recorder = recorder
        self._repository = repository or AccountsRepository()

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
