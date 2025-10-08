"""Infrastructure cost aware throttling utilities.

The module keeps a lightweight in-memory registry of throttle state per
account.  Decisions are made by comparing infrastructure spend against recent
profit and loss obtained from :mod:`cost_efficiency`.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Callable, ClassVar, Dict, Mapping, Optional, Tuple

from cost_efficiency import CostEfficiencyMetrics, get_cost_metrics

logger = logging.getLogger(__name__)

try:  # pragma: no cover - SQLAlchemy is optional in some environments
    from sqlalchemy import Boolean, Column, DateTime, Float, Integer, MetaData, String, Table, create_engine, select
    from sqlalchemy.orm import Session, sessionmaker
    from sqlalchemy.pool import StaticPool

    SQLALCHEMY_AVAILABLE = getattr(Boolean, "__module__", "").startswith("sqlalchemy")
    if not SQLALCHEMY_AVAILABLE:  # pragma: no cover - stubbed SQLAlchemy detected
        Session = sessionmaker = object  # type: ignore[assignment]
        select = create_engine = None  # type: ignore[assignment]
        StaticPool = object  # type: ignore[assignment]
except Exception:  # pragma: no cover - fall back gracefully when SQLAlchemy missing
    SQLALCHEMY_AVAILABLE = False
    Boolean = Column = DateTime = Float = Integer = MetaData = String = Table = object  # type: ignore[assignment]
    Session = sessionmaker = object  # type: ignore[assignment]
    select = create_engine = None  # type: ignore[assignment]
    StaticPool = object  # type: ignore[assignment]

__all__ = [
    "CostThrottler",
    "ThrottleHistoryEntry",
    "ThrottleRepository",
    "ThrottleStatus",
    "throttle_log",
    "get_throttle_log",
    "clear_throttle_log",
]


def _env_ratio_threshold() -> float:
    value = os.getenv("COST_THROTTLE_RATIO", "0.6")
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.6


DEFAULT_RATIO_THRESHOLD = _env_ratio_threshold()


def _ensure_sqlalchemy_available() -> None:
    if not SQLALCHEMY_AVAILABLE:
        raise RuntimeError("sqlalchemy is required for persistent cost throttling state")


@dataclass(frozen=True)
class ThrottleStatus:
    """Current throttling state for an account."""

    active: bool
    reason: Optional[str] = None
    action: Optional[str] = None
    cost_ratio: Optional[float] = None
    infra_cost: Optional[float] = None
    recent_pnl: Optional[float] = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class ThrottleHistoryEntry:
    """Immutable representation of a throttle history record."""

    account_id: str
    active: bool
    reason: Optional[str]
    action: Optional[str]
    cost_ratio: Optional[float]
    infra_cost: Optional[float]
    recent_pnl: Optional[float]
    recorded_at: datetime

    def as_dict(self) -> dict[str, object]:
        """Return a serialisable view of the history record."""

        return {
            "account_id": self.account_id,
            "active": self.active,
            "reason": self.reason,
            "action": self.action,
            "cost_ratio": self.cost_ratio,
            "infra_cost": self.infra_cost,
            "recent_pnl": self.recent_pnl,
            "recorded_at": self.recorded_at,
            # ``ts`` maintained for backwards compatibility with legacy tests/tools.
            "ts": self.recorded_at,
        }


if SQLALCHEMY_AVAILABLE:

    class ThrottleRepository:
        """Persistence layer for throttle status and audit history."""

        _STATUS_TABLE_NAME: ClassVar[str] = "cost_throttle_status"
        _HISTORY_TABLE_NAME: ClassVar[str] = "cost_throttle_history"
        _metadata: ClassVar[MetaData | None] = None
        _status_table: ClassVar[Table | None] = None
        _history_table: ClassVar[Table | None] = None

        def __init__(
            self,
            session_factory: Callable[[], Session] | sessionmaker | None = None,
        ) -> None:
            _ensure_sqlalchemy_available()

            if session_factory is None:
                session_factory = self._default_session_factory()

            if isinstance(session_factory, sessionmaker):
                factory_callable: Callable[[], Session] = session_factory  # type: ignore[assignment]
            else:
                factory_callable = session_factory  # type: ignore[assignment]

            probe_session = factory_callable()
            try:
                bind = getattr(probe_session, "get_bind", lambda: None)()
                if bind is None:
                    raise RuntimeError(
                        "session_factory must provide sessions bound to an engine",
                    )
                engine = getattr(bind, "engine", bind)
            finally:
                close = getattr(probe_session, "close", None)
                if callable(close):
                    close()

            self._session_factory = factory_callable
            self._engine = engine
            self._ensure_schema()

        @staticmethod
        def _default_session_factory() -> sessionmaker:
            database_url = os.getenv("COST_THROTTLE_DATABASE_URL")
            if not database_url:
                logger.warning(
                    "COST_THROTTLE_DATABASE_URL not configured; using transient in-memory SQLite store"
                )
                engine = create_engine(
                    "sqlite+pysqlite:///:memory:",
                    future=True,
                    connect_args={"check_same_thread": False},
                    poolclass=StaticPool,
                )
            else:
                engine = create_engine(database_url, future=True)
            return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

        @classmethod
        def _define_tables(cls) -> tuple[Table, Table]:
            if cls._metadata is None:
                cls._metadata = MetaData()
                cls._status_table = Table(
                    cls._STATUS_TABLE_NAME,
                    cls._metadata,
                    Column("account_id", String, primary_key=True),
                    Column("active", Boolean, nullable=False),
                    Column("reason", String, nullable=True),
                    Column("action", String, nullable=True),
                    Column("cost_ratio", Float, nullable=True),
                    Column("infra_cost", Float, nullable=True),
                    Column("recent_pnl", Float, nullable=True),
                    Column("updated_at", DateTime(timezone=True), nullable=False),
                )
                cls._history_table = Table(
                    cls._HISTORY_TABLE_NAME,
                    cls._metadata,
                    Column("id", Integer, primary_key=True, autoincrement=True),
                    Column("account_id", String, nullable=False, index=True),
                    Column("active", Boolean, nullable=False),
                    Column("reason", String, nullable=True),
                    Column("action", String, nullable=True),
                    Column("cost_ratio", Float, nullable=True),
                    Column("infra_cost", Float, nullable=True),
                    Column("recent_pnl", Float, nullable=True),
                    Column("recorded_at", DateTime(timezone=True), nullable=False),
                )
            assert cls._status_table is not None
            assert cls._history_table is not None
            return cls._status_table, cls._history_table

        def _ensure_schema(self) -> None:
            status_table, history_table = self._define_tables()
            assert self._engine is not None
            with self._engine.begin() as connection:
                metadata = status_table.metadata
                assert metadata is not None
                metadata.create_all(connection, tables=[status_table, history_table])

        @property
        def session_factory(self) -> Callable[[], Session]:
            return self._session_factory

        @staticmethod
        def _normalize_datetime(value: datetime) -> datetime:
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)

        def _row_to_status(self, row: Mapping[str, object]) -> ThrottleStatus:
            return ThrottleStatus(
                active=bool(row["active"]),
                reason=row.get("reason"),
                action=row.get("action"),
                cost_ratio=row.get("cost_ratio"),
                infra_cost=row.get("infra_cost"),
                recent_pnl=row.get("recent_pnl"),
                updated_at=self._normalize_datetime(row["updated_at"]),
            )

        def get_status(self, account_id: str) -> Optional[ThrottleStatus]:
            status_table, _ = self._define_tables()
            with self._session_factory() as session:
                row = session.execute(
                    select(status_table).where(status_table.c.account_id == account_id).limit(1)
                ).mappings().first()
            if row is None:
                return None
            return self._row_to_status(row)

        def persist_evaluation(self, account_id: str, status: ThrottleStatus) -> Optional[ThrottleStatus]:
            status_table, history_table = self._define_tables()
            history_reason = status.reason or ("throttle_cleared" if not status.active else None)
            recorded_at = self._normalize_datetime(status.updated_at)
            status_payload = {
                "account_id": account_id,
                "active": status.active,
                "reason": status.reason,
                "action": status.action,
                "cost_ratio": status.cost_ratio,
                "infra_cost": status.infra_cost,
                "recent_pnl": status.recent_pnl,
                "updated_at": recorded_at,
            }
            history_payload = {
                "account_id": account_id,
                "active": status.active,
                "reason": history_reason,
                "action": status.action,
                "cost_ratio": status.cost_ratio,
                "infra_cost": status.infra_cost,
                "recent_pnl": status.recent_pnl,
                "recorded_at": recorded_at,
            }

            with self._session_factory() as session:
                with session.begin():
                    previous_row = session.execute(
                        select(status_table).where(status_table.c.account_id == account_id).limit(1)
                    ).mappings().first()
                    if previous_row is None:
                        session.execute(status_table.insert().values(**status_payload))
                    else:
                        session.execute(
                            status_table.update()
                            .where(status_table.c.account_id == account_id)
                            .values(**status_payload)
                        )
                    session.execute(history_table.insert().values(**history_payload))

                if previous_row is None:
                    return None
                return self._row_to_status(previous_row)

        def history(self, account_id: Optional[str] = None) -> list[ThrottleHistoryEntry]:
            _, history_table = self._define_tables()
            stmt = select(history_table).order_by(history_table.c.recorded_at.asc(), history_table.c.id.asc())
            if account_id is not None:
                stmt = stmt.where(history_table.c.account_id == account_id)
            with self._session_factory() as session:
                rows = session.execute(stmt).mappings().all()
            return [
                ThrottleHistoryEntry(
                    account_id=row["account_id"],
                    active=bool(row["active"]),
                    reason=row.get("reason"),
                    action=row.get("action"),
                    cost_ratio=row.get("cost_ratio"),
                    infra_cost=row.get("infra_cost"),
                    recent_pnl=row.get("recent_pnl"),
                    recorded_at=self._normalize_datetime(row["recorded_at"]),
                )
                for row in rows
            ]

        def record_manual_entry(self, account_id: str, *, reason: str, recorded_at: datetime) -> None:
            _, history_table = self._define_tables()
            normalized = self._normalize_datetime(recorded_at)
            with self._session_factory() as session:
                with session.begin():
                    session.execute(
                        history_table.insert().values(
                            account_id=account_id,
                            active=reason != "throttle_cleared",
                            reason=reason,
                            action=None,
                            cost_ratio=None,
                            infra_cost=None,
                            recent_pnl=None,
                            recorded_at=normalized,
                        )
                    )

        def clear(self) -> None:
            """Remove all status and history records. Intended for tests only."""

            status_table, history_table = self._define_tables()
            with self._session_factory() as session:
                with session.begin():
                    session.execute(history_table.delete())
                    session.execute(status_table.delete())

        def clear_account(self, account_id: str) -> None:
            status_table, history_table = self._define_tables()
            with self._session_factory() as session:
                with session.begin():
                    session.execute(history_table.delete().where(history_table.c.account_id == account_id))
                    session.execute(status_table.delete().where(status_table.c.account_id == account_id))

        def clear_history(self) -> None:
            _, history_table = self._define_tables()
            with self._session_factory() as session:
                with session.begin():
                    session.execute(history_table.delete())
else:

    class ThrottleRepository:
        """In-memory fallback repository when SQLAlchemy is unavailable."""

        def __init__(self, session_factory: Callable[[], object] | None = None) -> None:
            del session_factory
            self._status: Dict[str, ThrottleStatus] = {}
            self._history: list[ThrottleHistoryEntry] = []

        @staticmethod
        def _normalize_datetime(value: datetime) -> datetime:
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)

        def get_status(self, account_id: str) -> Optional[ThrottleStatus]:
            return self._status.get(account_id)

        def persist_evaluation(self, account_id: str, status: ThrottleStatus) -> Optional[ThrottleStatus]:
            previous = self._status.get(account_id)
            normalized = self._normalize_datetime(status.updated_at)
            current = replace(status, updated_at=normalized)
            self._status[account_id] = current

            history_reason = status.reason or ("throttle_cleared" if not status.active else None)
            self._history.append(
                ThrottleHistoryEntry(
                    account_id=account_id,
                    active=status.active,
                    reason=history_reason,
                    action=status.action,
                    cost_ratio=status.cost_ratio,
                    infra_cost=status.infra_cost,
                    recent_pnl=status.recent_pnl,
                    recorded_at=normalized,
                )
            )
            return previous

        def history(self, account_id: Optional[str] = None) -> list[ThrottleHistoryEntry]:
            if account_id is None:
                return list(self._history)
            return [entry for entry in self._history if entry.account_id == account_id]

        def record_manual_entry(self, account_id: str, *, reason: str, recorded_at: datetime) -> None:
            normalized = self._normalize_datetime(recorded_at)
            self._history.append(
                ThrottleHistoryEntry(
                    account_id=account_id,
                    active=reason != "throttle_cleared",
                    reason=reason,
                    action=None,
                    cost_ratio=None,
                    infra_cost=None,
                    recent_pnl=None,
                    recorded_at=normalized,
                )
            )

        def clear(self) -> None:
            self._status.clear()
            self._history.clear()

        def clear_account(self, account_id: str) -> None:
            self._status.pop(account_id, None)
            self._history = [entry for entry in self._history if entry.account_id != account_id]

        def clear_history(self) -> None:
            self._history.clear()


_DEFAULT_REPOSITORY: ThrottleRepository | None = None


def _get_repository() -> ThrottleRepository:
    global _DEFAULT_REPOSITORY
    if _DEFAULT_REPOSITORY is None:
        _DEFAULT_REPOSITORY = ThrottleRepository()
    return _DEFAULT_REPOSITORY


def _set_repository(repository: ThrottleRepository) -> None:
    global _DEFAULT_REPOSITORY
    _DEFAULT_REPOSITORY = repository


def throttle_log(account_id: str, reason: str, ts: datetime) -> None:
    """Persist throttle actions for auditability."""

    repository = _get_repository()
    repository.record_manual_entry(account_id, reason=reason, recorded_at=ts)
    logger.info(
        "Manual throttle log entry",
        extra={"event": "cost_throttle.manual_log", "account_id": account_id, "reason": reason, "ts": ts},
    )


def get_throttle_log() -> list[dict[str, object]]:
    repository = _get_repository()
    return [entry.as_dict() for entry in repository.history()]


def clear_throttle_log() -> None:
    repository = _get_repository()
    repository.clear_history()


class CostThrottler:
    """Simple controller that toggles throttled mode based on cost ratios."""

    def __init__(
        self,
        ratio_threshold: Optional[float] = None,
        *,
        repository: Optional[ThrottleRepository] = None,
    ) -> None:
        if ratio_threshold is None:
            ratio_threshold = DEFAULT_RATIO_THRESHOLD
        self._ratio_threshold = max(float(ratio_threshold), 0.0)
        if repository is None:
            repository = _get_repository()
        else:
            _set_repository(repository)
        self._repository = repository

    @staticmethod
    def _extract_metrics(metrics: Optional[CostEfficiencyMetrics]) -> Optional[Tuple[float, float]]:
        if metrics is None:
            return None

        cost = getattr(metrics, "infra_cost", None)
        pnl = getattr(metrics, "recent_pnl", None)
        if cost is None and isinstance(metrics, dict):
            cost = metrics.get("infra_cost")
        if pnl is None and isinstance(metrics, dict):
            pnl = metrics.get("recent_pnl")

        if cost is None or pnl is None:
            return None
        try:
            cost_val = float(cost)
        except (TypeError, ValueError):
            return None
        try:
            pnl_val = float(pnl)
        except (TypeError, ValueError):
            return None
        return cost_val, pnl_val

    def _determine_action(self, ratio: float) -> str:
        if ratio >= self._ratio_threshold * 1.5:
            return "reduce_inference_refresh"
        return "throttle_retraining"

    @staticmethod
    def _status_as_dict(status: ThrottleStatus) -> dict[str, object]:
        return {
            "active": status.active,
            "reason": status.reason,
            "action": status.action,
            "cost_ratio": status.cost_ratio,
            "infra_cost": status.infra_cost,
            "recent_pnl": status.recent_pnl,
            "updated_at": status.updated_at,
        }

    def evaluate(self, account_id: str) -> ThrottleStatus:
        snapshot = get_cost_metrics(account_id)
        if snapshot.metrics is None or snapshot.is_stale():
            return self.get_status(account_id)

        metrics = self._extract_metrics(snapshot.metrics)
        if metrics is None:
            return self.get_status(account_id)

        infra_cost, recent_pnl = metrics
        pnl_reference = abs(recent_pnl)
        if pnl_reference <= 0:
            cost_ratio = float("inf") if infra_cost > 0 else 0.0
        else:
            cost_ratio = infra_cost / pnl_reference

        now = snapshot.retrieved_at
        if cost_ratio >= self._ratio_threshold:
            action = self._determine_action(cost_ratio)
            reason = (
                "Infrastructure cost %.2f exceeds %.0f%% of recent PnL"
                % (infra_cost, self._ratio_threshold * 100)
            )
            status = ThrottleStatus(
                active=True,
                reason=reason,
                action=action,
                cost_ratio=cost_ratio,
                infra_cost=infra_cost,
                recent_pnl=recent_pnl,
                updated_at=now,
            )
        else:
            status = ThrottleStatus(
                active=False,
                reason=None,
                action=None,
                cost_ratio=cost_ratio,
                infra_cost=infra_cost,
                recent_pnl=recent_pnl,
                updated_at=now,
            )

        previous = self._repository.persist_evaluation(account_id, status)
        log_payload = {
            "event": "cost_throttle.evaluate",
            "account_id": account_id,
            "status": self._status_as_dict(status),
        }
        if previous is not None:
            log_payload["previous_status"] = self._status_as_dict(previous)
        logger.info("Cost throttle evaluation", extra=log_payload)
        return status

    def get_status(self, account_id: str) -> ThrottleStatus:
        status = self._repository.get_status(account_id)
        if status is None:
            return ThrottleStatus(active=False)
        return replace(status)

    def clear(self, account_id: Optional[str] = None) -> None:
        repository = self._repository
        if account_id is None:
            repository.clear()
        else:
            repository.clear_account(account_id)

