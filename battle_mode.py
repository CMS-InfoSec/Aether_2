"""Battle mode controller coordinating volatility driven trading restrictions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Callable, ContextManager, Dict, Iterable, List, Optional

from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

try:  # pragma: no cover - prefer the real dependency when installed
    import sqlalchemy as _sqlalchemy_module  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - exercised in dependency-light environments
    _sqlalchemy_module = None  # type: ignore[assignment]

from sqlalchemy import Column, DateTime, String, select
from sqlalchemy.orm import Session, declarative_base

from shared.account_scope import account_id_column

BattleModeBase = declarative_base()

_USING_SQLALCHEMY_STUB = bool(
    getattr(_sqlalchemy_module, "__aether_stub__", False)
)


class BattleModeLog(BattleModeBase):
    """SQLAlchemy table tracking battle mode state transitions."""

    __tablename__ = "battle_mode_log"

    account_id = account_id_column(primary_key=True)
    entered_at = Column(DateTime(timezone=True), primary_key=True)
    exited_at = Column(DateTime(timezone=True), nullable=True)
    reason = Column(String, nullable=False, default="")


@dataclass
class BattleModeStatus:
    """Serializable representation of an account's battle mode status."""

    account_id: str
    engaged: bool
    threshold: float
    reason: Optional[str] = None
    entered_at: Optional[datetime] = None
    exited_at: Optional[datetime] = None
    volatility: Optional[float] = None
    metric: Optional[str] = None
    automatic: bool = False
    last_updated: Optional[datetime] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "account_id": self.account_id,
            "engaged": self.engaged,
            "threshold": self.threshold,
            "reason": self.reason,
            "entered_at": self.entered_at.isoformat() if self.entered_at else None,
            "exited_at": self.exited_at.isoformat() if self.exited_at else None,
            "volatility": self.volatility,
            "metric": self.metric,
            "automatic": self.automatic,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
        }


@dataclass
class _BattleModeState:
    engaged: bool
    reason: str
    entered_at: datetime
    automatic: bool
    last_volatility: Optional[float] = None
    metric: Optional[str] = None
    last_updated: Optional[datetime] = None


@dataclass
class _MetricSnapshot:
    value: float
    metric: str
    timestamp: datetime


class BattleModeController:
    """Coordinates battle mode state driven by realized market volatility."""

    def __init__(
        self,
        session_factory: Callable[[], ContextManager[Session]],
        *,
        threshold: float,
    ) -> None:
        self._session_factory = session_factory
        self._threshold = max(float(threshold), 0.0)
        self._lock = Lock()
        self._history_lock = Lock()
        self._states: Dict[str, _BattleModeState] = {}
        self._metrics: Dict[str, _MetricSnapshot] = {}
        self._history: Dict[str, List[BattleModeLog]] = {}
        self._use_in_memory = _USING_SQLALCHEMY_STUB
        self._hydrate_active_states()

    @property
    def threshold(self) -> float:
        return self._threshold

    def _hydrate_active_states(self) -> None:
        if self._use_in_memory:
            records = self._list_in_memory_records(active_only=True)
        else:
            try:
                with self._session_factory() as session:
                    records = (
                        session.execute(
                            select(BattleModeLog).where(BattleModeLog.exited_at.is_(None))
                        )
                        .scalars()
                        .all()
                    )
            except Exception:
                # Database might not be initialised yet during bootstrap.
                return

        now = datetime.now(timezone.utc)
        with self._lock:
            for record in records:
                entered_at = record.entered_at
                if entered_at is None:
                    entered_at = now
                self._states[self._normalize(record.account_id)] = _BattleModeState(
                    engaged=True,
                    reason=record.reason or "volatility_exceeded",
                    entered_at=entered_at,
                    automatic=False,
                    last_updated=entered_at,
                )

    def status(self, account_id: str) -> BattleModeStatus:
        normalized = self._normalize(account_id)
        with self._lock:
            state = self._states.get(normalized)
            metric_snapshot = self._metrics.get(normalized)

        if state and state.engaged:
            volatility = state.last_volatility
            metric_name = state.metric
            updated = state.last_updated
            if metric_snapshot:
                volatility = metric_snapshot.value
                metric_name = metric_snapshot.metric
                updated = metric_snapshot.timestamp
            return BattleModeStatus(
                account_id=normalized,
                engaged=True,
                threshold=self._threshold,
                reason=state.reason,
                entered_at=state.entered_at,
                volatility=volatility,
                metric=metric_name,
                automatic=state.automatic,
                last_updated=updated,
            )

        record = self._load_last_record(normalized)
        volatility = None
        metric_name = None
        updated = None
        if metric_snapshot:
            volatility = metric_snapshot.value
            metric_name = metric_snapshot.metric
            updated = metric_snapshot.timestamp
        return BattleModeStatus(
            account_id=normalized,
            engaged=False,
            threshold=self._threshold,
            reason=record.reason if record else None,
            entered_at=record.entered_at if record else None,
            exited_at=record.exited_at if record else None,
            volatility=volatility,
            metric=metric_name,
            automatic=False,
            last_updated=updated,
        )

    def auto_update(
        self,
        account_id: str,
        volatility: Optional[float],
        *,
        metric: str,
        reason: Optional[str] = None,
    ) -> None:
        if volatility is None or metric.strip() == "":
            return

        normalized = self._normalize(account_id)
        now = datetime.now(timezone.utc)
        snapshot = _MetricSnapshot(value=float(volatility), metric=metric, timestamp=now)
        with self._lock:
            self._metrics[normalized] = snapshot
            state = self._states.get(normalized)
            if volatility > self._threshold:
                if state and state.engaged:
                    state.last_volatility = float(volatility)
                    state.metric = metric
                    state.last_updated = now
                    return
                entry_reason = reason or (
                    f"{metric}={volatility:.6f} exceeded threshold {self._threshold:.6f}"
                )
                self._enter(normalized, entry_reason, automatic=True, volatility=float(volatility), metric=metric, updated=now)
            elif state and state.engaged and volatility < self._threshold:
                exit_reason = reason or (
                    f"{metric}={volatility:.6f} fell below threshold {self._threshold:.6f}"
                )
                self._exit(normalized, exit_reason, automatic=True, updated=now)

    def manual_toggle(
        self,
        account_id: str,
        *,
        engage: Optional[bool] = None,
        reason: str,
    ) -> BattleModeStatus:
        normalized = self._normalize(account_id)
        if not reason:
            raise ValueError("A reason must be provided when toggling battle mode.")

        with self._lock:
            state = self._states.get(normalized)
            target = engage if engage is not None else not (state and state.engaged)
            if target:
                self._enter(normalized, reason, automatic=False)
            else:
                self._exit(normalized, reason, automatic=False)

        return self.status(normalized)

    def is_engaged(self, account_id: str) -> bool:
        normalized = self._normalize(account_id)
        with self._lock:
            state = self._states.get(normalized)
            return bool(state and state.engaged)

    def _enter(
        self,
        account_id: str,
        reason: str,
        *,
        automatic: bool,
        volatility: Optional[float] = None,
        metric: Optional[str] = None,
        updated: Optional[datetime] = None,
    ) -> None:
        ts = updated or datetime.now(timezone.utc)
        reason_text = reason or "volatility_exceeded"
        entered_at = ts
        if self._use_in_memory:
            entered_at = self._record_in_memory_entry(
                account_id,
                reason_text,
                timestamp=ts,
            )
        else:
            with self._session_factory() as session:
                existing = (
                    session.execute(
                        select(BattleModeLog)
                        .where(
                            BattleModeLog.account_id == account_id,
                            BattleModeLog.exited_at.is_(None),
                        )
                        .order_by(BattleModeLog.entered_at.desc())
                    )
                    .scalars()
                    .first()
                )
                if existing:
                    existing.reason = reason_text
                    entered_at = existing.entered_at or ts
                else:
                    session.add(
                        BattleModeLog(
                            account_id=account_id,
                            entered_at=ts,
                            exited_at=None,
                            reason=reason_text,
                        )
                    )
                    entered_at = ts

        self._states[account_id] = _BattleModeState(
            engaged=True,
            reason=reason_text,
            entered_at=entered_at,
            automatic=automatic,
            last_volatility=volatility,
            metric=metric,
            last_updated=ts,
        )
        if volatility is not None and metric:
            self._metrics[account_id] = _MetricSnapshot(value=float(volatility), metric=metric, timestamp=ts)

    def _exit(
        self,
        account_id: str,
        reason: str,
        *,
        automatic: bool,
        updated: Optional[datetime] = None,
    ) -> None:
        ts = updated or datetime.now(timezone.utc)
        reason_text = reason or "volatility_normalized"
        if self._use_in_memory:
            self._record_in_memory_exit(account_id, reason_text, timestamp=ts)
        else:
            with self._session_factory() as session:
                active = (
                    session.execute(
                        select(BattleModeLog)
                        .where(
                            BattleModeLog.account_id == account_id,
                            BattleModeLog.exited_at.is_(None),
                        )
                        .order_by(BattleModeLog.entered_at.desc())
                    )
                    .scalars()
                    .first()
                )
                if active:
                    active.exited_at = ts
                    if reason_text:
                        if active.reason:
                            active.reason = f"{active.reason} | exit: {reason_text}"
                        else:
                            active.reason = reason_text
                else:
                    session.add(
                        BattleModeLog(
                            account_id=account_id,
                            entered_at=ts,
                            exited_at=ts,
                            reason=reason_text,
                        )
                    )

        self._states.pop(account_id, None)
        snapshot = self._metrics.get(account_id)
        if snapshot:
            self._metrics[account_id] = _MetricSnapshot(
                value=snapshot.value,
                metric=snapshot.metric,
                timestamp=ts,
            )

    def _load_last_record(self, account_id: str) -> Optional[BattleModeLog]:
        if self._use_in_memory:
            return self._last_in_memory_record(account_id)
        with self._session_factory() as session:
            return (
                session.execute(
                    select(BattleModeLog)
                    .where(BattleModeLog.account_id == account_id)
                    .order_by(BattleModeLog.entered_at.desc())
                )
                .scalars()
                .first()
            )

    @staticmethod
    def _normalize(account_id: str) -> str:
        normalized = account_id.strip()
        if not normalized:
            raise ValueError("Account identifier must not be empty.")
        return normalized

    def _list_in_memory_records(
        self, *, account_id: Optional[str] = None, active_only: bool = False
    ) -> List[BattleModeLog]:
        with self._history_lock:
            histories: Iterable[List[BattleModeLog]]
            if account_id is not None:
                histories = (self._history.get(account_id, []),)
            else:
                histories = self._history.values()
            records: List[BattleModeLog] = [
                record
                for history in histories
                for record in history
                if not active_only or record.exited_at is None
            ]
            return list(records)

    def _record_in_memory_entry(
        self,
        account_id: str,
        reason: str,
        *,
        timestamp: datetime,
    ) -> datetime:
        with self._history_lock:
            history = self._history.setdefault(account_id, [])
            active = next((record for record in reversed(history) if record.exited_at is None), None)
            if active:
                active.reason = reason
                return active.entered_at or timestamp

            entry = BattleModeLog(
                account_id=account_id,
                entered_at=timestamp,
                exited_at=None,
                reason=reason,
            )
            history.append(entry)
            return timestamp

    def _record_in_memory_exit(
        self,
        account_id: str,
        reason: str,
        *,
        timestamp: datetime,
    ) -> None:
        with self._history_lock:
            history = self._history.setdefault(account_id, [])
            active = next((record for record in reversed(history) if record.exited_at is None), None)
            if active:
                active.exited_at = timestamp
                if reason:
                    if active.reason:
                        active.reason = f"{active.reason} | exit: {reason}"
                    else:
                        active.reason = reason
                return

            history.append(
                BattleModeLog(
                    account_id=account_id,
                    entered_at=timestamp,
                    exited_at=timestamp,
                    reason=reason,
                )
            )

    def _last_in_memory_record(self, account_id: str) -> Optional[BattleModeLog]:
        with self._history_lock:
            history = self._history.get(account_id, [])
            return history[-1] if history else None


def create_battle_mode_tables(engine) -> None:
    """Initialise battle mode persistence layer."""

    BattleModeBase.metadata.create_all(bind=engine)
