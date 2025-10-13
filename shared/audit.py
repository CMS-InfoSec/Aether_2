"""Audit logging facilities for sensitive actions."""
from __future__ import annotations

import datetime as dt
import logging
import time
import uuid
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Protocol

from .correlation import get_correlation_id


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuditLogEntry:
    """Immutable representation of an audit log record."""

    id: uuid.UUID
    action: str
    actor_id: str
    account_id: str
    before: MappingProxyType
    after: MappingProxyType
    correlation_id: Optional[str]
    created_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))


class AuditLogBackend(Protocol):
    """Protocol describing the persistence surface required by the store."""

    def record_audit_log(self, record: Mapping[str, Any]) -> None:
        ...

    def audit_logs(self) -> Iterable[Mapping[str, Any]]:
        ...


SleepFn = Callable[[float], None]


def _coerce_datetime(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)

    if isinstance(value, str):
        try:
            parsed = dt.datetime.fromisoformat(value)
        except ValueError:
            return dt.datetime.now(dt.timezone.utc)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)

    return dt.datetime.now(dt.timezone.utc)


class AuditLogStore:
    """TimescaleDB-backed audit log store enforcing immutability semantics."""

    _DEFAULT_ACCOUNT_ID = "aether-audit"

    def __init__(
        self,
        *,
        account_id: str | None = None,
        backend: AuditLogBackend | None = None,
        max_retries: int = 3,
        backoff_seconds: float = 0.05,
        sleep_fn: SleepFn | None = None,
    ) -> None:
        self._account_id = account_id or self._DEFAULT_ACCOUNT_ID
        self._backend_impl: AuditLogBackend | None = backend
        self._max_retries = max(0, int(max_retries))
        self._backoff_seconds = max(0.0, float(backoff_seconds))
        self._sleep: SleepFn = sleep_fn or time.sleep

    @property
    def account_id(self) -> str:
        """Return the account identifier associated with the store."""

        return self._account_id

    def _get_backend(self) -> AuditLogBackend:
        if self._backend_impl is None:
            from services.common.adapters import TimescaleAdapter  # local import to avoid cycles

            self._backend_impl = TimescaleAdapter(account_id=self._account_id)
        return self._backend_impl

    def append(self, entry: AuditLogEntry) -> None:
        payload = {
            "before": dict(entry.before),
            "after": dict(entry.after),
            "correlation_id": entry.correlation_id,
            "entry_id": str(entry.id),
            "account_id": entry.account_id,
        }
        record = {
            "actor": entry.actor_id,
            "action": entry.action,
            "target": entry.correlation_id,
            "created_at": entry.created_at,
            "payload": payload,
            "account_id": entry.account_id,
        }

        attempt = 0
        while True:
            try:
                self._get_backend().record_audit_log(record)
                return
            except Exception as exc:  # pragma: no cover - exercised via unit tests with stubs
                if attempt >= self._max_retries:
                    logger.error("Failed to persist audit log entry", exc_info=exc)
                    raise
                delay = self._backoff_seconds * (2 ** attempt)
                logger.warning(
                    "Transient error persisting audit log entry (attempt %s/%s): %s", 
                    attempt + 1,
                    self._max_retries + 1,
                    exc,
                )
                if delay > 0:
                    self._sleep(delay)
                attempt += 1

    def all(self) -> Iterable[AuditLogEntry]:
        entries: List[AuditLogEntry] = []
        for raw in self._get_backend().audit_logs():
            payload = raw.get("payload", {})
            before = MappingProxyType(dict(payload.get("before") or {}))
            after = MappingProxyType(dict(payload.get("after") or {}))
            correlation_id = payload.get("correlation_id") or raw.get("target")
            created_at = _coerce_datetime(raw.get("created_at"))
            raw_id: Any = payload.get("entry_id") or raw.get("id") or raw.get("log_id")
            raw_account: Any = raw.get("account_id") or payload.get("account_id")
            try:
                entry_id = raw_id if isinstance(raw_id, uuid.UUID) else uuid.UUID(str(raw_id))
            except (TypeError, ValueError):
                entry_id = uuid.uuid4()

            account_id = (
                str(raw_account)
                if isinstance(raw_account, (str, uuid.UUID))
                else self._account_id
            )
            entries.append(
                AuditLogEntry(
                    id=entry_id,
                    action=str(raw.get("action", "")),
                    actor_id=str(raw.get("actor", "")),
                    account_id=account_id,
                    before=before,
                    after=after,
                    correlation_id=correlation_id,
                    created_at=created_at,
                )
            )

        entries.sort(key=lambda record: record.created_at)
        return tuple(entries)


class TimescaleAuditLogger:
    """Writer abstraction for the TimescaleDB ``audit_logs`` hypertable."""

    def __init__(self, store: AuditLogStore) -> None:
        self._store = store

    def record(
        self,
        *,
        action: str,
        actor_id: str,
        before: Optional[Dict[str, Any]],
        after: Optional[Dict[str, Any]],
        correlation_id: Optional[str] = None,
        account_id: Optional[str] = None,
    ) -> AuditLogEntry:
        entry = AuditLogEntry(
            id=uuid.uuid4(),
            action=action,
            actor_id=actor_id,
            account_id=account_id or self._store.account_id,
            before=MappingProxyType(dict(before or {})),
            after=MappingProxyType(dict(after or {})),
            correlation_id=correlation_id,
        )
        self._store.append(entry)
        return entry


class SensitiveActionRecorder:
    """Utility for recording before/after snapshots of sensitive operations."""

    def __init__(self, logger: TimescaleAuditLogger) -> None:
        self._logger = logger

    def record(
        self,
        *,
        action: str,
        actor_id: str,
        before: Optional[Dict[str, Any]],
        after: Optional[Dict[str, Any]],
    ) -> AuditLogEntry:
        correlation_id = get_correlation_id()
        return self._logger.record(
            action=action,
            actor_id=actor_id,
            before=before,
            after=after,
            correlation_id=correlation_id,
        )


__all__ = [
    "AuditLogEntry",
    "AuditLogStore",
    "TimescaleAuditLogger",
    "SensitiveActionRecorder",
]
