"""Audit logging facilities for sensitive actions."""
from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Dict, Iterable, List, Optional

from .correlation import get_correlation_id


@dataclass(frozen=True)
class AuditLogEntry:
    """Immutable representation of an audit log record."""

    id: uuid.UUID
    action: str
    actor_id: str
    before: MappingProxyType
    after: MappingProxyType
    correlation_id: Optional[str]
    created_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))


class AuditLogStore:
    """TimescaleDB-backed audit log emulation enforcing immutability."""

    def __init__(self) -> None:
        self._entries: List[AuditLogEntry] = []

    def append(self, entry: AuditLogEntry) -> None:
        self._entries.append(entry)

    def all(self) -> Iterable[AuditLogEntry]:
        return tuple(self._entries)


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
    ) -> AuditLogEntry:
        entry = AuditLogEntry(
            id=uuid.uuid4(),
            action=action,
            actor_id=actor_id,
            before=MappingProxyType(before or {}),
            after=MappingProxyType(after or {}),
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
