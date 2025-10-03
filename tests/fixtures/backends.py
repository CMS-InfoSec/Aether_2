"""Light-weight fakes for infrastructure dependencies used in tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, List, Optional

import pytest


@dataclass(slots=True)
class FakeTimescaleTable:
    """In-memory representation of a TimescaleDB table."""

    name: str
    rows: List[Dict[str, Any]] = field(default_factory=list)

    def insert(self, row: Dict[str, Any]) -> None:
        self.rows.append(row)


class FakeTimescaleDB:
    """Very small shim that mimics the subset of SQLAlchemy we rely on in tests."""

    def __init__(self) -> None:
        self.tables: Dict[str, FakeTimescaleTable] = {}

    def table(self, name: str) -> FakeTimescaleTable:
        return self.tables.setdefault(name, FakeTimescaleTable(name=name))

    async def begin(self) -> AsyncIterator["FakeTimescaleDB"]:
        yield self

    async def execute_batch(self, table: str, batch: Iterable[Dict[str, Any]]) -> None:
        destination = self.table(table)
        for row in batch:
            destination.insert(dict(row))


class FakeRedis:
    """In-memory Redis replacement with dict semantics."""

    def __init__(self) -> None:
        self._values: Dict[str, Any] = {}

    async def get(self, key: str) -> Any:
        return self._values.get(key)

    async def set(self, key: str, value: Any, *, expire: Optional[int] = None) -> None:
        del expire  # Expirations are ignored for the fake.
        self._values[key] = value

    async def incr(self, key: str, amount: int = 1) -> int:
        current = int(self._values.get(key, 0)) + amount
        self._values[key] = current
        return current


class MemoryRedis:
    """Thread-safe in-memory Redis replacement for synchronous code paths."""

    def __init__(self) -> None:
        self._values: Dict[str, str] = {}
        self._lock = Lock()

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            return self._values.get(key)

    def set(self, key: str, value: str) -> None:
        with self._lock:
            self._values[key] = value

    def delete(self, key: str) -> None:
        with self._lock:
            self._values.pop(key, None)


class FakeKafkaProducer:
    """Captures messages that would normally be published to Kafka."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.messages: List[tuple[str, Dict[str, Any]]] = []
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            self.started = True

    async def stop(self) -> None:
        async with self._lock:
            self.stopped = True

    async def send_and_wait(self, topic: str, payload: Dict[str, Any]) -> None:
        async with self._lock:
            self.messages.append((topic, dict(payload)))


@dataclass
class FakeAuditEvent:
    action: str
    actor_id: str
    before: Dict[str, Any]
    after: Dict[str, Any]
    recorded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class FakeSensitiveActionRecorder:
    """Records sensitive audit operations for later verification."""

    def __init__(self) -> None:
        self.events: List[FakeAuditEvent] = []

    def record(
        self,
        *,
        action: str,
        actor_id: str,
        before: Optional[Dict[str, Any]] = None,
        after: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.events.append(
            FakeAuditEvent(
                action=action,
                actor_id=actor_id,
                before=before or {},
                after=after or {},
            )
        )


@pytest.fixture
def fake_timescale() -> FakeTimescaleDB:
    return FakeTimescaleDB()


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def fake_kafka_producer() -> FakeKafkaProducer:
    return FakeKafkaProducer()


@pytest.fixture
def fake_auditor() -> FakeSensitiveActionRecorder:
    return FakeSensitiveActionRecorder()
