"""Strategy signal bus coordinating Kafka publication and registry updates.

This module exposes a lightweight publish/subscribe helper that strategies can
use to exchange engineered features.  Signals are published to a shared Kafka
topic while their schemas are persisted to Postgres so other strategies can
discover them.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, Iterable, List, Mapping, MutableSet, Optional

try:  # pragma: no cover - aiokafka is optional in test environments
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
except Exception:  # pragma: no cover - keep graceful degradation
    AIOKafkaConsumer = None  # type: ignore
    AIOKafkaProducer = None  # type: ignore

from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

from sqlalchemy import Column, DateTime, JSON, String
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker


LOGGER = logging.getLogger(__name__)

SignalBase = declarative_base()


def _jsonify(value: Any) -> Any:
    """Best-effort conversion of arbitrary payloads into JSON-serialisable data."""

    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        if isinstance(value, Mapping):
            return {str(key): _jsonify(val) for key, val in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_jsonify(item) for item in value]
        if hasattr(value, "model_dump"):
            return _jsonify(value.model_dump())  # type: ignore[no-untyped-call]
        if hasattr(value, "dict"):
            return _jsonify(value.dict())  # type: ignore[attr-defined]
        if hasattr(value, "__dict__"):
            return _jsonify(vars(value))
        if hasattr(value, "tolist"):
            return _jsonify(value.tolist())  # type: ignore[no-untyped-call]
        return str(value)


class StrategySignalRecord(SignalBase):
    """SQLAlchemy model representing a published strategy signal."""

    __tablename__ = "strategy_signals"

    name = Column(String, primary_key=True)
    publisher = Column(String, nullable=False)
    schema = Column(JSON, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)


@dataclass(frozen=True, slots=True)
class StrategySignalMetadata:
    """Metadata describing a shared signal available on the bus."""

    name: str
    publisher: str
    schema: Any
    ts: datetime

    def as_payload(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "publisher": self.publisher,
            "schema": self.schema,
            "ts": self.ts,
        }


class StrategySignalRegistry:
    """Persistence helper backed by Postgres for strategy signals."""

    def __init__(self, session_factory: sessionmaker) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session_scope(self) -> Iterable[Session]:
        session: Session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:  # pragma: no cover - defensive rollback
            session.rollback()
            raise
        finally:
            session.close()

    def upsert(self, metadata: StrategySignalMetadata) -> None:
        with self._session_scope() as session:
            record = session.get(StrategySignalRecord, metadata.name)
            if record is None:
                record = StrategySignalRecord(
                    name=metadata.name,
                    publisher=metadata.publisher,
                    schema=metadata.schema,
                    ts=metadata.ts,
                )
                session.add(record)
            else:
                record.publisher = metadata.publisher
                record.schema = metadata.schema
                record.ts = metadata.ts

    def list(self) -> List[StrategySignalMetadata]:
        with self._session_scope() as session:
            rows = session.query(StrategySignalRecord).order_by(StrategySignalRecord.ts.desc()).all()

        return [
            StrategySignalMetadata(
                name=row.name,
                publisher=row.publisher,
                schema=row.schema,
                ts=row.ts,
            )
            for row in rows
        ]


class StrategySignalBus:
    """Kafka-backed signal bus with Postgres discovery registry."""

    def __init__(
        self,
        session_factory: sessionmaker,
        *,
        kafka_bootstrap_servers: str,
        topic: str = "strategy.signals",
        client_id: str = "strategy-bus",
    ) -> None:
        self._topic = topic
        self._client_id = client_id
        self._kafka_bootstrap_servers = kafka_bootstrap_servers
        self._registry = StrategySignalRegistry(session_factory)
        self._producer: Optional[AIOKafkaProducer] = None
        self._local_subscribers: MutableSet[asyncio.Queue[Dict[str, Any]]] = set()
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        if AIOKafkaProducer is None:
            LOGGER.warning(
                "aiokafka is not available; strategy signals will only be dispatched in-memory."
            )
            self._started = True
            return

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._kafka_bootstrap_servers,
            client_id=self._client_id,
        )
        await self._producer.start()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        self._started = False

    async def publish(
        self,
        *,
        name: str,
        publisher: str,
        schema: Any,
        features: Any,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Publish a signal payload to Kafka and update the registry."""

        ts = timestamp or datetime.now(timezone.utc)
        normalized_schema = _jsonify(schema)
        normalized_features = _jsonify(features)
        metadata = StrategySignalMetadata(
            name=name,
            publisher=publisher,
            schema=normalized_schema,
            ts=ts,
        )
        self._registry.upsert(metadata)

        payload = {
            "name": name,
            "publisher": publisher,
            "schema": normalized_schema,
            "features": normalized_features,
            "ts": ts.isoformat(),
        }

        if not self._started:
            await self.start()

        if self._producer is None:
            await self._dispatch_locally(payload)
            return

        serialized = json.dumps(payload).encode("utf-8")
        await self._producer.send_and_wait(self._topic, serialized)

    async def _dispatch_locally(self, payload: Dict[str, Any]) -> None:
        if not self._local_subscribers:
            LOGGER.debug("No local subscribers registered for strategy signal payloads.")
            return
        for queue in list(self._local_subscribers):
            await queue.put(dict(payload))

    @asynccontextmanager
    async def subscribe(
        self,
        *,
        group_id: Optional[str] = None,
        auto_offset_reset: str = "latest",
    ) -> AsyncIterator[AsyncIterator[Dict[str, Any]]]:
        """Subscribe to the shared signal topic.

        Returns an async generator yielding decoded payloads. When Kafka is not
        available the generator is backed by an in-memory queue fed by calls to
        :py:meth:`publish`.
        """

        if not self._started:
            await self.start()

        if AIOKafkaConsumer is None:
            queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
            self._local_subscribers.add(queue)

            async def iterator() -> AsyncIterator[Dict[str, Any]]:
                try:
                    while True:
                        payload = await queue.get()
                        yield payload
                finally:
                    self._local_subscribers.discard(queue)

            try:
                yield iterator()
            finally:
                self._local_subscribers.discard(queue)
            return

        consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._kafka_bootstrap_servers,
            client_id=self._client_id,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
        )

        await consumer.start()

        async def kafka_iterator() -> AsyncIterator[Dict[str, Any]]:
            async for message in consumer:
                yield json.loads(message.value.decode("utf-8"))

        try:
            yield kafka_iterator()
        finally:
            await consumer.stop()

    def list_signals(self) -> List[StrategySignalMetadata]:
        """Return the catalog of available shared signals."""

        return self._registry.list()


def ensure_signal_tables(engine: Engine) -> None:
    """Create the strategy signal tables if they do not exist."""

    SignalBase.metadata.create_all(bind=engine)

