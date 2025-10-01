from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional

from services.common.config import get_kafka_producer
from services.common.security import ADMIN_ACCOUNTS

try:  # pragma: no cover - optional dependency in CI
    from aiokafka.errors import KafkaError
except Exception:  # pragma: no cover - aiokafka may be unavailable
    KafkaError = Exception  # type: ignore[assignment]


logger = logging.getLogger(__name__)


@dataclass
class KafkaFillReplayer:
    account_id: str
    bootstrap_servers: str
    topic: str
    lookback_seconds: float = 900.0
    poll_timeout: float = 0.5
    idle_grace: float = 2.0
    batch_size: int = 200

    async def replay(self, handler: Callable[[Dict[str, Any]], Awaitable[bool]]) -> int:
        try:  # pragma: no cover - aiokafka optional dependency
            from aiokafka import AIOKafkaConsumer
            from aiokafka.structs import TopicPartition
        except Exception as exc:  # pragma: no cover - skip when aiokafka missing
            logger.warning(
                "Warm start fill replay disabled for account %s: aiokafka unavailable (%s)",
                self.account_id,
                exc,
            )
            return 0

        consumer = AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            enable_auto_commit=False,
            value_deserializer=lambda payload: self._deserialize(payload),
        )
        await consumer.start()
        processed = 0
        try:
            partitions = await consumer.partitions_for_topic(self.topic)
            if not partitions:
                logger.info(
                    "Warm start fill replay: topic %s has no partitions for account %s",
                    self.topic,
                    self.account_id,
                )
                return 0

            assignments = [TopicPartition(self.topic, partition) for partition in partitions]
            consumer.assign(assignments)

            cutoff_ms = int(
                (datetime.now(timezone.utc) - timedelta(seconds=self.lookback_seconds)).timestamp()
                * 1000
            )

            offsets = await consumer.offsets_for_times({tp: cutoff_ms for tp in assignments})
            beginnings = await consumer.beginning_offsets(assignments)
            for tp in assignments:
                offset_meta = offsets.get(tp)
                if offset_meta is not None and offset_meta.offset is not None:
                    consumer.seek(tp, offset_meta.offset)
                else:
                    consumer.seek(tp, beginnings.get(tp, 0))

            idle_deadline = time.monotonic() + self.idle_grace
            while True:
                batch = await consumer.getmany(
                    timeout_ms=int(self.poll_timeout * 1000), max_records=self.batch_size
                )
                if not batch:
                    if time.monotonic() >= idle_deadline:
                        break
                    continue

                idle_deadline = time.monotonic() + self.idle_grace
                for messages in batch.values():
                    for message in messages:
                        if message.timestamp is not None and message.timestamp < cutoff_ms:
                            continue
                        payload = message.value
                        if not isinstance(payload, dict):
                            continue
                        try:
                            applied = await handler(payload)
                        except Exception as exc:  # pragma: no cover - handler failure
                            logger.warning(
                                "Warm start fill handler failed for account %s: %s",
                                self.account_id,
                                exc,
                            )
                            continue
                        if applied:
                            processed += 1
            return processed
        except KafkaError as exc:  # pragma: no cover - broker failure
            logger.warning(
                "Warm start fill replay failed for account %s on topic %s: %s",
                self.account_id,
                self.topic,
                exc,
            )
            return 0
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning(
                "Unexpected warm start fill replay error for account %s: %s",
                self.account_id,
                exc,
            )
            return 0
        finally:
            await consumer.stop()

    @staticmethod
    def _deserialize(payload: bytes | str | Dict[str, Any] | None) -> Dict[str, Any] | None:
        if payload is None:
            return None
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, bytes):
            try:
                return json.loads(payload.decode("utf-8"))
            except Exception:  # pragma: no cover - defensive
                return None
        if isinstance(payload, str):
            try:
                return json.loads(payload)
            except Exception:  # pragma: no cover - defensive
                return None
        return None


class WarmStartCoordinator:
    def __init__(
        self,
        manager_getter: Callable[[], Any],
        *,
        fills_topic: Optional[str] = None,
        lookback_seconds: Optional[float] = None,
        poll_timeout: Optional[float] = None,
        idle_grace: Optional[float] = None,
        batch_size: Optional[int] = None,
    ) -> None:
        self._manager_getter = manager_getter
        self._fills_topic = fills_topic or os.getenv("OMS_WARM_START_FILLS_TOPIC", "oms.executions")
        self._lookback_seconds = (
            lookback_seconds
            if lookback_seconds is not None
            else float(os.getenv("OMS_WARM_START_LOOKBACK_SECONDS", "900"))
        )
        self._poll_timeout = (
            poll_timeout
            if poll_timeout is not None
            else float(os.getenv("OMS_WARM_START_POLL_TIMEOUT", "0.5"))
        )
        self._idle_grace = (
            idle_grace if idle_grace is not None else float(os.getenv("OMS_WARM_START_IDLE_GRACE", "2.0"))
        )
        self._batch_size = (
            batch_size if batch_size is not None else int(os.getenv("OMS_WARM_START_BATCH_SIZE", "200"))
        )
        self._status_lock = asyncio.Lock()
        self._orders_resynced = 0
        self._fills_replayed = 0

    async def run(self, accounts: Optional[Iterable[str]] = None) -> None:
        manager = self._manager_getter()
        if manager is None:
            return

        targets = list(accounts or self._discover_accounts())
        async with self._status_lock:
            self._orders_resynced = 0
            self._fills_replayed = 0

        for account_id in targets:
            try:
                account = await manager.get_account(account_id)
            except Exception as exc:
                logger.warning(
                    "Warm start failed to load account context for %s: %s",
                    account_id,
                    exc,
                )
                continue

            orders = await self._resync_account(account)
            fills = await self._replay_account_fills(account)
            async with self._status_lock:
                self._orders_resynced += orders
                self._fills_replayed += fills

    async def status(self) -> Dict[str, int]:
        async with self._status_lock:
            return {
                "orders_resynced": self._orders_resynced,
                "fills_replayed": self._fills_replayed,
            }

    async def _resync_account(self, account: Any) -> int:
        if not hasattr(account, "resync_from_exchange"):
            return 0
        try:
            return int(await account.resync_from_exchange())
        except Exception as exc:
            account_id = getattr(account, "account_id", "<unknown>")
            logger.warning(
                "Warm start order resync failed for account %s: %s",
                account_id,
                exc,
            )
            return 0

    async def _replay_account_fills(self, account: Any) -> int:
        if not hasattr(account, "apply_fill_event"):
            return 0
        account_id = getattr(account, "account_id", "<unknown>")
        try:
            config = get_kafka_producer(account_id)
        except Exception as exc:
            logger.warning(
                "Warm start skipped Kafka replay for account %s: %s",
                account_id,
                exc,
            )
            return 0

        topic = self._resolve_topic(config.topic_prefix)
        replayer = KafkaFillReplayer(
            account_id=account_id,
            bootstrap_servers=config.bootstrap_servers,
            topic=topic,
            lookback_seconds=self._lookback_seconds,
            poll_timeout=self._poll_timeout,
            idle_grace=self._idle_grace,
            batch_size=self._batch_size,
        )
        try:
            return await replayer.replay(account.apply_fill_event)
        except Exception as exc:
            logger.warning(
                "Warm start replay encountered an error for account %s: %s",
                account_id,
                exc,
            )
            return 0

    def _resolve_topic(self, prefix: str) -> str:
        if prefix and not self._fills_topic.startswith(prefix):
            return f"{prefix}.{self._fills_topic}"
        return self._fills_topic

    def _discover_accounts(self) -> List[str]:
        accounts: set[str] = set()
        configured = os.getenv("OMS_WARM_START_ACCOUNTS")
        if configured:
            accounts.update(part.strip() for part in configured.split(",") if part.strip())
        accounts.update(ADMIN_ACCOUNTS)

        secrets_base = Path(os.getenv("KRAKEN_SECRETS_BASE", "/var/run/secrets/kraken"))
        if secrets_base.exists():
            for entry in secrets_base.iterdir():
                if entry.is_dir():
                    accounts.add(entry.name)
                elif entry.is_file() and entry.suffix == ".json":
                    accounts.add(entry.stem)

        return sorted(accounts)


__all__ = ["KafkaFillReplayer", "WarmStartCoordinator"]
