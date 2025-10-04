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
    offset_log_dir: Optional[Path | str] = None

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

            logged_offsets = self._load_logged_offsets(assignments)
            offsets = await consumer.offsets_for_times({tp: cutoff_ms for tp in assignments})
            beginnings = await consumer.beginning_offsets(assignments)
            for tp in assignments:
                start_offset = None
                if tp in logged_offsets:
                    start_offset = logged_offsets[tp]
                else:
                    offset_meta = offsets.get(tp)
                    if offset_meta is not None and offset_meta.offset is not None:
                        start_offset = offset_meta.offset
                if start_offset is None:
                    start_offset = beginnings.get(tp, 0)
                consumer.seek(tp, max(0, start_offset))

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

    def _load_logged_offsets(self, assignments: Iterable[Any]) -> Dict[Any, int]:
        if not self.offset_log_dir:
            return {}

        base_path = Path(self.offset_log_dir)
        if base_path.is_dir():
            file_path = base_path / f"{self.account_id}.{self.topic.replace('.', '_')}.json"
        else:
            file_path = base_path

        if not file_path.exists():
            return {}

        try:
            raw = json.loads(file_path.read_text())
        except Exception as exc:  # pragma: no cover - malformed log
            logger.warning(
                "Warm start could not read Kafka offset log for account %s: %s",
                self.account_id,
                exc,
            )
            return {}

        offsets: Dict[Any, int] = {}
        partition_data: Dict[str, Any]
        if isinstance(raw, dict):
            partition_data = raw.get("partitions") if isinstance(raw.get("partitions"), dict) else raw
        elif isinstance(raw, list):
            partition_data = {str(idx): entry for idx, entry in enumerate(raw)}
        else:
            return {}

        for tp in assignments:
            entry = partition_data.get(str(getattr(tp, "partition", tp)))
            if entry is None:
                continue
            offset_value: Optional[int] = None
            if isinstance(entry, dict):
                if "next_offset" in entry:
                    offset_value = _coerce_int(entry.get("next_offset"))
                elif "offset" in entry:
                    offset_value = _coerce_int(entry.get("offset"))
                elif "last_offset" in entry:
                    base_offset = _coerce_int(entry.get("last_offset"))
                    offset_value = None if base_offset is None else base_offset + 1
            else:
                offset_value = _coerce_int(entry)
            if offset_value is None:
                continue
            offsets[tp] = offset_value

        return offsets

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


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):  # pragma: no cover - defensive
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
        offset_log_dir: Optional[str | Path] = None,
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
        self._offset_log_dir = Path(offset_log_dir) if offset_log_dir else None
        if self._offset_log_dir is None:
            env_offset_dir = os.getenv("OMS_WARM_START_OFFSET_LOG_DIR")
            if env_offset_dir:
                self._offset_log_dir = Path(env_offset_dir)
        self._status_lock = asyncio.Lock()
        self._run_lock = asyncio.Lock()
        self._orders_resynced = 0
        self._fills_replayed = 0
        self._latency_ms = 0.0
        self._max_attempts = int(os.getenv("OMS_WARM_START_MAX_ATTEMPTS", "5"))
        self._retry_delay = float(os.getenv("OMS_WARM_START_RETRY_DELAY", "1.0"))

    async def run(self, accounts: Optional[Iterable[str]] = None) -> None:
        async with self._run_lock:
            await self._execute_run(accounts)

    async def _execute_run(self, accounts: Optional[Iterable[str]]) -> None:
        manager = self._manager_getter()
        if manager is None:
            return

        targets = list(accounts or self._discover_accounts())
        async with self._status_lock:
            self._orders_resynced = 0
            self._fills_replayed = 0
            self._latency_ms = 0.0

        if not targets:
            return

        started = time.perf_counter()
        attempts = 0
        total_orders = 0
        total_fills = 0
        while True:
            attempts += 1
            attempt_orders = 0
            attempt_fills = 0
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
                await self._resync_positions(account)
                await self._resync_balances(account)
                trade_fills = await self._replay_account_trades(account)
                kafka_fills = await self._replay_account_fills(account)
                attempt_orders += max(0, orders)
                attempt_fills += max(0, trade_fills + kafka_fills)

            total_orders += attempt_orders
            total_fills += attempt_fills

            if attempt_fills == 0 or attempts >= self._max_attempts:
                break

            await asyncio.sleep(self._retry_delay)

        latency_ms = (time.perf_counter() - started) * 1000.0
        async with self._status_lock:
            self._orders_resynced = total_orders
            self._fills_replayed = total_fills
            self._latency_ms = latency_ms

    async def status(self) -> Dict[str, int]:
        async with self._status_lock:
            return {
                "orders_resynced": self._orders_resynced,
                "fills_replayed": self._fills_replayed,
                "latency_ms": int(self._latency_ms),
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

    async def _resync_positions(self, account: Any) -> int:
        if not hasattr(account, "resync_positions"):
            return 0
        try:
            return int(await account.resync_positions())
        except Exception as exc:
            account_id = getattr(account, "account_id", "<unknown>")
            logger.warning(
                "Warm start position resync failed for account %s: %s",
                account_id,
                exc,
            )
            return 0

    async def _replay_account_trades(self, account: Any) -> int:
        if not hasattr(account, "resync_trades"):
            return 0
        try:
            return int(await account.resync_trades())
        except Exception as exc:
            account_id = getattr(account, "account_id", "<unknown>")
            logger.warning(
                "Warm start trade resync failed for account %s: %s",
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
            offset_log_dir=self._offset_log_dir,
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

    async def _resync_balances(self, account: Any) -> int:
        if not hasattr(account, "resync_balances"):
            return 0
        try:
            result = await account.resync_balances()
        except Exception as exc:
            account_id = getattr(account, "account_id", "<unknown>")
            logger.warning(
                "Warm start balance resync failed for account %s: %s",
                account_id,
                exc,
            )
            return 0
        try:
            return int(result)
        except (TypeError, ValueError):  # pragma: no cover - defensive
            return 0

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
