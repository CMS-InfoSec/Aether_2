"""Replay Engine for deterministic Kafka event replays.

This module provides utilities to replay historical Kafka topics in order
and with controllable pacing.  It is designed for operational backtesting
where new downstream services can be validated against historical events.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional
from uuid import uuid4

try:
    from aiokafka import AIOKafkaConsumer  # type: ignore
except ImportError:  # pragma: no cover - optional dependency for runtime
    AIOKafkaConsumer = None  # type: ignore


LOG = logging.getLogger(__name__)
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_LOG_PATH = Path(os.getenv("REPLAY_LOG_PATH", "reports/replay_log.jsonl"))


class ReplayError(RuntimeError):
    """Raised when the replay engine encounters a fatal configuration error."""


def _ensure_consumer_available() -> None:
    if AIOKafkaConsumer is None:
        raise ReplayError(
            "aiokafka is required for replay functionality. Install aiokafka to continue."
        )


def _coerce_datetime(value: Any) -> datetime:
    """Convert a value into an aware UTC datetime."""
    if value is None:
        raise ReplayError("Timestamp value cannot be None")

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError as exc:  # pragma: no cover - defensive
            raise ReplayError(f"Invalid datetime string: {value!r}") from exc
    else:
        raise ReplayError(f"Unsupported timestamp type: {type(value)!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


async def _replay_async(
    topic: str,
    start_ts: datetime,
    end_ts: Optional[datetime],
    *,
    bootstrap_servers: str,
    group_id: str,
    pace: str,
    playback_speed: float,
    batch_size: int,
    event_handler: Optional[Callable[[Dict[str, Any]], None]],
) -> int:
    _ensure_consumer_available()

    consumer = AIOKafkaConsumer(  # type: ignore[misc]
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    events_replayed = 0
    start_ms = int(start_ts.timestamp() * 1000)
    end_ms = int(end_ts.timestamp() * 1000) if end_ts else None

    await consumer.start()
    try:
        # Ensure partitions are assigned before seeking by timestamp.
        while not consumer.assignment():
            await asyncio.sleep(0.1)

        assignments = consumer.assignment()
        timestamps = {tp: start_ms for tp in assignments}
        offsets = await consumer.offsets_for_times(timestamps)
        for tp, offset_and_ts in offsets.items():
            if offset_and_ts is None:
                consumer.seek_to_end(tp)
            else:
                consumer.seek(tp, offset_and_ts.offset)

        # Baseline timestamps for pacing calculations.
        first_event_ts = None
        loop = asyncio.get_running_loop()
        loop_start = loop.time()

        while True:
            batch = await consumer.getmany(timeout_ms=1000, max_records=batch_size)
            if not batch:
                break

            for _tp, messages in batch.items():
                for message in messages:
                    msg_ts = message.timestamp
                    if msg_ts is None:
                        continue
                    if end_ms and msg_ts > end_ms:
                        return events_replayed

                    event_time = datetime.fromtimestamp(msg_ts / 1000.0, tz=timezone.utc)
                    if pace != "fast":
                        if first_event_ts is None:
                            first_event_ts = msg_ts
                        elapsed_event = (msg_ts - first_event_ts) / 1000.0
                        elapsed_wall = loop.time() - loop_start
                        target_delay = elapsed_event / max(playback_speed, 1e-9)
                        delay = target_delay - elapsed_wall
                        if delay > 0:
                            await asyncio.sleep(delay)

                    payload = {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "timestamp": event_time.isoformat(),
                        "value": message.value,
                        "key": message.key,
                    }
                    if event_handler:
                        event_handler(payload)
                    else:
                        LOG.debug("Replayed event: %s", payload)
                    events_replayed += 1
    finally:
        await consumer.stop()

    return events_replayed


def replay_log(run_id: str, topic: str, events_replayed: int, ts: Optional[datetime] = None) -> None:
    """Persist replay metadata to a JSON Lines file."""
    timestamp = (ts or datetime.now(tz=timezone.utc)).isoformat()
    record = {
        "run_id": run_id,
        "topic": topic,
        "events_replayed": events_replayed,
        "timestamp": timestamp,
    }

    DEFAULT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with DEFAULT_LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")

    LOG.info(
        "Replay run %s logged: topic=%s events=%s timestamp=%s",
        run_id,
        topic,
        events_replayed,
        timestamp,
    )


def replay_from_kafka(
    topic: str,
    start_ts: Any,
    end_ts: Optional[Any] = None,
    *,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    group_id: str = "replay-engine",
    pace: str = "realtime",
    playback_speed: float = 1.0,
    batch_size: int = 500,
    event_handler: Optional[Callable[[Dict[str, Any]], None]] = None,
    run_id: Optional[str] = None,
    log_results: bool = True,
) -> int:
    """Replay events from Kafka between two timestamps."""
    start_dt = _coerce_datetime(start_ts)
    end_dt = _coerce_datetime(end_ts) if end_ts else None

    if pace not in {"realtime", "fast"}:
        raise ReplayError("pace must be either 'realtime' or 'fast'")
    if playback_speed <= 0:
        raise ReplayError("playback_speed must be positive")

    run_identifier = run_id or uuid4().hex
    events = asyncio.run(
        _replay_async(
            topic,
            start_dt,
            end_dt,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            pace="fast" if pace == "fast" else "realtime",
            playback_speed=playback_speed,
            batch_size=batch_size,
            event_handler=event_handler,
        )
    )

    if log_results:
        replay_log(run_identifier, topic, events)

    return events


def _parse_args(args: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay Kafka topics for backtesting")
    parser.add_argument("--topic", required=True, help="Kafka topic to replay")
    parser.add_argument(
        "--start-ts",
        help="ISO8601 start timestamp. Defaults to now minus --hours if provided.",
    )
    parser.add_argument(
        "--end-ts",
        help="ISO8601 end timestamp. Defaults to now if --hours is provided without end.",
    )
    parser.add_argument(
        "--hours",
        type=float,
        help="Convenience window length in hours counted backwards from now.",
    )
    parser.add_argument(
        "--pace",
        choices=["realtime", "fast"],
        default="realtime",
        help="Real-time pacing or fast-forward without delays.",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Playback speed multiplier when pacing in real-time.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP,
        help="Kafka bootstrap servers (comma separated).",
    )
    parser.add_argument(
        "--group-id",
        default="replay-engine",
        help="Kafka consumer group id for the replay session.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Maximum number of records to fetch per poll.",
    )
    parser.add_argument(
        "--run-id",
        help="Optional run identifier for tracking/logging purposes.",
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Disable replay result logging.",
    )
    return parser.parse_args(args=args)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = _parse_args(argv)

    if args.hours and not args.start_ts:
        args.start_ts = (datetime.now(tz=timezone.utc) - timedelta(hours=args.hours)).isoformat()
        if not args.end_ts:
            args.end_ts = datetime.now(tz=timezone.utc).isoformat()
    elif args.hours and args.start_ts:
        LOG.warning("--hours ignored because --start-ts was provided explicitly")

    events = replay_from_kafka(
        args.topic,
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        pace=args.pace,
        playback_speed=args.speed,
        batch_size=args.batch_size,
        run_id=args.run_id,
        log_results=not args.no_log,
    )

    print(json.dumps({"topic": args.topic, "events_replayed": events}))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
