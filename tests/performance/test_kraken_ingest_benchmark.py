"""Pytest-benchmark suite measuring Kraken ingestion latency and throughput."""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Iterable, List

import pytest

pytest.importorskip("pytest_benchmark")

from services.kraken_ws_ingest import KrakenConfig, KrakenIngestor


SAMPLE_TRADES: List[List[str]] = [
    ["43000.1", "0.0125", "1714971020.1234", "b", "l", ""],
    ["43000.4", "0.0210", "1714971020.2234", "s", "m", ""],
    ["43001.2", "0.0340", "1714971020.3234", "b", "l", ""],
]

SAMPLE_BOOK_SNAPSHOT = {
    "as": [["43002.1", "1.2", "1714971020.4234"], ["43002.5", "0.8", "1714971020.5234"]],
    "bs": [["42998.4", "0.9", "1714971020.6234"], ["42998.0", "1.6", "1714971020.7234"]],
}

SAMPLE_BOOK_UPDATE = {
    "a": [["43003.4", "1.05", "1714971020.8234"], ["43004.1", "0.65", "1714971020.9234"]],
    "b": [["42997.8", "1.10", "1714971021.0234"], ["42997.1", "0.55", "1714971021.1234"]],
}

PAIR = "BTC/USD"
BATCHES_PER_RUN = 50
MESSAGES_PER_BATCH = len(SAMPLE_TRADES) + sum(len(levels) for levels in SAMPLE_BOOK_SNAPSHOT.values()) + sum(
    len(levels) for levels in SAMPLE_BOOK_UPDATE.values()
)
PUBLISH_LATENCY_SECONDS = 0.0002
TARGET_MESSAGES_PER_SECOND = 2500.0
TARGET_PER_MESSAGE_LATENCY_MS = 0.5


class BenchmarkProducer:
    """Stub Kafka producer instrumented for benchmarking."""

    def __init__(self, publish_latency: float) -> None:
        self.publish_latency = publish_latency
        self.sent_messages = 0

    async def start(self) -> None:  # pragma: no cover - compatibility shim
        return None

    async def stop(self) -> None:  # pragma: no cover - compatibility shim
        return None

    async def send_and_wait(self, topic: str, payload: dict[str, object]) -> None:
        await asyncio.sleep(self.publish_latency)
        self.sent_messages += 1


@dataclass(slots=True)
class KrakenIngestBatch:
    trades: Iterable[List[str]]
    snapshot: dict[str, list[list[str]]]
    update: dict[str, list[list[str]]]

    async def run(self, ingestor: KrakenIngestor, pair: str) -> None:
        await ingestor._handle_trade_message(self.trades, pair)  # noqa: SLF001 - intentionally exercising internals
        await ingestor._handle_book_message(self.snapshot, pair)  # noqa: SLF001 - intentionally exercising internals
        await ingestor._handle_book_message(self.update, pair)  # noqa: SLF001 - intentionally exercising internals


def _run_ingestion_round(batch: KrakenIngestBatch) -> tuple[float, int]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        config = KrakenConfig(pairs=[PAIR], kafka_bootstrap_servers="kafka:9092")
        ingestor = KrakenIngestor(config)
        producer = BenchmarkProducer(PUBLISH_LATENCY_SECONDS)
        ingestor._producer = producer  # noqa: SLF001 - injecting benchmarking stub

        async def _process() -> None:
            for _ in range(BATCHES_PER_RUN):
                await batch.run(ingestor, "BTC-USD")

        start = time.perf_counter()
        loop.run_until_complete(_process())
        duration = time.perf_counter() - start
        loop.run_until_complete(loop.shutdown_asyncgens())
        return duration, producer.sent_messages
    finally:
        asyncio.set_event_loop(None)
        loop.close()


@pytest.mark.performance
def test_kraken_ingest_benchmark(benchmark, monkeypatch: pytest.MonkeyPatch) -> None:
    """Record baseline ingestion latency and throughput metrics."""

    batch = KrakenIngestBatch(
        trades=SAMPLE_TRADES,
        snapshot=SAMPLE_BOOK_SNAPSHOT,
        update=SAMPLE_BOOK_UPDATE,
    )

    durations: list[float] = []
    message_counts: list[int] = []

    def _target() -> int:
        duration, messages = _run_ingestion_round(batch)
        durations.append(duration)
        message_counts.append(messages)
        return messages

    result = benchmark(_target)
    assert result is not None  # ensure the benchmark executed at least once

    total_messages = sum(message_counts)
    total_duration = sum(durations)
    assert total_messages > 0
    assert total_duration > 0.0

    mean_run_duration = total_duration / len(durations)
    mean_batch_latency_ms = (mean_run_duration / BATCHES_PER_RUN) * 1000.0
    per_message_latency_ms = mean_batch_latency_ms / MESSAGES_PER_BATCH
    throughput_msgs_per_second = total_messages / total_duration

    assert (
        throughput_msgs_per_second >= TARGET_MESSAGES_PER_SECOND
    ), f"Throughput below baseline target: {throughput_msgs_per_second:.2f} msg/s"
    assert (
        per_message_latency_ms <= TARGET_PER_MESSAGE_LATENCY_MS
    ), f"Per-message latency regression detected: {per_message_latency_ms:.4f} ms"

    baseline_snapshot = {
        "runs": len(durations),
        "total_messages": total_messages,
        "messages_per_batch": MESSAGES_PER_BATCH,
        "batches_per_run": BATCHES_PER_RUN,
        "mean_batch_latency_ms": round(mean_batch_latency_ms, 4),
        "per_message_latency_ms": round(per_message_latency_ms, 4),
        "throughput_msgs_per_second": round(throughput_msgs_per_second, 2),
    }
    monkeypatch.setenv("KRAKEN_INGEST_BENCHMARK_BASELINE", json.dumps(baseline_snapshot))
