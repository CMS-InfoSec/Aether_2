"""Locust-driven load test for Kraken WebSocket ingestion."""
from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import dataclass
from statistics import mean
from typing import Awaitable, Iterable, List

import pytest

pytest.importorskip("locust")
from locust import Environment, User, constant, task  # type: ignore  # noqa: E402

try:  # pragma: no cover - prefer the real gevent when available
    gevent = pytest.importorskip("gevent")
except Exception as exc:  # pragma: no cover - surface clear skip reason
    pytest.skip(f"gevent is unavailable for Kraken ingestion load test: {exc}", allow_module_level=True)

from services.kraken_ws_ingest import KrakenConfig, KrakenIngestor  # noqa: E402


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
USER_COUNT = 2
RUN_TIME_SECONDS = 15
WAIT_TIME_SECONDS = 0.1
TARGET_BATCH_LATENCY_MS = 25.0
TARGET_MESSAGES_PER_SECOND = 500.0
MESSAGES_PER_BATCH = len(SAMPLE_TRADES) + sum(len(levels) for levels in SAMPLE_BOOK_SNAPSHOT.values()) + sum(
    len(levels) for levels in SAMPLE_BOOK_UPDATE.values()
)


class AsyncDriver:
    """Minimal wrapper around an asyncio event loop for gevent users."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()

    def run(self, coro: Awaitable[object | None]) -> object | None:
        return self._loop.run_until_complete(coro)

    def close(self) -> None:
        try:
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
        finally:
            self._loop.close()


class RecordingProducer:
    """A stub Kafka producer that records publish counts and simulates latency."""

    def __init__(self, publish_latency: float = 0.0005) -> None:
        self.publish_latency = publish_latency
        self.sent_messages = 0

    async def start(self) -> None:  # pragma: no cover - compatibility with real producer API
        return None

    async def stop(self) -> None:  # pragma: no cover - compatibility with real producer API
        return None

    async def send_and_wait(self, topic: str, payload: dict[str, object]) -> None:
        await asyncio.sleep(self.publish_latency)
        self.sent_messages += 1


class IngestMetricsCollector:
    """Collects ingest batch latency samples and message throughput."""

    def __init__(self) -> None:
        self._latencies_ms: list[float] = []
        self._messages: list[int] = []

    def record(self, latency_ms: float, messages: int) -> None:
        self._latencies_ms.append(latency_ms)
        self._messages.append(messages)

    def percentile(self, pct: float) -> float:
        if not self._latencies_ms:
            raise AssertionError("no latency samples recorded")
        sorted_samples = sorted(self._latencies_ms)
        rank = (pct / 100.0) * (len(sorted_samples) - 1)
        lower_index = math.floor(rank)
        upper_index = math.ceil(rank)
        if lower_index == upper_index:
            return sorted_samples[lower_index]
        lower_value = sorted_samples[lower_index]
        upper_value = sorted_samples[upper_index]
        weight = rank - lower_index
        return lower_value * (1.0 - weight) + upper_value * weight

    def average(self) -> float:
        if not self._latencies_ms:
            return 0.0
        return mean(self._latencies_ms)

    def sample_count(self) -> int:
        return len(self._latencies_ms)

    def messages_per_second(self) -> float:
        total_messages = sum(self._messages)
        total_time_seconds = sum(self._latencies_ms) / 1000.0
        if total_time_seconds <= 0.0:
            return 0.0
        return total_messages / total_time_seconds


@dataclass(slots=True)
class KrakenIngestBatch:
    trades: Iterable[List[str]]
    snapshot: dict[str, list[list[str]]]
    update: dict[str, list[list[str]]]

    async def run(self, ingestor: KrakenIngestor, pair: str) -> None:
        await ingestor._handle_trade_message(self.trades, pair)  # noqa: SLF001 - exercising internal pipeline
        await ingestor._handle_book_message(self.snapshot, pair)  # noqa: SLF001 - exercising internal pipeline
        await ingestor._handle_book_message(self.update, pair)  # noqa: SLF001 - exercising internal pipeline


class KrakenIngestUser(User):
    wait_time = constant(WAIT_TIME_SECONDS)

    def __init__(self, environment: Environment) -> None:
        super().__init__(environment)
        self._driver: AsyncDriver | None = None
        self._ingestor: KrakenIngestor | None = None
        self._producer: RecordingProducer | None = None
        self._batch = KrakenIngestBatch(
            trades=SAMPLE_TRADES,
            snapshot=SAMPLE_BOOK_SNAPSHOT,
            update=SAMPLE_BOOK_UPDATE,
        )

    def on_start(self) -> None:  # pragma: no cover - executed by Locust
        self._driver = AsyncDriver()
        config = KrakenConfig(pairs=[PAIR], kafka_bootstrap_servers="kafka:9092")
        self._ingestor = KrakenIngestor(config)
        self._producer = RecordingProducer()
        # Inject the stub producer so messages are captured locally during tests.
        self._ingestor._producer = self._producer  # noqa: SLF001 - patching internal collaborator for testability

    def on_stop(self) -> None:  # pragma: no cover - executed by Locust
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    @task
    def process_batch(self) -> None:  # pragma: no cover - executed by Locust
        assert self._driver is not None and self._ingestor is not None and self._producer is not None

        before = self._producer.sent_messages
        start = time.perf_counter()
        self._driver.run(self._batch.run(self._ingestor, "BTC-USD"))
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        produced = self._producer.sent_messages - before

        if produced <= 0:
            raise AssertionError("Kraken ingestion produced no Kafka messages")

        collector: IngestMetricsCollector = getattr(self.environment, "collector")
        collector.record(elapsed_ms, produced)

        self.environment.events.request_success.fire(
            request_type="ingest",
            name="kraken_batch",
            response_time=elapsed_ms,
            response_length=produced,
        )


@pytest.mark.load
@pytest.mark.slow
def test_kraken_ingest_sustains_load(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure the Kraken ingestion pipeline maintains latency and throughput targets."""

    collector = IngestMetricsCollector()
    environment = Environment(user_classes=[KrakenIngestUser])
    setattr(environment, "collector", collector)
    runner = environment.create_local_runner()

    runner.start(user_count=USER_COUNT, spawn_rate=USER_COUNT)
    gevent.sleep(RUN_TIME_SECONDS)
    runner.quit()
    runner.greenlet.join()

    stats_entry = environment.stats.get("kraken_batch", method="ingest")
    assert stats_entry.num_requests > 0, "no ingestion batches were processed"

    p95_latency = collector.percentile(95.0)
    assert (
        p95_latency <= TARGET_BATCH_LATENCY_MS
    ), f"Kraken ingestion p95 latency exceeded target: {p95_latency:.2f}ms"

    throughput = collector.messages_per_second()
    assert (
        throughput >= TARGET_MESSAGES_PER_SECOND
    ), f"Kraken ingestion throughput below target: {throughput:.2f} msg/s"

    latency_snapshot = {
        "samples": collector.sample_count(),
        "p50_batch_latency_ms": round(collector.percentile(50.0), 3),
        "p95_batch_latency_ms": round(p95_latency, 3),
        "avg_batch_latency_ms": round(collector.average(), 3),
        "messages_per_second": round(throughput, 2),
        "messages_per_batch": MESSAGES_PER_BATCH,
        "total_messages": stats_entry.num_requests * MESSAGES_PER_BATCH,
    }
    monkeypatch.setenv("KRAKEN_INGEST_LOAD_BASELINE", json.dumps(latency_snapshot))
