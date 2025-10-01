"""Locust-based load test validating sequencer throughput and latency targets."""

from __future__ import annotations

import math
import time
from statistics import mean
from typing import Dict, Iterable
from uuid import uuid4

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

pytest.importorskip("locust")
from locust import Environment, User, constant, task
from prometheus_client import CollectorRegistry, Histogram, generate_latest

from sequencer import app


gevent = pytest.importorskip("gevent")


TARGET_REQUESTS_PER_MINUTE = 100
RUN_TIME_SECONDS = 20
USER_COUNT = 2
WAIT_TIME_SECONDS = 1.2
STAGES_OF_INTEREST = ("policy", "risk", "oms")


class StageLatencyCollector:
    """Collects stage-level latency samples and exposes Prometheus histograms."""

    def __init__(self, stages: Iterable[str]) -> None:
        self._samples: Dict[str, list[float]] = {stage: [] for stage in stages}
        self.registry = CollectorRegistry()
        self._histograms: Dict[str, Histogram] = {
            stage: Histogram(
                f"sequencer_{stage}_latency_ms",
                f"Latency of the {stage} stage in milliseconds.",
                registry=self.registry,
                buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000, float("inf")),
            )
            for stage in stages
        }

    def record(self, latencies: Dict[str, float]) -> None:
        for stage in self._samples:
            value = float(latencies.get(stage, 0.0))
            if value <= 0.0:
                continue
            self._samples[stage].append(value)
            self._histograms[stage].observe(value)

    def percentile(self, stage: str, pct: float) -> float:
        samples = self._samples.get(stage, [])
        if not samples:
            raise AssertionError(f"no latency samples recorded for stage '{stage}'")
        sorted_samples = sorted(samples)
        rank = (pct / 100.0) * (len(sorted_samples) - 1)
        lower_index = math.floor(rank)
        upper_index = math.ceil(rank)
        if lower_index == upper_index:
            return sorted_samples[lower_index]
        lower_value = sorted_samples[lower_index]
        upper_value = sorted_samples[upper_index]
        weight = rank - lower_index
        return lower_value * (1.0 - weight) + upper_value * weight

    def sample_count(self, stage: str) -> int:
        return len(self._samples.get(stage, []))

    def average(self, stage: str) -> float:
        samples = self._samples.get(stage, [])
        if not samples:
            return 0.0
        return mean(samples)


class SequencerUser(User):
    wait_time = constant(WAIT_TIME_SECONDS)

    def __init__(self, environment: Environment) -> None:
        super().__init__(environment)
        self._client: TestClient | None = None

    def on_start(self) -> None:  # pragma: no cover - executed by Locust
        self._client = TestClient(app)

    def on_stop(self) -> None:  # pragma: no cover - executed by Locust
        if self._client is not None:
            self._client.close()
            self._client = None

    @task
    def submit_intent(self) -> None:  # pragma: no cover - executed by Locust
        assert self._client is not None, "HTTP client was not initialised"
        payload = {
            "intent": {
                "account_id": "load-test-account",
                "order_id": f"ORD-{uuid4()}",
                "instrument": "BTC-USD",
                "side": "BUY",
                "quantity": 0.25,
                "price": 27500.5,
                "constraints": {"max_slippage_bps": 25},
            }
        }
        start = time.perf_counter()
        response = self._client.post("/sequencer/submit_intent", json=payload)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        try:
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - failure path exercised under load
            self.environment.events.request_failure.fire(
                request_type="POST",
                name="/sequencer/submit_intent",
                response_time=elapsed_ms,
                exception=exc,
            )
            raise

        data = response.json()
        self.environment.events.request_success.fire(
            request_type="POST",
            name="/sequencer/submit_intent",
            response_time=elapsed_ms,
            response_length=len(response.content),
        )
        collector: StageLatencyCollector = getattr(self.environment, "collector")
        collector.record(data.get("stage_latencies_ms", {}))


@pytest.mark.load
@pytest.mark.slow
def test_sequencer_throughput_under_load(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure the sequencer sustains throughput while meeting latency SLOs."""

    collector = StageLatencyCollector(STAGES_OF_INTEREST)

    environment = Environment(user_classes=[SequencerUser])
    # Stash the collector so Locust users can access it without relying on globals.
    setattr(environment, "collector", collector)
    runner = environment.create_local_runner()

    runner.start(user_count=USER_COUNT, spawn_rate=USER_COUNT)
    gevent.sleep(RUN_TIME_SECONDS)
    runner.quit()
    runner.greenlet.join()

    stats_entry = environment.stats.get("/sequencer/submit_intent", method="POST")
    assert stats_entry.num_requests > 0, "no sequencer intents were submitted"

    last_timestamp = stats_entry.last_request_timestamp or stats_entry.start_time
    actual_duration = max(last_timestamp - stats_entry.start_time, 1e-6)
    requests_per_minute = stats_entry.num_requests / actual_duration * 60.0
    assert requests_per_minute >= TARGET_REQUESTS_PER_MINUTE * 0.95

    stage_latencies_summary: Dict[str, float] = {}
    for stage in STAGES_OF_INTEREST:
        p95_latency = collector.percentile(stage, 95.0)
        stage_latencies_summary[stage] = p95_latency
        assert (
            p95_latency < 250.0
        ), f"Stage '{stage}' exceeded the 250ms p95 latency target (observed {p95_latency:.2f}ms)"

    metrics_output = generate_latest(collector.registry).decode("utf-8")
    for stage in STAGES_OF_INTEREST:
        metric_name = f"sequencer_{stage}_latency_ms_bucket"
        assert metric_name in metrics_output

    # Provide a concise log-friendly summary for debugging if the test fails.
    latency_snapshot = {
        stage: {
            "samples": collector.sample_count(stage),
            "p95_ms": round(stage_latencies_summary[stage], 3),
            "avg_ms": round(collector.average(stage), 3),
        }
        for stage in STAGES_OF_INTEREST
    }
    monkeypatch.setenv("SEQUENCER_LOAD_LATENCY_SNAPSHOT", str(latency_snapshot))
