"""Performance regression test covering OMS latency under burst load."""
from __future__ import annotations

import math
import time
from typing import Dict, List

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("prometheus_client")

from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest

import sequencer
from sequencer import PipelineHistory, SequencerPipeline, Stage, StageResult
from tests.fixtures.mock_kraken import MockKrakenServer

TOTAL_INTENTS = 1000
P95_LATENCY_BUDGET_MS = 200.0
MAX_ERROR_RATE = 0.01


def _percentile(values: List[float], percentile: float) -> float:
    if not values:
        raise AssertionError("no values provided for percentile calculation")
    sorted_values = sorted(values)
    rank = (percentile / 100.0) * (len(sorted_values) - 1)
    lower_index = math.floor(rank)
    upper_index = math.ceil(rank)
    if lower_index == upper_index:
        return sorted_values[lower_index]
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    weight = rank - lower_index
    return lower_value * (1.0 - weight) + upper_value * weight


@pytest.mark.performance
@pytest.mark.slow
def test_oms_latency_within_budget(monkeypatch: pytest.MonkeyPatch, kraken_mock_server: MockKrakenServer) -> None:
    """Submit intents via the Sequencer and ensure OMS latency meets SLOs."""

    kraken_mock_server.config.latency = 0.002

    history = PipelineHistory(capacity=TOTAL_INTENTS)

    async def policy_handler(payload: Dict[str, object], ctx) -> StageResult:
        artifact = {"approved": True, "selected_action": "maker"}
        new_payload = dict(payload)
        new_payload["policy_decision"] = artifact
        return StageResult(payload=new_payload, artifact=artifact)

    async def risk_handler(payload: Dict[str, object], ctx) -> StageResult:
        artifact = {"valid": True, "reasons": []}
        new_payload = dict(payload)
        new_payload["risk_validation"] = artifact
        return StageResult(payload=new_payload, artifact=artifact)

    async def oms_handler(payload: Dict[str, object], ctx) -> StageResult:
        intent = dict(payload.get("intent", {}))
        response = await kraken_mock_server.add_order(
            pair=str(intent.get("instrument", "BTC/USD")),
            side=str(intent.get("side", "buy")).lower(),
            volume=float(intent.get("quantity", 0.0)),
            price=float(intent.get("price", 0.0)),
            account=str(intent.get("account_id", "default")),
            ordertype="limit",
            userref=str(intent.get("order_id")),
        )
        fills = response.get("fills", [])
        filled_qty = sum(float(fill.get("volume", 0.0)) for fill in fills) or float(intent.get("quantity", 0.0))
        notional = sum(float(fill.get("price", 0.0)) * float(fill.get("volume", 0.0)) for fill in fills)
        avg_price = notional / filled_qty if filled_qty and notional else float(intent.get("price", 0.0))
        artifact = {
            "accepted": True,
            "transport": "mock-kraken",
            "client_order_id": intent.get("order_id"),
            "filled_qty": filled_qty,
            "avg_price": avg_price,
            "fills": fills,
            "order": response.get("order", {}),
        }
        new_payload = dict(payload)
        new_payload["oms_result"] = artifact
        return StageResult(payload=new_payload, artifact=artifact)

    pipeline = SequencerPipeline(
        stages=[
            Stage(name="policy", handler=policy_handler, timeout=1.0),
            Stage(name="risk", handler=risk_handler, timeout=1.0),
            Stage(name="oms", handler=oms_handler, timeout=1.0),
        ],
        history=history,
    )

    monkeypatch.setattr(sequencer, "history", history)
    monkeypatch.setattr(sequencer, "pipeline", pipeline)

    registry = CollectorRegistry()
    latency_histogram = Histogram(
        "sequencer_performance_oms_latency_ms",
        "Observed OMS latency per sequencer run in milliseconds.",
        registry=registry,
        buckets=(1, 5, 10, 25, 50, 100, 200, 400, 800, float("inf")),
    )
    error_counter = Counter(
        "sequencer_performance_oms_errors_total",
        "Total errors encountered while submitting intents.",
        registry=registry,
    )

    latencies: List[float] = []
    errors = 0

    with TestClient(sequencer.app) as client:
        for idx in range(TOTAL_INTENTS):
            intent = {
                "account_id": "performance-account",
                "order_id": f"ORD-{idx:05d}",
                "instrument": "BTC/USD",
                "side": "buy",
                "quantity": 0.5,
                "price": 30_000.0,
            }
            started = time.perf_counter()
            response = client.post("/sequencer/submit_intent", json={"intent": intent})
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            if response.status_code != 200:
                errors += 1
                error_counter.inc()
                continue
            payload = response.json()
            stage_latencies = payload.get("stage_latencies_ms", {})
            oms_latency = float(stage_latencies.get("oms", 0.0)) or elapsed_ms
            latencies.append(oms_latency)
            latency_histogram.observe(oms_latency)

    assert latencies, "no successful OMS submissions recorded"

    p95_latency = _percentile(latencies, 95.0)
    error_rate = errors / TOTAL_INTENTS

    summary = {
        "count": len(latencies),
        "p95_ms": round(p95_latency, 3),
        "max_ms": round(max(latencies), 3),
        "errors": errors,
        "error_rate": round(error_rate, 4),
    }
    monkeypatch.setenv("OMS_LATENCY_PERF_SUMMARY", str(summary))

    assert p95_latency < P95_LATENCY_BUDGET_MS, (
        f"OMS stage p95 latency exceeded threshold: {p95_latency:.3f}ms >= {P95_LATENCY_BUDGET_MS}ms"
    )
    assert error_rate < MAX_ERROR_RATE, (
        f"Error rate exceeded threshold: {error_rate:.4f} >= {MAX_ERROR_RATE:.4f}"
    )

    metrics_export = generate_latest(registry).decode("utf-8")
    assert "sequencer_performance_oms_latency_ms_bucket" in metrics_export
    assert "sequencer_performance_oms_errors_total" in metrics_export
