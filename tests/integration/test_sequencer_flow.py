"""Integration test validating the sequencer orchestration pipeline."""

from __future__ import annotations

from typing import Any, Dict, Iterable

import pytest

pytest.importorskip("fastapi")

from sequencer import pipeline
from services.common.adapters import KafkaNATSAdapter


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sequencer_flow_emits_complete_audit_trail(monkeypatch: pytest.MonkeyPatch) -> None:
    """Submitting an intent yields policy→risk→OMS flow with consistent auditing."""

    # Ensure deterministic state for the in-memory adapters.
    KafkaNATSAdapter.reset()

    # ``latest_override`` consults a SQLite store; stub it out so we remain in-memory.
    monkeypatch.setattr("override_service.latest_override", lambda intent_id: None)

    intent: Dict[str, Any] = {
        "account_id": "AlphaDesk",
        "order_id": "ORD-1001",
        "instrument": "BTC-USD",
        "side": "buy",
        "quantity": 1.5,
        "price": 34_250.0,
        "venue": "demo-exchange",
        "correlation_id": "corr-alpha-42",
    }

    result = await pipeline.submit(intent)

    # ------------------------------------------------------------------
    # Successful pipeline execution with recorded artifacts.
    # ------------------------------------------------------------------
    assert result.status == "success"
    assert result.run_id
    assert set(result.stage_artifacts) == {"policy", "risk", "override", "oms"}
    assert result.fill_event["event_type"] == "FillEvent"
    assert result.fill_event["run_id"] == result.run_id
    assert result.fill_event["client_order_id"] == intent["order_id"]
    assert result.fill_event["order_id"] == result.fill_event["exchange_order_id"]
    assert result.fill_event["status"] == "filled"

    # ------------------------------------------------------------------
    # Audit log emitted the lifecycle (start/complete) for every stage.
    # ------------------------------------------------------------------
    account_key = intent["account_id"].strip().lower()
    history: Iterable[Dict[str, Any]] = KafkaNATSAdapter(account_id=account_key).history()
    topics = [entry["topic"] for entry in history]

    expected_sequence = [
        "sequencer.pipeline.start",
        "sequencer.policy.start",
        "sequencer.policy.complete",
        "sequencer.risk.start",
        "sequencer.risk.complete",
        "sequencer.override.start",
        "sequencer.override.complete",
        "sequencer.oms.start",
        "sequencer.oms.complete",
        "sequencer.fill.publish",
        "sequencer.pipeline.complete",
    ]
    assert topics == expected_sequence

    for stage in ("policy", "risk", "override", "oms"):
        assert f"sequencer.{stage}.start" in topics
        assert f"sequencer.{stage}.complete" in topics

    # ------------------------------------------------------------------
    # Correlation identifiers remain attached to payloads for each stage.
    # ------------------------------------------------------------------
    correlation_id = intent["correlation_id"]
    stage_start_events = {
        entry["topic"]: entry["payload"]
        for entry in history
        if entry["topic"].endswith(".start")
    }

    pipeline_start_data = stage_start_events["sequencer.pipeline.start"]["data"]
    assert pipeline_start_data["intent"]["correlation_id"] == correlation_id

    for stage in ("policy", "risk", "override", "oms"):
        event_payload = stage_start_events[f"sequencer.{stage}.start"]["data"]
        stage_intent = event_payload["payload"]["intent"]
        assert stage_intent["correlation_id"] == correlation_id

    # Final fill event should reference all stage artifacts to demonstrate the lifecycle closure.
    lifecycle_artifacts = result.fill_event["stage_artifacts"]
    assert lifecycle_artifacts.keys() == {"policy", "risk", "override", "oms"}
