from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterable, Mapping

import pytest

from common.schemas.contracts import IntentEvent
import common.utils.tracing as tracing_module
from services.common.config import TimescaleSession


def _load_sequencer_module() -> ModuleType:
    try:
        from shared.common_bootstrap import ensure_common_helpers
    except Exception:  # pragma: no cover - bootstrap may be unavailable
        ensure_common_helpers = None  # type: ignore[assignment]

    if ensure_common_helpers is not None:
        ensure_common_helpers()

    try:
        return importlib.import_module("services.core.sequencer")
    except Exception:
        root = Path(__file__).resolve().parents[2]

        if "services.common.adapters" not in sys.modules:
            adapters_module = ModuleType("services.common.adapters")

            class _StubKafkaAdapter:
                def __init__(self, account_id: str) -> None:  # pragma: no cover - simple stub
                    self.account_id = account_id

                async def publish(self, topic: str, payload: Dict[str, Any]) -> None:  # pragma: no cover
                    return None

            adapters_module.KafkaNATSAdapter = _StubKafkaAdapter
            sys.modules["services.common.adapters"] = adapters_module

        spec = importlib.util.spec_from_file_location(
            "services.core.sequencer", root / "services" / "core" / "sequencer.py"
        )
        if spec is None or spec.loader is None:  # pragma: no cover - defensive
            raise RuntimeError("Failed to load sequencer module spec")
        module = importlib.util.module_from_spec(spec)
        sys.modules["services.core.sequencer"] = module
        spec.loader.exec_module(module)
        return module


sequencer_module = _load_sequencer_module()
TradingSequencer = sequencer_module.TradingSequencer


class InMemoryLifecycleLogger:
    instances: list["InMemoryLifecycleLogger"] = []

    def __init__(self, session: TimescaleSession) -> None:  # pragma: no cover - signature only
        self.session = session
        self.records: list[Dict[str, Any]] = []
        self.__class__.instances.append(self)

    async def record(self, **kwargs: Any) -> None:
        self.records.append(dict(kwargs))


class InMemoryLatencyRecorder:
    def __init__(self, session: TimescaleSession) -> None:  # pragma: no cover - signature only
        self.session = session
        self.records: list[Dict[str, Any]] = []

    async def record(self, **kwargs: Any) -> None:
        self.records.append(dict(kwargs))


class StubRiskService:
    def __init__(self) -> None:
        self.intents: list[tuple[IntentEvent, str]] = []
        self.fills: list[tuple[Any, str]] = []

    async def validate_intent(self, intent: IntentEvent, *, correlation_id: str) -> Mapping[str, Any]:
        self.intents.append((intent, correlation_id))
        assert intent.intent["correlation_id"] == correlation_id
        return {"approved": True, "decision_id": "risk-1"}

    async def handle_fill(self, fill: Any, *, correlation_id: str) -> None:
        self.fills.append((fill, correlation_id))


class StubOMSClient:
    def __init__(self) -> None:
        self.orders: list[tuple[IntentEvent, Mapping[str, Any], str]] = []
        self.fill_streams: list[tuple[Mapping[str, Any], str]] = []

    async def place_order(
        self,
        intent: IntentEvent,
        decision: Mapping[str, Any],
        *,
        correlation_id: str,
    ) -> Mapping[str, Any]:
        self.orders.append((intent, decision, correlation_id))
        assert decision.get("correlation_id") == correlation_id
        return {"order_id": "order-1", "status": "submitted"}

    async def stream_fills(
        self,
        order: Mapping[str, Any],
        *,
        correlation_id: str,
    ) -> Iterable[Mapping[str, Any]]:
        self.fill_streams.append((order, correlation_id))
        yield {
            "fill_id": "fill-1",
            "qty": 1,
            "price": 10,
            "fee": 0.1,
            "ts": datetime.now(timezone.utc).isoformat(),
            "status": "filled",
        }


class StubPnLTracker:
    def __init__(self) -> None:
        self.fills: list[tuple[Any, str]] = []

    async def handle_fill(self, fill: Any, *, correlation_id: str) -> None:
        self.fills.append((fill, correlation_id))


class StubKafkaAdapter:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.published: list[tuple[str, Dict[str, Any]]] = []

    async def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        self.published.append((topic, dict(payload)))


def test_trading_sequencer_logs_full_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    InMemoryLifecycleLogger.instances.clear()

    monkeypatch.setattr(sequencer_module, "_TradeLifecycleLogger", InMemoryLifecycleLogger)
    monkeypatch.setattr(sequencer_module, "_LatencyRecorder", InMemoryLatencyRecorder)
    monkeypatch.setattr(tracing_module, "_STAGE_LATENCY_HISTOGRAM", None)

    dummy_session = TimescaleSession(dsn="sqlite:///:memory:", account_schema="acct_test")
    monkeypatch.setattr(
        sequencer_module,
        "get_timescale_session",
        lambda account_id: dummy_session,
    )

    risk_service = StubRiskService()
    oms_client = StubOMSClient()
    pnl_tracker = StubPnLTracker()

    sequencer = TradingSequencer(
        risk_service=risk_service,
        oms=oms_client,
        pnl_tracker=pnl_tracker,
        kafka_factory=lambda account_id: StubKafkaAdapter(account_id),
    )

    event = IntentEvent(
        account_id="acct-test",
        symbol="BTCUSD",
        intent={"strategy": "alpha"},
        ts=datetime.now(timezone.utc),
    )

    result = asyncio.run(sequencer.process_intent(event))

    assert result.status == "filled"
    assert result.correlation_id

    logger = InMemoryLifecycleLogger.instances[0]
    stages = [entry["stage"] for entry in logger.records]
    assert stages == ["policy", "risk", "oms", "fill", "pnl"]
    assert all(entry["correlation_id"] == result.correlation_id for entry in logger.records)

    assert risk_service.intents[0][1] == result.correlation_id
    assert risk_service.fills[0][1] == result.correlation_id
    assert pnl_tracker.fills[0][1] == result.correlation_id

    placed_intent, decision_payload, decision_corr = oms_client.orders[0]
    assert decision_corr == result.correlation_id
    assert decision_payload["correlation_id"] == result.correlation_id
    assert placed_intent.intent["correlation_id"] == result.correlation_id
