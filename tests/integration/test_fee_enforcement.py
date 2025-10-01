from __future__ import annotations

import asyncio
import contextlib
import importlib
import sys
import types
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict

import pytest


def test_fee_enforcement_blocks_negative_edge(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure intents with insufficient edge are rejected before reaching the OMS."""

    metrics_stub = types.SimpleNamespace(
        increment_rejected_intents=lambda *args, **kwargs: None,
        increment_trades_submitted=lambda *args, **kwargs: None,
        observe_policy_inference_latency=lambda *args, **kwargs: None,
        observe_risk_validation_latency=lambda *args, **kwargs: None,
        observe_oms_submit_latency=lambda *args, **kwargs: None,
        set_pipeline_latency=lambda *args, **kwargs: None,
        setup_metrics=lambda *args, **kwargs: None,
        traced_span=lambda *args, **kwargs: contextlib.nullcontext(),
    )
    monkeypatch.setitem(sys.modules, "metrics", metrics_stub)

    fastapi_stub = types.ModuleType("fastapi")

    class _FastAPI:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.routes: list[types.SimpleNamespace] = []

        def post(self, path: str, **_: object):
            def decorator(func):
                self.routes.append(types.SimpleNamespace(path=path, methods=["POST"]))
                return func

            return decorator

        def get(self, path: str, **_: object):
            def decorator(func):
                self.routes.append(types.SimpleNamespace(path=path, methods=["GET"]))
                return func

            return decorator

        def on_event(self, *_args: object, **_kwargs: object):
            def decorator(func):
                return func

            return decorator

    class _HTTPException(Exception):
        def __init__(self, status_code: int, detail: str) -> None:
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    fastapi_stub.FastAPI = _FastAPI  # type: ignore[attr-defined]
    fastapi_stub.HTTPException = _HTTPException  # type: ignore[attr-defined]

    status_module = types.ModuleType("fastapi.status")
    status_module.HTTP_504_GATEWAY_TIMEOUT = 504
    status_module.HTTP_400_BAD_REQUEST = 400
    fastapi_stub.status = status_module  # type: ignore[attr-defined]

    encoders_module = types.ModuleType("fastapi.encoders")

    def _jsonable_encoder(value: Any, *args: object, **kwargs: object) -> Any:
        del args, kwargs
        return value

    encoders_module.jsonable_encoder = _jsonable_encoder  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "fastapi", fastapi_stub)
    monkeypatch.setitem(sys.modules, "fastapi.status", status_module)
    monkeypatch.setitem(sys.modules, "fastapi.encoders", encoders_module)

    pydantic_stub = types.ModuleType("pydantic")

    class _BaseModel:
        def __init__(self, **data: Any) -> None:
            for key, value in data.items():
                setattr(self, key, value)

        @classmethod
        def model_validate(cls, data: Dict[str, Any]):
            return cls(**data)

        def model_dump(self, *args: object, **kwargs: object) -> Dict[str, Any]:
            del args, kwargs
            return dict(self.__dict__)

    def _Field(default: Any = None, **kwargs: Any) -> Any:
        factory = kwargs.get("default_factory")
        if factory is not None:
            return factory()
        return default

    pydantic_stub.BaseModel = _BaseModel  # type: ignore[attr-defined]
    pydantic_stub.Field = _Field  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "pydantic", pydantic_stub)

    adapters_stub = types.ModuleType("services.common.adapters")

    class _KafkaNATSAdapter:
        _event_store: Dict[str, list[Dict[str, Any]]] = {}

        def __init__(self, account_id: str) -> None:
            self.account_id = account_id
            self._event_store.setdefault(account_id, [])

        def publish(self, topic: str, payload: Dict[str, Any]) -> None:
            record = {"topic": topic, "payload": dict(payload)}
            self._event_store[self.account_id].append(record)

        def history(self) -> list[Dict[str, Any]]:
            return list(self._event_store.get(self.account_id, []))

        @classmethod
        def reset(cls, account_id: str | None = None) -> None:
            if account_id is None:
                cls._event_store.clear()
            else:
                cls._event_store.pop(account_id, None)

    adapters_stub.KafkaNATSAdapter = _KafkaNATSAdapter  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "services.common.adapters", adapters_stub)

    override_stub = types.ModuleType("override_service")

    class OverrideDecision(Enum):
        APPROVE = "approve"
        REJECT = "reject"

    @dataclass
    class OverrideRecord:
        intent_id: str
        account_id: str
        actor: str
        decision: OverrideDecision
        reason: str
        ts: datetime

    def latest_override(intent_id: str):  # type: ignore[unused-arg]
        return None

    override_stub.OverrideDecision = OverrideDecision  # type: ignore[attr-defined]
    override_stub.OverrideRecord = OverrideRecord  # type: ignore[attr-defined]
    override_stub.latest_override = latest_override  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "override_service", override_stub)

    monkeypatch.delitem(sys.modules, "sequencer", raising=False)
    sequencer_module = importlib.import_module("sequencer")

    Stage = sequencer_module.Stage
    StageFailedError = sequencer_module.StageFailedError
    StageResult = sequencer_module.StageResult
    PipelineHistory = sequencer_module.PipelineHistory
    SequencerPipeline = sequencer_module.SequencerPipeline
    KafkaNATSAdapter = adapters_stub.KafkaNATSAdapter

    KafkaNATSAdapter.reset()

    history = PipelineHistory(capacity=4)
    oms_calls: list[Dict[str, Any]] = []

    async def policy_handler(payload: Dict[str, Any], ctx) -> StageResult:
        decision = {
            "approved": True,
            "reason": None,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "expected_edge_bps": 3.2,
            "selected_action": "maker",
            "effective_fee": {"maker": 5.5, "taker": 7.8},
            "action_templates": [
                {"name": "maker", "edge_bps": 3.2, "fee_bps": 5.5},
                {"name": "taker", "edge_bps": 2.1, "fee_bps": 7.8},
            ],
        }
        new_payload = dict(payload)
        new_payload["policy_decision"] = decision
        return StageResult(payload=new_payload, artifact=decision)

    async def risk_handler(payload: Dict[str, Any], ctx) -> StageResult:
        decision = payload.get("policy_decision", {})
        expected_edge = float(decision.get("expected_edge_bps", 0.0))
        selected_action = str(decision.get("selected_action", "")).lower()
        templates = decision.get("action_templates", [])
        selected_template = next(
            (
                template
                for template in templates
                if str(template.get("name", "")).lower() == selected_action
            ),
            None,
        )
        fee_bps = float(selected_template.get("fee_bps", 0.0)) if selected_template else 0.0
        if expected_edge < fee_bps:
            raise StageFailedError("risk", "rejected: insufficient edge")

        assessment = {
            "valid": True,
            "reasons": [],
            "assessed_at": datetime.now(timezone.utc).isoformat(),
        }
        new_payload = dict(payload)
        new_payload["risk_validation"] = assessment
        return StageResult(payload=new_payload, artifact=assessment)

    async def oms_handler(payload: Dict[str, Any], ctx) -> StageResult:
        oms_calls.append(payload)
        artifact = {
            "accepted": True,
            "client_order_id": payload.get("intent", {}).get("order_id"),
        }
        return StageResult(payload=payload, artifact=artifact)

    pipeline = SequencerPipeline(
        stages=[
            Stage(name="policy", handler=policy_handler, timeout=1.0),
            Stage(name="risk", handler=risk_handler, timeout=1.0),
            Stage(name="oms", handler=oms_handler, timeout=1.0),
        ],
        history=history,
    )

    intent = {
        "account_id": "company",
        "order_id": "ORD-EDGE-001",
        "instrument": "BTC-USD",
        "side": "buy",
        "quantity": 0.25,
        "price": 30_000.0,
    }

    async def run_pipeline() -> None:
        with pytest.raises(StageFailedError) as excinfo:
            await pipeline.submit(intent)

        assert excinfo.value.stage == "risk"
        assert "rejected: insufficient edge" in excinfo.value.message
        assert not oms_calls, "OMS stage should not be invoked when risk rejects the trade"

        events = KafkaNATSAdapter(account_id="company").history()
        error_messages = [
            entry["payload"].get("data", {}).get("error", "")
            for entry in events
            if entry["topic"].endswith("failed")
        ]
        assert any(
            "rejected: insufficient edge" in message for message in error_messages
        ), "Audit log should record the insufficient edge rejection"

        history_snapshot = await history.snapshot()
        assert history_snapshot
        last_run = history_snapshot[-1]
        assert last_run.status == "failed"
        assert last_run.error
        assert "rejected: insufficient edge" in last_run.error

    asyncio.run(run_pipeline())

    KafkaNATSAdapter.reset()
    monkeypatch.delitem(sys.modules, "sequencer", raising=False)
