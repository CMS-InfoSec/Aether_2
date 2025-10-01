from __future__ import annotations

import asyncio
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, Optional

import pytest


if "fastapi" not in sys.modules:
    fastapi_stub = ModuleType("fastapi")

    class _FastAPI:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.routes: list[SimpleNamespace] = []

        def post(self, path: str, **_: object):
            def decorator(func):
                self.routes.append(SimpleNamespace(path=path, methods=["POST"]))
                return func

            return decorator

        def get(self, path: str, **_: object):
            def decorator(func):
                self.routes.append(SimpleNamespace(path=path, methods=["GET"]))
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

    status_module = ModuleType("fastapi.status")
    status_module.HTTP_504_GATEWAY_TIMEOUT = 504
    status_module.HTTP_400_BAD_REQUEST = 400
    fastapi_stub.status = status_module  # type: ignore[attr-defined]

    encoders_module = ModuleType("fastapi.encoders")

    def _jsonable_encoder(value: Any, *args: object, **kwargs: object) -> Any:
        del args, kwargs
        return value

    encoders_module.jsonable_encoder = _jsonable_encoder  # type: ignore[attr-defined]

    sys.modules["fastapi"] = fastapi_stub
    sys.modules["fastapi.status"] = status_module
    sys.modules["fastapi.encoders"] = encoders_module


if "pydantic" not in sys.modules:
    pydantic_stub = ModuleType("pydantic")

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

    sys.modules["pydantic"] = pydantic_stub


if "services.common.adapters" not in sys.modules:
    adapters_stub = ModuleType("services.common.adapters")

    class _KafkaNATSAdapter:
        _event_store: dict[str, list[Dict[str, Any]]] = {}

        def __init__(self, account_id: str) -> None:
            self.account_id = account_id
            self._event_store.setdefault(account_id, [])

        def publish(self, topic: str, payload: Dict[str, Any]) -> None:
            self._event_store[self.account_id].append({
                "topic": topic,
                "payload": dict(payload),
            })

        def history(self) -> list[Dict[str, Any]]:
            return list(self._event_store.get(self.account_id, []))

        @classmethod
        def reset(cls, account_id: Optional[str] = None) -> None:
            if account_id is None:
                cls._event_store.clear()
            else:
                cls._event_store.pop(account_id, None)

    adapters_stub.KafkaNATSAdapter = _KafkaNATSAdapter  # type: ignore[attr-defined]
    sys.modules["services.common.adapters"] = adapters_stub


if "override_service" not in sys.modules:
    override_stub = ModuleType("override_service")

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
        ts: Any

    async def latest_override(intent_id: str) -> Optional[OverrideRecord]:
        del intent_id
        return None

    override_stub.OverrideDecision = OverrideDecision  # type: ignore[attr-defined]
    override_stub.OverrideRecord = OverrideRecord  # type: ignore[attr-defined]
    override_stub.latest_override = latest_override  # type: ignore[attr-defined]
    sys.modules["override_service"] = override_stub


from sequencer import PipelineHistory, SequencerPipeline, Stage, StageResult


class MetricsRecorder:
    """Lightweight metrics sink to assert increments in the pipeline."""

    def __init__(self) -> None:
        self._counters: Counter[tuple[str, str, str]] = Counter()

    def increment(self, name: str, *, account_id: str, symbol: str, amount: int = 1) -> None:
        key = (name, account_id, symbol)
        self._counters[key] += amount

    def read(self, name: str, *, account_id: str, symbol: str) -> int:
        return self._counters[(name, account_id, symbol)]


class DummyFeeService:
    """Deterministic fee lookup used by the policy stage."""

    def __init__(self, metrics: MetricsRecorder) -> None:
        self._metrics = metrics
        self.calls: list[Dict[str, Any]] = []

    async def effective_bps(
        self,
        *,
        account_id: str,
        symbol: str,
        liquidity: str,
        notional: float,
    ) -> float:
        await asyncio.sleep(0)  # ensure async scheduling similar to real client
        base = 5.0 if liquidity == "maker" else 8.0
        adjustment = min(notional / 100_000.0, 1.25)
        bps = round(base - adjustment, 4)
        payload = {
            "account_id": account_id,
            "symbol": symbol,
            "liquidity": liquidity,
            "notional": notional,
            "bps": bps,
        }
        self.calls.append(payload)
        self._metrics.increment(
            "fee_lookup", account_id=account_id, symbol=symbol, amount=1
        )
        return bps


@pytest.mark.integration
def test_sequencer_pipeline_traverses_services(
    caplog: pytest.LogCaptureFixture,
    kraken_mock_server,
) -> None:
    asyncio.run(
        _run_sequencer_pipeline_integration(
            caplog=caplog,
            kraken_mock_server=kraken_mock_server,
        )
    )


async def _run_sequencer_pipeline_integration(
    *, caplog: pytest.LogCaptureFixture, kraken_mock_server
) -> None:
    metrics = MetricsRecorder()
    fee_service = DummyFeeService(metrics)
    caplog.set_level(logging.INFO)

    policy_logger = logging.getLogger("integration.policy")
    risk_logger = logging.getLogger("integration.risk")
    oms_logger = logging.getLogger("integration.oms")
    caplog.set_level(logging.INFO, logger=policy_logger.name)
    caplog.set_level(logging.INFO, logger=risk_logger.name)
    caplog.set_level(logging.INFO, logger=oms_logger.name)

    kraken_mock_server.reset()

    async def policy_handler(payload: Dict[str, Any], ctx) -> StageResult:
        intent = payload["intent"]
        symbol = str(intent["instrument"]).lower()
        account_id = ctx.account_id
        notional = float(intent["quantity"]) * float(intent["price"])
        maker_bps = await fee_service.effective_bps(
            account_id=account_id,
            symbol=symbol,
            liquidity="maker",
            notional=notional,
        )
        taker_bps = await fee_service.effective_bps(
            account_id=account_id,
            symbol=symbol,
            liquidity="taker",
            notional=notional,
        )

        metrics.increment("policy_decisions", account_id=account_id, symbol=symbol)
        policy_logger.info(
            "policy_decision_approved",
            extra={"stage": "policy", "account": account_id, "symbol": symbol},
        )

        artifact = {
            "approved": True,
            "selected_action": "maker",
            "effective_fee": {"maker": maker_bps, "taker": taker_bps},
            "notional": notional,
        }
        new_payload = dict(payload)
        new_payload["policy_decision"] = artifact
        return StageResult(payload=new_payload, artifact=artifact)

    async def policy_rollback(result: StageResult, ctx) -> None:
        policy_logger.info(
            "policy_rollback",
            extra={"stage": "policy", "account": ctx.account_id},
        )

    async def risk_handler(payload: Dict[str, Any], ctx) -> StageResult:
        intent = payload["intent"]
        symbol = str(intent["instrument"]).lower()
        decision = payload.get("policy_decision", {})
        if not decision.get("approved"):
            raise AssertionError("Policy decision should be approved in integration test")

        account_id = ctx.account_id
        projected_notional = float(decision["notional"])
        metrics.increment("risk_validations", account_id=account_id, symbol=symbol)
        risk_logger.info(
            "risk_validation_passed",
            extra={"stage": "risk", "account": account_id, "symbol": symbol},
        )

        artifact = {
            "valid": True,
            "projected_notional": projected_notional,
            "reasons": [],
        }
        new_payload = dict(payload)
        new_payload["risk_validation"] = artifact
        return StageResult(payload=new_payload, artifact=artifact)

    async def risk_rollback(result: StageResult, ctx) -> None:
        risk_logger.info(
            "risk_rollback",
            extra={"stage": "risk", "account": ctx.account_id},
        )

    async def oms_handler(payload: Dict[str, Any], ctx) -> StageResult:
        intent = payload["intent"]
        symbol = str(intent["instrument"]).lower()
        account_id = ctx.account_id

        response = await kraken_mock_server.add_order(
            pair=intent["instrument"],
            side=intent["side"].lower(),
            volume=float(intent["quantity"]),
            price=float(intent["price"]),
            account=account_id,
            ordertype="limit",
        )
        fills = response.get("fills", [])
        filled_qty = sum(float(fill["volume"]) for fill in fills)
        notional = sum(float(fill["price"]) * float(fill["volume"]) for fill in fills)
        avg_price = notional / filled_qty if filled_qty else float(intent["price"])

        metrics.increment("oms_orders", account_id=account_id, symbol=symbol)
        oms_logger.info(
            "oms_order_placed",
            extra={"stage": "oms", "account": account_id, "symbol": symbol},
        )

        artifact = {
            "accepted": True,
            "client_order_id": intent["order_id"],
            "filled_qty": filled_qty,
            "avg_price": avg_price,
            "fills": fills,
        }
        new_payload = dict(payload)
        new_payload["oms_result"] = artifact
        return StageResult(payload=new_payload, artifact=artifact)

    async def oms_rollback(result: StageResult, ctx) -> None:
        oms_logger.info(
            "oms_rollback",
            extra={"stage": "oms", "account": ctx.account_id},
        )

    history = PipelineHistory(capacity=5)
    pipeline = SequencerPipeline(
        stages=[
            Stage(name="policy", handler=policy_handler, rollback=policy_rollback, timeout=2.0),
            Stage(name="risk", handler=risk_handler, rollback=risk_rollback, timeout=2.0),
            Stage(name="oms", handler=oms_handler, rollback=oms_rollback, timeout=2.0),
        ],
        history=history,
    )

    intent = {
        "account_id": "Company",
        "order_id": "ORD-1001",
        "instrument": "BTC/USD",
        "side": "buy",
        "quantity": 0.6,
        "price": 30_250.0,
    }

    result = await pipeline.submit(intent)

    assert result.status == "success"
    assert set(result.stage_artifacts) == {"policy", "risk", "oms"}

    policy_artifact = result.stage_artifacts["policy"]
    risk_artifact = result.stage_artifacts["risk"]
    oms_artifact = result.stage_artifacts["oms"]

    assert policy_artifact["approved"] is True
    assert policy_artifact["selected_action"] == "maker"
    assert policy_artifact["effective_fee"]["maker"] < 5.0
    assert policy_artifact["effective_fee"]["taker"] < 8.0

    assert risk_artifact["valid"] is True
    assert risk_artifact["projected_notional"] == pytest.approx(0.6 * 30_250.0)

    assert oms_artifact["accepted"] is True
    assert oms_artifact["filled_qty"] == pytest.approx(0.6)
    assert oms_artifact["avg_price"] == pytest.approx(30_010.0, rel=0.01)
    assert result.fill_event["status"] == "filled"
    assert result.fill_event["filled_qty"] == pytest.approx(oms_artifact["filled_qty"])

    trades = await kraken_mock_server.get_trades(account="company", pair="BTC/USD")
    assert len(trades) == 1
    assert float(trades[0]["volume"]) == pytest.approx(0.6)

    symbol_key = "btc/usd"
    account_key = "company"
    assert metrics.read("policy_decisions", account_id=account_key, symbol=symbol_key) == 1
    assert metrics.read("risk_validations", account_id=account_key, symbol=symbol_key) == 1
    assert metrics.read("oms_orders", account_id=account_key, symbol=symbol_key) == 1
    assert metrics.read("fee_lookup", account_id=account_key, symbol=symbol_key) == 2

    messages = {(record.stage, record.message) for record in caplog.records if hasattr(record, "stage")}
    assert ("policy", "policy_decision_approved") in messages
    assert ("risk", "risk_validation_passed") in messages
    assert ("oms", "oms_order_placed") in messages

    runs = await history.snapshot()
    assert len(runs) == 1
    assert runs[0].status == "success"

    assert len(fee_service.calls) == 2
    fee_lookup = {entry["liquidity"]: entry["bps"] for entry in fee_service.calls}
    assert set(fee_lookup) == {"maker", "taker"}
    assert policy_artifact["effective_fee"]["maker"] == fee_lookup["maker"]
    assert policy_artifact["effective_fee"]["taker"] == fee_lookup["taker"]
