from typing import Any, Dict, Iterable, List, Optional

import httpx
import pytest

import sequencer
from sequencer import PipelineHistory, SequencerPipeline, Stage, StageFailedError, StageResult
from services.common.adapters import KafkaNATSAdapter


class DummyAsyncClient:
    def __init__(self, responses: Iterable[Any]) -> None:
        self._responses: List[Any] = list(responses)
        self.requests: List[Dict[str, Any]] = []

    async def __aenter__(self) -> "DummyAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> Optional[bool]:
        return False

    async def post(self, url: str, json: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> httpx.Response:
        self.requests.append({"url": url, "json": json, "headers": headers})
        if not self._responses:
            raise AssertionError("Unexpected HTTP request")
        result = self._responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _response(status_code: int, payload: Dict[str, Any]) -> httpx.Response:
    request = httpx.Request("POST", "http://risk.test/risk/validate")
    return httpx.Response(status_code, json=payload, request=request)


async def _noop_sleep(_: float) -> None:
    return None


@pytest.mark.asyncio
async def test_risk_handler_accepts_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    KafkaNATSAdapter.reset()

    validation_payload = {"valid": True, "reasons": [], "limits": {"nav_pct": 0.5}}
    client = DummyAsyncClient([_response(200, validation_payload)])
    monkeypatch.setattr(sequencer.httpx, "AsyncClient", lambda *args, **kwargs: client)
    monkeypatch.setattr(sequencer, "RISK_SERVICE_URL", "http://risk.test")
    monkeypatch.setattr(sequencer, "RISK_SERVICE_TIMEOUT", 0.5)
    monkeypatch.setattr(sequencer, "RISK_SERVICE_MAX_RETRIES", 0)
    monkeypatch.setattr(sequencer, "RISK_SERVICE_BACKOFF_SECONDS", 0.0)
    monkeypatch.setattr(sequencer.asyncio, "sleep", _noop_sleep)

    async def policy_handler(payload: Dict[str, Any], ctx) -> StageResult:
        decision = {"approved": True, "constraints": {}}
        new_payload = dict(payload)
        new_payload["policy_decision"] = decision
        return StageResult(payload=new_payload, artifact=decision)

    oms_calls: List[Dict[str, Any]] = []

    async def oms_handler(payload: Dict[str, Any], ctx) -> StageResult:
        oms_calls.append(payload)
        return StageResult(payload=payload, artifact={"accepted": True})

    history = PipelineHistory(capacity=5)
    pipeline = SequencerPipeline(
        stages=[
            Stage(name="policy", handler=policy_handler, timeout=1.0),
            Stage(name="risk", handler=sequencer.risk_handler, timeout=1.0),
            Stage(name="oms", handler=oms_handler, timeout=1.0),
        ],
        history=history,
    )

    intent = {
        "account_id": "Company",
        "order_id": "ORD-123",
        "instrument": "BTC-USD",
        "side": "buy",
        "quantity": 0.1,
        "price": 30_000.0,
    }

    result = await pipeline.submit(intent)

    artifact = result.stage_artifacts["risk"]
    assert artifact["valid"] is True
    assert artifact["reasons"] == []
    assert artifact["limits"] == validation_payload["limits"]
    assert "correlation_id" in artifact
    assert oms_calls, "OMS stage should receive payload when risk validation passes"

    assert client.requests, "Risk service should receive the validation request"
    sent_payload = client.requests[0]["json"]
    assert sent_payload["intent"]["instrument"] == "BTC-USD"
    assert sent_payload["policy_decision"]["approved"] is True
    assert client.requests[0]["headers"]["X-Account-ID"] == "company"


@pytest.mark.asyncio
async def test_risk_handler_rejects_and_emits_reasons(monkeypatch: pytest.MonkeyPatch) -> None:
    KafkaNATSAdapter.reset()

    reasons = ["Insufficient margin", "Circuit breaker"]
    validation_payload = {"valid": False, "reasons": reasons, "fee": {"maker": 5.0}}
    client = DummyAsyncClient([_response(200, validation_payload)])
    monkeypatch.setattr(sequencer.httpx, "AsyncClient", lambda *args, **kwargs: client)
    monkeypatch.setattr(sequencer, "RISK_SERVICE_URL", "http://risk.test")
    monkeypatch.setattr(sequencer, "RISK_SERVICE_TIMEOUT", 0.5)
    monkeypatch.setattr(sequencer, "RISK_SERVICE_MAX_RETRIES", 0)
    monkeypatch.setattr(sequencer, "RISK_SERVICE_BACKOFF_SECONDS", 0.0)
    monkeypatch.setattr(sequencer.asyncio, "sleep", _noop_sleep)

    async def policy_handler(payload: Dict[str, Any], ctx) -> StageResult:
        decision = {"approved": True}
        new_payload = dict(payload)
        new_payload["policy_decision"] = decision
        return StageResult(payload=new_payload, artifact=decision)

    async def oms_handler(payload: Dict[str, Any], ctx) -> StageResult:  # pragma: no cover - should not run
        raise AssertionError("OMS handler should not be invoked when risk rejects")

    history = PipelineHistory(capacity=5)
    pipeline = SequencerPipeline(
        stages=[
            Stage(name="policy", handler=policy_handler, timeout=1.0),
            Stage(name="risk", handler=sequencer.risk_handler, timeout=1.0),
            Stage(name="oms", handler=oms_handler, timeout=1.0),
        ],
        history=history,
    )

    intent = {
        "account_id": "Company",
        "order_id": "ORD-REJECT",
        "instrument": "ETH-USD",
        "side": "sell",
        "quantity": 1.2,
        "price": 2_000.0,
    }

    with pytest.raises(StageFailedError) as excinfo:
        await pipeline.submit(intent)

    assert excinfo.value.stage == "risk"
    assert all(reason in excinfo.value.message for reason in reasons)
    assert excinfo.value.details == {"reasons": reasons, "response": validation_payload}

    events = KafkaNATSAdapter(account_id="company").history()
    failure_events = [event for event in events if event["topic"] == "sequencer.risk.failed"]
    assert failure_events, "Risk failure event should be published"
    failure_payload = failure_events[0]["payload"]["data"]
    assert failure_payload["error"].startswith("Risk validation rejected intent")
    assert failure_payload["details"]["reasons"] == reasons


@pytest.mark.asyncio
async def test_risk_handler_times_out_after_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    KafkaNATSAdapter.reset()

    request = httpx.Request("POST", "http://risk.test/risk/validate")
    timeout_error = httpx.ReadTimeout("timed out", request=request)
    client = DummyAsyncClient([timeout_error, timeout_error])
    monkeypatch.setattr(sequencer.httpx, "AsyncClient", lambda *args, **kwargs: client)
    monkeypatch.setattr(sequencer, "RISK_SERVICE_URL", "http://risk.test")
    monkeypatch.setattr(sequencer, "RISK_SERVICE_TIMEOUT", 0.1)
    monkeypatch.setattr(sequencer, "RISK_SERVICE_MAX_RETRIES", 1)
    monkeypatch.setattr(sequencer, "RISK_SERVICE_BACKOFF_SECONDS", 0.0)
    monkeypatch.setattr(sequencer.asyncio, "sleep", _noop_sleep)

    async def policy_handler(payload: Dict[str, Any], ctx) -> StageResult:
        decision = {"approved": True}
        new_payload = dict(payload)
        new_payload["policy_decision"] = decision
        return StageResult(payload=new_payload, artifact=decision)

    async def oms_handler(payload: Dict[str, Any], ctx) -> StageResult:  # pragma: no cover - should not run
        raise AssertionError("OMS handler should not be invoked on timeout")

    history = PipelineHistory(capacity=5)
    pipeline = SequencerPipeline(
        stages=[
            Stage(name="policy", handler=policy_handler, timeout=1.0),
            Stage(name="risk", handler=sequencer.risk_handler, timeout=1.0),
            Stage(name="oms", handler=oms_handler, timeout=1.0),
        ],
        history=history,
    )

    intent = {
        "account_id": "Company",
        "order_id": "ORD-TIMEOUT",
        "instrument": "SOL-USD",
        "side": "buy",
        "quantity": 5.0,
        "price": 35.0,
    }

    with pytest.raises(StageFailedError) as excinfo:
        await pipeline.submit(intent)

    assert excinfo.value.stage == "risk"
    assert "after 2 attempts" in excinfo.value.message
    assert excinfo.value.details == {"attempts": 2, "error": "timed out"}

    events = KafkaNATSAdapter(account_id="company").history()
    failure_events = [event for event in events if event["topic"] == "sequencer.risk.failed"]
    assert failure_events
    failure_payload = failure_events[0]["payload"]["data"]
    assert failure_payload["details"]["attempts"] == 2
    assert not client._responses
    assert len(client.requests) == 2
