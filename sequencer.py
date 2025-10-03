"""Sequencer service orchestrating policy, risk, and OMS pipelines."""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import os
import time
import uuid
from typing import Any, Awaitable, Callable, Deque, Dict, List, Mapping, MutableMapping, Optional, Tuple

from fastapi import FastAPI, HTTPException, status
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field

import httpx
from auth.session_client import AdminSessionManager, get_default_session_manager
from common.utils import tracing
from services.common.adapters import KafkaNATSAdapter
from services.common.schemas import (
    FeeBreakdown,
    PolicyDecisionRequest,
    PolicyDecisionResponse,
)
from override_service import OverrideDecision, OverrideRecord, latest_override

from metrics import (
    increment_rejected_intents,
    increment_trades_submitted,
    observe_oms_submit_latency,
    observe_policy_inference_latency,
    observe_risk_validation_latency,
    set_pipeline_latency,
    setup_metrics,
    traced_span,
    get_request_id,
)


LOGGER = logging.getLogger("sequencer")

tracing.init_tracing("sequencer-service")


DEFAULT_POLICY_TIMEOUT = float(os.getenv("SEQUENCER_POLICY_TIMEOUT", "2.0"))
DEFAULT_RISK_TIMEOUT = float(os.getenv("SEQUENCER_RISK_TIMEOUT", "2.0"))
DEFAULT_OMS_TIMEOUT = float(os.getenv("SEQUENCER_OMS_TIMEOUT", "2.5"))
DEFAULT_PUBLISH_TIMEOUT = float(os.getenv("SEQUENCER_PUBLISH_TIMEOUT", "1.0"))

POLICY_SERVICE_URL = os.getenv("POLICY_SERVICE_URL", "http://policy-service").strip()
POLICY_DECISION_ENDPOINT = os.getenv("POLICY_DECISION_ENDPOINT", "/policy/decide").strip()

RECENT_RUN_CAPACITY = int(os.getenv("SEQUENCER_HISTORY_SIZE", "200"))
TOPIC_PREFIX = os.getenv("SEQUENCER_TOPIC_PREFIX", "sequencer")


def _join_url(base_url: str, endpoint: str) -> str:
    base = (base_url or "").rstrip("/")
    path = endpoint.strip()
    if not path.startswith("/"):
        path = f"/{path}"
    if not base:
        return path
    return f"{base}{path}"


def _optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _require_positive_float(
    intent: Mapping[str, Any],
    keys: Tuple[str, ...],
    *,
    field: str,
) -> float:
    for key in keys:
        if key not in intent:
            continue
        try:
            value = float(intent[key])
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    raise StageFailedError("policy", f"Intent is missing required {field}")


def _normalize_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side not in {"BUY", "SELL"}:
        raise StageFailedError("policy", "Intent side must be BUY or SELL")
    return side


def _resolve_fee(intent: Mapping[str, Any]) -> FeeBreakdown:
    raw_fee = intent.get("fee") or intent.get("fees")
    if isinstance(raw_fee, Mapping):
        try:
            return FeeBreakdown.model_validate(raw_fee)
        except Exception as exc:
            raise StageFailedError("policy", f"Invalid fee payload: {exc}") from exc

    currency = str(
        intent.get("fee_currency")
        or intent.get("quote_currency")
        or intent.get("settlement_currency")
        or "USD"
    ).strip()
    maker = max(_optional_float(intent.get("maker_fee")) or 0.0, 0.0)
    taker = max(_optional_float(intent.get("taker_fee")) or 0.0, 0.0)
    return FeeBreakdown(currency=currency or "USD", maker=maker, taker=taker)


class PolicyServiceClient:
    """HTTP client responsible for fetching decisions from the policy service."""

    def __init__(
        self,
        *,
        base_url: str,
        endpoint: str,
        timeout: float,
        session_manager_factory: Callable[[], AdminSessionManager] = get_default_session_manager,
    ) -> None:
        self._base_url = base_url
        self._endpoint = endpoint
        self._timeout = timeout
        self._session_manager_factory = session_manager_factory

    def _url(self) -> str:
        if not self._base_url:
            raise RuntimeError("Policy service URL is not configured")
        return _join_url(self._base_url, self._endpoint)

    async def _authorization_headers(self, account_id: str) -> Dict[str, str]:
        manager = self._session_manager_factory()
        token = await manager.token_for_account(account_id)
        if not token:
            raise RuntimeError("Session service returned empty token for policy request")
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Account-ID": account_id,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        request_id = get_request_id() or str(uuid.uuid4())
        headers.setdefault("X-Request-ID", request_id)
        return headers

    async def decide(self, *, request: PolicyDecisionRequest) -> PolicyDecisionResponse:
        url = self._url()
        headers = await self._authorization_headers(request.account_id)
        payload = request.model_dump(mode="json")
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(url, json=payload, headers=headers)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"Policy service returned {exc.response.status_code}: {exc.response.text}"
            ) from exc
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError("Policy service returned invalid JSON payload") from exc
        return PolicyDecisionResponse.model_validate(data)


policy_client = PolicyServiceClient(
    base_url=POLICY_SERVICE_URL,
    endpoint=POLICY_DECISION_ENDPOINT or "/policy/decide",
    timeout=DEFAULT_POLICY_TIMEOUT,
)


class SequencerIntentRequest(BaseModel):
    """Incoming request describing an intent payload to be sequenced."""

    intent: Dict[str, Any] = Field(..., description="Raw intent payload to process")


class SequencerResponse(BaseModel):
    """Response returned to the caller after the pipeline completes."""

    run_id: str = Field(..., description="Unique identifier for the sequencer run")
    status: str = Field(..., description="Final status of the sequencer pipeline")
    latency_ms: float = Field(..., ge=0.0, description="Total pipeline latency in milliseconds")
    stage_latencies_ms: Dict[str, float] = Field(
        default_factory=dict, description="Latency per pipeline stage in milliseconds"
    )
    stage_artifacts: Dict[str, Any] = Field(
        default_factory=dict, description="Artifacts emitted by each pipeline stage"
    )
    fill_event: Dict[str, Any] = Field(
        default_factory=dict, description="Fill event emitted to downstream consumers"
    )


class SequencerStatusResponse(BaseModel):
    """Represents a snapshot of recent pipeline runs and aggregate statistics."""

    runs: List[Dict[str, Any]]
    stats: Dict[str, Any]


class StageError(Exception):
    """Base class for stage execution errors."""

    def __init__(self, stage: str, message: str) -> None:
        super().__init__(message)
        self.stage = stage
        self.message = message


class StageTimeoutError(StageError):
    """Raised when a stage exceeds its allotted timeout."""


class StageFailedError(StageError):
    """Raised when a stage fails for reasons other than timeout."""


@dataclass
class StageResult:
    """Represents the outcome of a pipeline stage."""

    payload: Dict[str, Any]
    artifact: Dict[str, Any]


@dataclass
class PipelineContext:
    """Context shared across pipeline stages for a specific run."""

    run_id: str
    account_id: str
    intent_id: str
    publisher: "AuditPublisher"


@dataclass
class Stage:
    """Encapsulates a single stage in the sequencer pipeline."""

    name: str
    handler: Callable[[Dict[str, Any], PipelineContext], Awaitable[StageResult]]
    rollback: Optional[Callable[[StageResult, PipelineContext], Awaitable[None]]] = None
    timeout: float = 1.0

    async def execute(self, payload: Dict[str, Any], ctx: PipelineContext) -> StageResult:
        """Execute the stage handler with timeout protection."""

        task = asyncio.create_task(self.handler(payload, ctx))
        try:
            result = await asyncio.wait_for(task, timeout=self.timeout)
        except asyncio.TimeoutError as exc:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            raise StageTimeoutError(self.name, f"Stage '{self.name}' timed out after {self.timeout:.2f}s") from exc
        except Exception as exc:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            raise StageFailedError(self.name, f"Stage '{self.name}' failed: {exc}") from exc

        if not isinstance(result, StageResult):
            raise StageFailedError(self.name, "Stage handler returned an invalid result")
        return result


@dataclass
class PipelineRunSummary:
    """Captured metadata about a pipeline run for status reporting."""

    run_id: str
    account_id: str
    intent_id: str
    status: str
    started_at: datetime
    completed_at: datetime
    latency_ms: float
    stage_latencies_ms: Dict[str, float]
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "account_id": self.account_id,
            "intent_id": self.intent_id,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "latency_ms": round(self.latency_ms, 3),
            "stage_latencies_ms": {k: round(v, 3) for k, v in self.stage_latencies_ms.items()},
            "error": self.error,
        }


class PipelineHistory:
    """Thread-safe ring buffer tracking recent pipeline runs."""

    def __init__(self, capacity: int) -> None:
        self._runs: Deque[PipelineRunSummary] = deque(maxlen=capacity)
        self._lock = asyncio.Lock()

    async def record(self, summary: PipelineRunSummary) -> None:
        async with self._lock:
            self._runs.append(summary)

    async def snapshot(self) -> List[PipelineRunSummary]:
        async with self._lock:
            return list(self._runs)


class AuditPublisher:
    """Asynchronous Kafka publisher using the in-memory adapter for auditing."""

    def __init__(self, account_id: str, topic_prefix: str = TOPIC_PREFIX) -> None:
        self._account_id = account_id
        self._adapter = KafkaNATSAdapter(account_id=account_id)
        self._topic_prefix = topic_prefix.rstrip(".")

    async def publish(self, topic_suffix: str, payload: Mapping[str, Any]) -> None:
        topic = f"{self._topic_prefix}.{topic_suffix}" if topic_suffix else self._topic_prefix
        enriched = tracing.attach_correlation(payload)
        encoded = jsonable_encoder(enriched)
        await asyncio.to_thread(self._adapter.publish, topic, encoded)  # type: ignore[arg-type]

    async def publish_event(
        self,
        stage: str,
        phase: str,
        *,
        run_id: str,
        intent_id: str,
        account_id: str,
        data: Mapping[str, Any],
    ) -> None:
        payload = {
            "run_id": run_id,
            "intent_id": intent_id,
            "account_id": account_id,
            "stage": stage,
            "phase": phase,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }
        payload = tracing.attach_correlation(payload)
        await self.publish(f"{stage}.{phase}", payload)


class PipelineResult(BaseModel):
    run_id: str
    status: str
    latency_ms: float
    stage_latencies_ms: Dict[str, float]
    stage_artifacts: Dict[str, Any]
    fill_event: Dict[str, Any]


class SequencerPipeline:
    """Coordinates the policy → risk → OMS pipeline."""

    def __init__(self, stages: List[Stage], history: PipelineHistory) -> None:
        self._stages = stages
        self._history = history

    async def submit(self, intent: Dict[str, Any]) -> PipelineResult:
        run_id = str(uuid.uuid4())
        normalized_account = str(intent.get("account_id") or "unknown").strip().lower() or "unknown"
        intent_id = str(
            intent.get("order_id")
            or intent.get("intent_id")
            or intent.get("client_id")
            or uuid.uuid4()
        )
        publisher = AuditPublisher(normalized_account)
        ctx = PipelineContext(
            run_id=run_id,
            account_id=normalized_account,
            intent_id=intent_id,
            publisher=publisher,
        )

        intent_payload = jsonable_encoder(intent)
        correlation_source = (
            intent_payload.get("correlation_id")
            or intent_payload.get("corr_id")
            or intent_payload.get("correlation")
        )
        correlation_hint = (
            str(correlation_source).strip() if correlation_source is not None else None
        )

        stage_artifacts: Dict[str, Any] = {}
        stage_latencies: Dict[str, float] = {}
        executed: List[Tuple[Stage, StageResult]] = []

        started_at = datetime.now(timezone.utc)
        pipeline_start = time.perf_counter()
        status_value = "success"
        error_message: Optional[str] = None
        fill_event: Optional[Dict[str, Any]] = None
        total_latency: float = 0.0

        with tracing.correlation_scope(correlation_hint) as correlation_id:
            tracing.attach_correlation(intent_payload, mutate=True)
            payload: Dict[str, Any] = {"intent": intent_payload}
            tracing.attach_correlation(payload, mutate=True)

            await publisher.publish_event(
                "pipeline",
                "start",
                run_id=run_id,
                intent_id=intent_id,
                account_id=normalized_account,
                data={"intent": intent_payload},
            )

            try:
                with traced_span(
                    "sequencer.pipeline",
                    run_id=run_id,
                    account_id=normalized_account,
                    intent_id=intent_id,
                ):
                    for stage in self._stages:
                        await publisher.publish_event(
                            stage.name,
                            "start",
                            run_id=run_id,
                            intent_id=intent_id,
                            account_id=normalized_account,
                            data={"payload": payload},
                        )
                        stage_start = time.perf_counter()
                        with tracing.stage_span(
                            stage.name,
                            intent=payload.get("intent", {}),
                            correlation_id=correlation_id,
                            span_name=f"sequencer.{stage.name}",
                        ):
                            with traced_span(
                                f"sequencer.stage.{stage.name}",
                                stage=stage.name,
                                run_id=run_id,
                                account_id=normalized_account,
                                intent_id=intent_id,
                            ):
                                result = await stage.execute(payload, ctx)
                        stage_latency = (time.perf_counter() - stage_start) * 1000.0
                        stage_latencies[stage.name] = stage_latency
                        if stage.name == "policy":
                            observe_policy_inference_latency(stage_latency)
                        elif stage.name == "risk":
                            observe_risk_validation_latency(stage_latency)
                        elif stage.name == "oms":
                            artifact = result.artifact if isinstance(result.artifact, dict) else {}
                            transport = str(artifact.get("transport") or "sequencer")
                            observe_oms_submit_latency(stage_latency, transport=transport)
                        payload = tracing.attach_correlation(result.payload, mutate=True)
                        stage_artifact = tracing.attach_correlation(result.artifact, mutate=True)
                        stage_artifacts[stage.name] = stage_artifact

                        if stage.name == "policy":
                            decision_payload = payload.get("policy_decision", {})
                            if isinstance(decision_payload, Mapping) and not decision_payload.get(
                                "approved", True
                            ):
                                reason = str(decision_payload.get("reason") or "Policy decision rejected")
                                raise StageFailedError("policy", f"Policy decision rejected intent: {reason}")

                        executed.append((stage, result))
                        await publisher.publish_event(
                            stage.name,
                            "complete",
                            run_id=run_id,
                            intent_id=intent_id,
                            account_id=normalized_account,
                            data=stage_artifact,
                        )

                    with tracing.fill_span(
                        intent=payload.get("intent", {}),
                        correlation_id=correlation_id,
                        span_name="sequencer.fill",
                    ):
                        fill_event = tracing.attach_correlation(
                            await self._emit_fill_event(ctx, payload, stage_artifacts)
                        )
                    total_latency_snapshot = (time.perf_counter() - pipeline_start) * 1000.0
                    completion_payload = tracing.attach_correlation(
                        {"fill_event": fill_event, "latency_ms": total_latency_snapshot}
                    )
                    await publisher.publish_event(
                        "pipeline",
                        "complete",
                        run_id=run_id,
                        intent_id=intent_id,
                        account_id=normalized_account,
                        data=completion_payload,
                    )
            except StageError as exc:
                status_value = "failed"
                error_message = f"{exc.stage}: {exc.message}"
                increment_rejected_intents(exc.stage, exc.message)
                error_payload = tracing.attach_correlation({"error": exc.message})
                await publisher.publish_event(
                    exc.stage,
                    "failed",
                    run_id=run_id,
                    intent_id=intent_id,
                    account_id=normalized_account,
                    data=error_payload,
                )
                await publisher.publish_event(
                    "pipeline",
                    "failed",
                    run_id=run_id,
                    intent_id=intent_id,
                    account_id=normalized_account,
                    data=error_payload,
                )
                await self._rollback(executed, ctx)
                raise
            except Exception as exc:  # pragma: no cover - defensive guard
                status_value = "failed"
                error_message = f"unexpected: {exc}"
                error_payload = tracing.attach_correlation({"error": error_message})
                await publisher.publish_event(
                    "pipeline",
                    "failed",
                    run_id=run_id,
                    intent_id=intent_id,
                    account_id=normalized_account,
                    data=error_payload,
                )
                await self._rollback(executed, ctx)
                raise StageFailedError("pipeline", error_message) from exc
            finally:
                completed_at = datetime.now(timezone.utc)
                total_latency = (time.perf_counter() - pipeline_start) * 1000.0
                set_pipeline_latency(total_latency)
                summary = PipelineRunSummary(
                    run_id=run_id,
                    account_id=normalized_account,
                    intent_id=intent_id,
                    status=status_value,
                    started_at=started_at,
                    completed_at=completed_at,
                    latency_ms=total_latency,
                    stage_latencies_ms=stage_latencies,
                    error=error_message,
                )
                await self._history.record(summary)

        if fill_event is None:
            raise StageFailedError("pipeline", error_message or "Pipeline failed to produce fill event")

        increment_trades_submitted()

        return PipelineResult(
            run_id=run_id,
            status=status_value,
            latency_ms=total_latency,
            stage_latencies_ms=stage_latencies,
            stage_artifacts=stage_artifacts,
            fill_event=fill_event,
        )

    async def _rollback(self, executed: List[Tuple[Stage, StageResult]], ctx: PipelineContext) -> None:
        for stage, result in reversed(executed):
            if stage.rollback is None:
                continue
            try:
                await stage.rollback(result, ctx)
            except Exception:  # pragma: no cover - rollback best effort
                LOGGER.exception("Rollback for stage %s failed", stage.name)

    async def _emit_fill_event(
        self,
        ctx: PipelineContext,
        payload: MutableMapping[str, Any],
        stage_artifacts: Mapping[str, Any],
    ) -> Dict[str, Any]:
        intent = payload.get("intent", {})
        oms_result = stage_artifacts.get("oms", {})
        quantity = _safe_float(intent.get("quantity") or intent.get("qty") or 0.0)
        price = _safe_float(
            intent.get("price")
            or intent.get("limit_px")
            or (intent.get("mid_price") if isinstance(intent.get("mid_price"), (int, float)) else 0.0)
        )
        filled_qty = _safe_float(oms_result.get("filled_qty", quantity))
        avg_price = _safe_float(oms_result.get("avg_price", price))
        event = {
            "event_type": "FillEvent",
            "run_id": ctx.run_id,
            "account_id": ctx.account_id,
            "intent_id": ctx.intent_id,
            "order_id": intent.get("order_id"),
            "instrument": intent.get("instrument"),
            "status": "filled" if oms_result.get("accepted", True) else "rejected",
            "filled_qty": filled_qty,
            "avg_price": avg_price,
            "stage_artifacts": stage_artifacts,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        tracing.attach_correlation(event, mutate=True)
        await asyncio.wait_for(
            ctx.publisher.publish_event(
                "fill",
                "publish",
                run_id=ctx.run_id,
                intent_id=ctx.intent_id,
                account_id=ctx.account_id,
                data=event,
            ),
            timeout=DEFAULT_PUBLISH_TIMEOUT,
        )
        return event


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _build_policy_request(intent: Mapping[str, Any], ctx: PipelineContext) -> PolicyDecisionRequest:
    if not intent:
        raise StageFailedError("policy", "Intent payload is missing for policy evaluation")

    order_id = str(intent.get("order_id") or intent.get("intent_id") or ctx.intent_id)
    instrument = str(intent.get("instrument") or intent.get("symbol") or "").strip()
    if not instrument:
        raise StageFailedError("policy", "Intent instrument is required for policy evaluation")

    side = _normalize_side(intent.get("side") or intent.get("direction"))
    quantity = _require_positive_float(intent, ("quantity", "qty"), field="quantity")
    price = _require_positive_float(
        intent,
        ("price", "limit_px", "mid_price"),
        field="price",
    )
    fee_breakdown = _resolve_fee(intent)

    request_kwargs: Dict[str, Any] = {
        "account_id": ctx.account_id,
        "order_id": order_id,
        "instrument": instrument,
        "side": side,
        "quantity": quantity,
        "price": price,
        "fee": fee_breakdown,
    }

    optional_fields = {
        "features": intent.get("features"),
        "book_snapshot": intent.get("book_snapshot"),
        "state": intent.get("state"),
        "expected_edge_bps": _optional_float(intent.get("expected_edge_bps")),
        "slippage_bps": _optional_float(intent.get("slippage_bps")),
        "take_profit_bps": _optional_float(intent.get("take_profit_bps")),
        "stop_loss_bps": _optional_float(intent.get("stop_loss_bps")),
        "confidence": intent.get("confidence"),
    }
    for key, value in optional_fields.items():
        if value is not None:
            request_kwargs[key] = value

    return PolicyDecisionRequest(**request_kwargs)


async def policy_handler(payload: Dict[str, Any], ctx: PipelineContext) -> StageResult:
    intent = payload.get("intent", {})
    try:
        request = _build_policy_request(intent, ctx)
    except StageFailedError:
        raise
    except Exception as exc:
        raise StageFailedError("policy", f"Failed to build policy request: {exc}") from exc

    try:
        decision = await policy_client.decide(request=request)
    except httpx.TimeoutException as exc:
        raise StageFailedError("policy", "Policy service request timed out") from exc
    except Exception as exc:
        raise StageFailedError("policy", f"Policy service request failed: {exc}") from exc

    artifact = decision.model_dump(mode="json")
    new_payload = dict(payload)
    new_payload["policy_decision"] = artifact
    return StageResult(payload=new_payload, artifact=artifact)


async def policy_rollback(result: StageResult, ctx: PipelineContext) -> None:
    await ctx.publisher.publish_event(
        "policy",
        "rollback",
        run_id=ctx.run_id,
        intent_id=ctx.intent_id,
        account_id=ctx.account_id,
        data=result.artifact,
    )


async def risk_handler(payload: Dict[str, Any], ctx: PipelineContext) -> StageResult:
    decision = payload.get("policy_decision", {})
    if not decision.get("approved", False):
        reason = decision.get("reason")
        message = "Policy decision rejected the intent"
        if reason:
            message = f"{message}: {reason}"
        raise StageFailedError("risk", message)

    intent = payload.get("intent", {})
    quantity = _safe_float(intent.get("quantity") or intent.get("qty"))
    price = _safe_float(intent.get("price") or intent.get("limit_px"))
    notional = abs(quantity * price)
    assessment = {
        "valid": True,
        "reasons": [],
        "assessed_at": datetime.now(timezone.utc).isoformat(),
        "projected_notional": notional,
        "net_exposure": quantity,
    }
    new_payload = dict(payload)
    new_payload["risk_validation"] = assessment
    return StageResult(payload=new_payload, artifact=assessment)


async def risk_rollback(result: StageResult, ctx: PipelineContext) -> None:
    await ctx.publisher.publish_event(
        "risk",
        "rollback",
        run_id=ctx.run_id,
        intent_id=ctx.intent_id,
        account_id=ctx.account_id,
        data=result.artifact,
    )


async def override_handler(payload: Dict[str, Any], ctx: PipelineContext) -> StageResult:
    record: Optional[OverrideRecord] = await asyncio.to_thread(latest_override, ctx.intent_id)
    artifact: Dict[str, Any] = {"overridden": record is not None}
    new_payload = dict(payload)

    if record is not None:
        artifact.update(
            {
                "intent_id": record.intent_id,
                "account_id": record.account_id,
                "actor": record.actor,
                "decision": record.decision.value,
                "reason": record.reason,
                "ts": record.ts.isoformat(),
            }
        )
        new_payload["override_decision"] = artifact
        if record.decision == OverrideDecision.REJECT:
            raise StageFailedError("override", f"Trade rejected by human decision: {record.reason}")

    return StageResult(payload=new_payload, artifact=artifact)


async def override_rollback(result: StageResult, ctx: PipelineContext) -> None:
    await ctx.publisher.publish_event(
        "override",
        "rollback",
        run_id=ctx.run_id,
        intent_id=ctx.intent_id,
        account_id=ctx.account_id,
        data=result.artifact,
    )


async def oms_handler(payload: Dict[str, Any], ctx: PipelineContext) -> StageResult:
    risk = payload.get("risk_validation", {})
    if not risk.get("valid", False):
        raise StageFailedError("oms", "Risk validation failed")

    intent = payload.get("intent", {})
    client_order_id = intent.get("order_id") or str(uuid.uuid4())
    filled_qty = _safe_float(intent.get("quantity") or intent.get("qty") or 0.0)
    avg_price = _safe_float(intent.get("price") or intent.get("limit_px") or 0.0)
    oms_result = {
        "accepted": True,
        "routed_venue": intent.get("venue") or "default",
        "client_order_id": client_order_id,
        "filled_qty": filled_qty,
        "avg_price": avg_price,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    new_payload = dict(payload)
    new_payload["oms_result"] = oms_result
    return StageResult(payload=new_payload, artifact=oms_result)


async def oms_rollback(result: StageResult, ctx: PipelineContext) -> None:
    await ctx.publisher.publish_event(
        "oms",
        "rollback",
        run_id=ctx.run_id,
        intent_id=ctx.intent_id,
        account_id=ctx.account_id,
        data=result.artifact,
    )


history = PipelineHistory(capacity=RECENT_RUN_CAPACITY)

pipeline = SequencerPipeline(
    stages=[
        Stage(
            name="policy",
            handler=policy_handler,
            rollback=policy_rollback,
            timeout=DEFAULT_POLICY_TIMEOUT,
        ),
        Stage(
            name="risk",
            handler=risk_handler,
            rollback=risk_rollback,
            timeout=DEFAULT_RISK_TIMEOUT,
        ),
        Stage(
            name="override",
            handler=override_handler,
            rollback=override_rollback,
            timeout=DEFAULT_POLICY_TIMEOUT,
        ),
        Stage(
            name="oms",
            handler=oms_handler,
            rollback=oms_rollback,
            timeout=DEFAULT_OMS_TIMEOUT,
        ),
    ],
    history=history,
)

app = FastAPI(title="Sequencer Service", version="1.0.0")
setup_metrics(app, service_name="sequencer")


@app.post("/sequencer/submit_intent", response_model=SequencerResponse)
async def submit_intent(request: SequencerIntentRequest) -> SequencerResponse:
    try:
        result = await pipeline.submit(request.intent)
    except StageTimeoutError as exc:
        raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail=exc.message) from exc
    except StageFailedError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=exc.message) from exc

    return SequencerResponse(**result.model_dump())


@app.get("/sequencer/status", response_model=SequencerStatusResponse)
async def sequencer_status() -> SequencerStatusResponse:
    runs = await history.snapshot()
    serialized_runs = [run.to_dict() for run in runs]
    latency_values = [run.latency_ms for run in runs if run.latency_ms >= 0]

    stats: Dict[str, Any] = {
        "total_runs": len(serialized_runs),
        "success": sum(1 for run in runs if run.status == "success"),
        "failed": sum(1 for run in runs if run.status != "success"),
        "average_latency_ms": round(_average(latency_values), 3) if latency_values else 0.0,
        "p95_latency_ms": round(_percentile(latency_values, 95), 3) if latency_values else 0.0,
        "max_latency_ms": round(max(latency_values), 3) if latency_values else 0.0,
    }

    stage_keys = {
        stage
        for run in runs
        for stage in run.stage_latencies_ms.keys()
    }
    stage_stats: Dict[str, Dict[str, float]] = {}
    for stage in stage_keys:
        values = [run.stage_latencies_ms.get(stage, 0.0) for run in runs if stage in run.stage_latencies_ms]
        if not values:
            continue
        stage_stats[stage] = {
            "average_latency_ms": round(_average(values), 3),
            "p95_latency_ms": round(_percentile(values, 95), 3),
            "max_latency_ms": round(max(values), 3),
        }
    if stage_stats:
        stats["stages"] = stage_stats

    return SequencerStatusResponse(runs=serialized_runs, stats=stats)


def _average(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _percentile(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    k = max(0, min(len(sorted_values) - 1, int(round((percentile / 100.0) * (len(sorted_values) - 1)))))
    return sorted_values[k]


__all__ = [
    "app",
    "pipeline",
    "SequencerIntentRequest",
    "SequencerResponse",
]
