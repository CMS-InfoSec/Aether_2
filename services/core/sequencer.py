"""Trading sequencer enforcing the policy → risk → OMS execution loop."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, Iterable, Mapping, MutableMapping, Optional, Protocol, cast

from shared.pydantic_compat import BaseModel
from sqlalchemy import Column, DateTime, Float, MetaData, String, Table, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from common.schemas.contracts import FillEvent, IntentEvent, OrderEvent, RiskDecisionEvent
from common.utils import tracing
from services.common.adapters import KafkaNATSAdapter
from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

from services.common.config import TimescaleSession, get_timescale_session

LOGGER = logging.getLogger(__name__)

tracing.init_tracing("core-trading-sequencer")


def _model_dump(model: Any) -> Dict[str, Any]:
    """Return a serialisable representation compatible with Pydantic v1/v2."""

    dump = getattr(model, "model_dump", None)
    if callable(dump):
        try:
            return cast(Dict[str, Any], dump(mode="json"))
        except TypeError:
            return cast(Dict[str, Any], dump())

    legacy_dump = getattr(model, "dict", None)
    if callable(legacy_dump):
        return cast(Dict[str, Any], legacy_dump())

    raise TypeError(f"Object {model!r} does not support Pydantic-style dumping")


class RiskServiceClient(Protocol):
    """Minimal client interface for the risk service."""

    async def validate_intent(
        self, intent: IntentEvent, *, correlation_id: str
    ) -> Mapping[str, Any]:
        """Validate a policy intent and return the risk decision payload."""

    async def handle_fill(self, fill: FillEvent, *, correlation_id: str) -> None:
        """Forward an execution fill back into the risk service."""


class OMSClient(Protocol):
    """Order management client used by the trading sequencer."""

    async def place_order(
        self,
        intent: IntentEvent,
        decision: Mapping[str, Any],
        *,
        correlation_id: str,
    ) -> Mapping[str, Any]:
        """Place an order derived from *intent* and return the OMS acknowledgement."""

    def stream_fills(
        self,
        order: Mapping[str, Any],
        *,
        correlation_id: str,
    ) -> AsyncIterator[Mapping[str, Any]]:
        """Yield fill payloads for the submitted *order*."""


class FillSink(Protocol):
    """Destination that consumes fills for downstream accounting."""

    async def handle_fill(self, fill: FillEvent, *, correlation_id: str) -> None:
        """Process a fill emitted by the sequencer."""


@dataclass(slots=True)
class SequencerResult:
    """Summary of a sequencer run for a single intent."""

    correlation_id: str
    account_id: str
    symbol: str
    status: str
    started_at: datetime
    completed_at: datetime
    latency_ms: float
    intent: Dict[str, Any]
    decision: Optional[Dict[str, Any]]
    order: Optional[Dict[str, Any]]
    fills: Iterable[Dict[str, Any]]
    error: Optional[str] = None


class _LatencyRecorder:
    """Persist round-trip latency samples to the ``latency_metrics`` table."""

    def __init__(self, session: TimescaleSession) -> None:
        self._session = session
        self._engine: Engine = create_engine(session.dsn, pool_pre_ping=True, future=True)
        self._Session = sessionmaker(bind=self._engine, expire_on_commit=False, future=True)

        metadata = MetaData(schema=session.account_schema)
        self._table = Table(
            "latency_metrics",
            metadata,
            Column("service", String, nullable=False),
            Column("endpoint", String, nullable=False),
            Column("p50", Float, nullable=False),
            Column("p95", Float, nullable=False),
            Column("p99", Float, nullable=False),
            Column("ts", DateTime(timezone=True), nullable=False),
            schema=session.account_schema,
        )
        metadata.create_all(self._engine, checkfirst=True)

    async def record(self, *, endpoint: str, latency_ms: float, ts: datetime) -> None:
        """Persist a latency sample for the sequencer."""

        def _write() -> None:
            try:
                with self._Session() as db:
                    session = cast(Session, db)
                    session.execute(
                        self._table.insert(),
                        [
                            {
                                "service": "sequencer",
                                "endpoint": endpoint,
                                "p50": latency_ms,
                                "p95": latency_ms,
                                "p99": latency_ms,
                                "ts": ts,
                            }
                        ],
                    )
                    session.commit()
            except SQLAlchemyError:
                LOGGER.exception(
                    "Failed to persist sequencer latency for schema %s",
                    self._session.account_schema,
                )

        await asyncio.to_thread(_write)


class _TradeLifecycleLogger:
    """Persist lifecycle transitions to the ``trade_lifecycle_log`` table."""

    def __init__(self, session: TimescaleSession) -> None:
        self._session = session
        self._engine: Engine = create_engine(session.dsn, pool_pre_ping=True, future=True)
        self._Session = sessionmaker(bind=self._engine, expire_on_commit=False, future=True)

        metadata = MetaData(schema=session.account_schema)
        self._table = Table(
            "trade_lifecycle_log",
            metadata,
            Column("correlation_id", String, nullable=False),
            Column("account_id", String, nullable=False),
            Column("symbol", String, nullable=False),
            Column("stage", String, nullable=False),
            Column("status", String, nullable=False),
            Column("ts", DateTime(timezone=True), nullable=False),
            schema=session.account_schema,
        )
        metadata.create_all(self._engine, checkfirst=True)

    async def record(
        self,
        *,
        correlation_id: str,
        account_id: str,
        symbol: str,
        stage: str,
        status: str,
        ts: datetime,
    ) -> None:
        """Persist a lifecycle transition for an intent."""

        payload = {
            "correlation_id": correlation_id,
            "account_id": account_id,
            "symbol": symbol,
            "stage": stage,
            "status": status,
            "ts": ts,
        }

        def _write() -> None:
            try:
                with self._Session() as db:
                    session = cast(Session, db)
                    session.execute(self._table.insert(), [payload])
                    session.commit()
            except SQLAlchemyError:
                LOGGER.exception(
                    "Failed to persist lifecycle transition for schema %s",
                    self._session.account_schema,
                )

        await asyncio.to_thread(_write)


class TradingSequencer:
    """Enforce the policy → risk → OMS trading loop with strict ordering."""

    def __init__(
        self,
        *,
        risk_service: RiskServiceClient,
        oms: OMSClient,
        pnl_tracker: FillSink | None = None,
        kafka_factory: Callable[..., KafkaNATSAdapter] = KafkaNATSAdapter,
    ) -> None:
        self._risk_service = risk_service
        self._oms = oms
        self._pnl_tracker = pnl_tracker
        self._kafka_factory = kafka_factory
        self._latency_recorders: MutableMapping[str, _LatencyRecorder] = {}
        self._lifecycle_loggers: MutableMapping[str, _TradeLifecycleLogger] = {}

    async def process_intent(self, event: IntentEvent) -> SequencerResult:
        """Process a single :class:`IntentEvent` through the trading pipeline."""

        started_at = datetime.now(timezone.utc)
        start_perf = time.perf_counter()
        adapter = self._kafka_factory(account_id=event.account_id)

        decision_payload: Optional[Dict[str, Any]] = None
        order_payload: Optional[Dict[str, Any]] = None
        order_event: Optional[OrderEvent] = None
        fills: list[Dict[str, Any]] = []
        status = "pending"
        error: Optional[str] = None
        completed_at = started_at
        latency_ms = 0.0
        correlation_source: Optional[Any] = None
        if isinstance(event.intent, Mapping):
            correlation_source = (
                event.intent.get("correlation_id")
                or event.intent.get("corr_id")
                or event.intent.get("correlation")
            )
        correlation_hint = (
            str(correlation_source).strip() if correlation_source is not None else None
        )

        with tracing.correlation_scope(correlation_hint) as correlation_id:
            if isinstance(event.intent, Mapping):
                intent_payload: Dict[str, Any] = dict(event.intent)
                intent_payload["correlation_id"] = correlation_id
                event_data = _model_dump(event)
                event_data["intent"] = intent_payload
                event = IntentEvent.model_validate(event_data)

            intent_dump = _model_dump(event)
            tracing.attach_correlation(intent_dump, mutate=True)

            lifecycle_logger = await self._lifecycle_logger(event.account_id)
            await lifecycle_logger.record(
                correlation_id=correlation_id,
                account_id=event.account_id,
                symbol=event.symbol,
                stage="policy",
                status="received",
                ts=started_at,
            )

            with tracing.policy_span(intent=intent_dump.get("intent", {})):
                await self._publish(adapter, "intent", event.account_id, intent_dump)

            try:
                with tracing.risk_span(intent=intent_dump.get("intent", {})):
                    decision_payload = tracing.attach_correlation(
                        _to_dict(
                            await self._risk_service.validate_intent(
                                event, correlation_id=correlation_id
                            )
                        )
                    )
                    decision_event = RiskDecisionEvent(
                        account_id=event.account_id,
                        symbol=event.symbol,
                        decision=decision_payload,
                        ts=datetime.now(timezone.utc),
                    )
                    decision_dump = _model_dump(decision_event)
                    tracing.attach_correlation(decision_dump, mutate=True)
                    await self._publish(
                        adapter,
                        "decision",
                        event.account_id,
                        decision_dump,
                    )

                decision_status = (
                    "approved"
                    if bool(
                        decision_payload.get("approved")
                        or decision_payload.get("valid")
                        or decision_payload.get("status") == "approved"
                    )
                    else "rejected"
                )
                await lifecycle_logger.record(
                    correlation_id=correlation_id,
                    account_id=event.account_id,
                    symbol=event.symbol,
                    stage="risk",
                    status=decision_status,
                    ts=decision_event.ts,
                )

                approved = decision_status == "approved"

                if not approved:
                    status = "rejected"
                else:
                    with tracing.oms_span(intent=intent_dump.get("intent", {})):
                        order_payload = tracing.attach_correlation(
                            _to_dict(
                                await self._oms.place_order(
                                    event, decision_payload, correlation_id=correlation_id
                                )
                            )
                        )
                        order_event = OrderEvent(
                            account_id=event.account_id,
                            symbol=event.symbol,
                            order_id=str(
                                order_payload.get("order_id")
                                or order_payload.get("client_order_id")
                                or order_payload.get("id")
                                or uuid.uuid4()
                            ),
                            status=str(order_payload.get("status") or "submitted"),
                            ts=datetime.now(timezone.utc),
                        )
                        order_dump = _model_dump(order_event)
                        order_dump["details"] = dict(order_payload)
                        tracing.attach_correlation(order_dump, mutate=True)
                        await self._publish(
                            adapter,
                            "order",
                            event.account_id,
                            order_dump,
                        )

                        await lifecycle_logger.record(
                            correlation_id=correlation_id,
                            account_id=event.account_id,
                            symbol=event.symbol,
                            stage="oms",
                            status=order_event.status,
                            ts=order_event.ts,
                        )

                        async for raw_fill in self._oms.stream_fills(
                            order_payload, correlation_id=correlation_id
                        ):
                            fill_payload = _to_dict(raw_fill)
                            fill_identifier = (
                                fill_payload.get("fill_id")
                                or fill_payload.get("id")
                                or fill_payload.get("execution_id")
                            )
                            with tracing.fill_span(
                                intent=intent_dump.get("intent", {}),
                                fill_id=str(fill_identifier or uuid.uuid4()),
                            ):
                                fill_event = self._build_fill_event(event, fill_payload)
                                fill_dump = _model_dump(fill_event)
                                fill_dump["details"] = fill_payload
                                tracing.attach_correlation(fill_dump, mutate=True)
                                await self._publish(
                                    adapter,
                                    "fill",
                                    event.account_id,
                                    fill_dump,
                                )

                                await lifecycle_logger.record(
                                    correlation_id=correlation_id,
                                    account_id=event.account_id,
                                    symbol=event.symbol,
                                    stage="fill",
                                    status=str(fill_payload.get("status") or "filled"),
                                    ts=fill_event.ts,
                                )

                                await self._risk_service.handle_fill(
                                    fill_event, correlation_id=correlation_id
                                )
                                destinations = ["risk"]
                                if self._pnl_tracker is not None:
                                    await self._pnl_tracker.handle_fill(
                                        fill_event, correlation_id=correlation_id
                                    )
                                    destinations.append("pnl")
                                    await lifecycle_logger.record(
                                        correlation_id=correlation_id,
                                        account_id=event.account_id,
                                        symbol=event.symbol,
                                        stage="pnl",
                                        status="updated",
                                        ts=datetime.now(timezone.utc),
                                    )
                                else:
                                    await lifecycle_logger.record(
                                        correlation_id=correlation_id,
                                        account_id=event.account_id,
                                        symbol=event.symbol,
                                        stage="pnl",
                                        status="skipped",
                                        ts=datetime.now(timezone.utc),
                                    )

                                update_payload = {
                                    "account_id": event.account_id,
                                    "symbol": event.symbol,
                                    "order_id": order_event.order_id,
                                    "destinations": destinations,
                                    "ts": datetime.now(timezone.utc).isoformat(),
                                }
                                tracing.attach_correlation(update_payload, mutate=True)
                                await self._publish(
                                    adapter,
                                    "update",
                                    event.account_id,
                                    update_payload,
                                )
                                fills.append(dict(fill_dump))

                    status = "filled" if fills else order_event.status
            except Exception as exc:
                error = str(exc)
                status = "error"
                error_payload = {
                    "account_id": event.account_id,
                    "symbol": event.symbol,
                    "error": error,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
                tracing.attach_correlation(error_payload, mutate=True)
                await self._publish(
                    adapter,
                    "error",
                    event.account_id,
                    error_payload,
                )
                await lifecycle_logger.record(
                    correlation_id=correlation_id,
                    account_id=event.account_id,
                    symbol=event.symbol,
                    stage="error",
                    status="raised",
                    ts=datetime.now(timezone.utc),
                )
                raise
            finally:
                completed_at = datetime.now(timezone.utc)
                latency_ms = (time.perf_counter() - start_perf) * 1000.0
                recorder = await self._latency_recorder(event.account_id)
                await recorder.record(
                    endpoint=f"trade_roundtrip.{event.symbol}",
                    latency_ms=latency_ms,
                    ts=completed_at,
                )

        intent_result = _model_dump(event)
        tracing.attach_correlation(intent_result, mutate=True)

        return SequencerResult(
            correlation_id=correlation_id,
            account_id=event.account_id,
            symbol=event.symbol,
            status=status,
            started_at=started_at,
            completed_at=completed_at,
            latency_ms=latency_ms,
            intent=intent_result,
            decision=decision_payload,
            order=order_payload,
            fills=fills,
            error=error,
        )

    async def _latency_recorder(self, account_id: str) -> _LatencyRecorder:
        recorder = self._latency_recorders.get(account_id)
        if recorder is None:
            session = get_timescale_session(account_id)
            recorder = _LatencyRecorder(session)
            self._latency_recorders[account_id] = recorder
        return recorder

    async def _lifecycle_logger(self, account_id: str) -> _TradeLifecycleLogger:
        logger = self._lifecycle_loggers.get(account_id)
        if logger is None:
            session = get_timescale_session(account_id)
            logger = _TradeLifecycleLogger(session)
            self._lifecycle_loggers[account_id] = logger
        return logger

    async def _publish(
        self,
        adapter: KafkaNATSAdapter,
        stage: str,
        account_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        topic = f"sequencer.{stage}"
        enriched = dict(payload)
        enriched.setdefault("account_id", account_id)
        tracing.attach_correlation(enriched, mutate=True)
        await asyncio.to_thread(adapter.publish, topic, enriched)

    def _build_fill_event(
        self, intent: IntentEvent, payload: Mapping[str, Any]
    ) -> FillEvent:
        qty = _coerce_float(payload, ["qty", "quantity", "filled_qty", "size"], default=0.0)
        price = _coerce_float(payload, ["price", "px", "avg_price", "fill_price"], default=0.0)
        fee = _coerce_float(payload, ["fee", "fees", "commission"], default=0.0)
        liquidity = str(payload.get("liquidity") or payload.get("liquidity_flag") or "unknown")
        ts_value = payload.get("ts") or payload.get("timestamp")
        if isinstance(ts_value, str):
            try:
                ts = datetime.fromisoformat(ts_value)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except ValueError:
                ts = datetime.now(timezone.utc)
        elif isinstance(ts_value, datetime):
            ts = ts_value if ts_value.tzinfo else ts_value.replace(tzinfo=timezone.utc)
        else:
            ts = datetime.now(timezone.utc)
        return FillEvent(
            account_id=intent.account_id,
            symbol=intent.symbol,
            qty=qty,
            price=price,
            fee=max(fee, 0.0),
            liquidity=liquidity,
            ts=ts,
        )
def _coerce_float(payload: Mapping[str, Any], keys: Iterable[str], *, default: float) -> float:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return float(default)


def _to_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, BaseModel):
        return _model_dump(value)
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return {"values": list(value)}
    return {"value": value}


__all__ = ["SequencerResult", "TradingSequencer"]
