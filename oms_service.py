from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import time
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator

from common.schemas.contracts import FillEvent
from metrics import (
    increment_oms_error_count,
    record_oms_latency,
    setup_metrics,
    traced_span,
)
from services.common.adapters import KafkaNATSAdapter
from services.oms.kraken_rest import KrakenRESTClient, KrakenRESTError
from services.oms.kraken_ws import (
    KrakenWSClient,
    KrakenWSError,
    KrakenWSTimeout,
    OrderAck,
    OrderState,
)

from services.oms.rate_limit_guard import RateLimitGuard, rate_limit_guard as shared_rate_limit_guard

from shared.graceful_shutdown import flush_logging_handlers, setup_graceful_shutdown
from services.oms.oms_service import (  # type: ignore  # pragma: no cover - shared helpers
    _PrecisionValidator,
    _normalize_symbol,
    _resolve_pair_metadata,
)


logger = logging.getLogger(__name__)


SHUTDOWN_TIMEOUT = float(os.getenv("OMS_SHUTDOWN_TIMEOUT", os.getenv("SERVICE_SHUTDOWN_TIMEOUT", "75.0")))

app = FastAPI(title="Kraken OMS Service")
setup_metrics(app, service_name="oms-service")


_OMS_ACTIVITY_LOG: List[Dict[str, Any]] = []

shutdown_manager = setup_graceful_shutdown(
    app,
    service_name="oms-service",
    allowed_paths={"/", "/docs", "/openapi.json"},
    shutdown_timeout=SHUTDOWN_TIMEOUT,
    logger_instance=logger,
)


def _flush_oms_event_buffers() -> None:
    """Flush OMS event buffers (Kafka and activity logs)."""

    flush_logging_handlers("", __name__)
    kafka_counts = KafkaNATSAdapter.flush_events()
    if kafka_counts:
        logger.info("Flushed Kafka/NATS buffers", extra={"event_counts": kafka_counts})
    activity_count = len(_OMS_ACTIVITY_LOG)
    if activity_count:
        logger.info(
            "Persisting OMS activity log prior to shutdown",
            extra={"entries": activity_count},
        )
        _OMS_ACTIVITY_LOG.clear()


shutdown_manager.register_flush_callback(_flush_oms_event_buffers)


def _coerce_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, str)):
        candidate = str(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Non-finite numeric values are not supported")
        candidate = repr(value)
    else:
        candidate = str(value)
    try:
        return Decimal(candidate)
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(f"Invalid decimal value: {value}") from exc


def _coerce_optional_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    return _coerce_decimal(value)


def _format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    # Ensure we always render in fixed-point form to match Kraken expectations
    return format(normalized, "f")


def oms_log(order_id: Optional[str], account_id: str, status: str, ts: datetime | None = None) -> None:
    """Record an OMS activity entry and emit a structured log line."""

    timestamp = ts or datetime.now(timezone.utc)
    entry = {"order_id": order_id, "account_id": account_id, "status": status, "ts": timestamp}
    _OMS_ACTIVITY_LOG.append(entry)
    logger.info("oms_log", extra={"order_id": order_id, "account_id": account_id, "status": status, "ts": timestamp.isoformat()})


def _enforce_stablecoin_guard() -> None:
    monitor = get_global_monitor()
    statuses = monitor.active_depegs()
    if not statuses:
        return

    detail = format_depeg_alert(statuses, monitor.config.depeg_threshold_bps)
    logger.error(
        "Stablecoin depeg guard triggered; refusing OMS order",
        extra={
            "threshold_bps": monitor.config.depeg_threshold_bps,
            "stablecoin_status": [
                {
                    "symbol": status.symbol,
                    "deviation_bps": round(status.deviation_bps, 3),
                    "price": round(status.price, 6),
                    "feed": status.feed,
                }
                for status in statuses
            ],
        },
    )
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=detail,
    )


class PlaceOrderRequest(BaseModel):
    account_id: str = Field(..., description="Account identifier")
    client_id: str = Field(..., description="Client supplied idempotency key")
    symbol: str = Field(..., description="Trading symbol e.g. BTC/USD")
    side: str = Field(..., description="BUY or SELL")
    order_type: str = Field(..., alias="type", description="Order type (limit, market, stop)")
    qty: Decimal = Field(..., gt=Decimal("0"), description="Quantity to trade")
    limit_px: Optional[Decimal] = Field(
        None,
        gt=Decimal("0"),
        description="Limit price when applicable",
    )
    tif: Optional[str] = Field(None, description="Time in force (GTC, IOC, FOK)")
    tp: Optional[Decimal] = Field(
        None,
        gt=Decimal("0"),
        description="Take profit price",
    )
    sl: Optional[Decimal] = Field(
        None,
        gt=Decimal("0"),
        description="Stop loss price",
    )
    trailing: Optional[Decimal] = Field(
        None,
        gt=Decimal("0"),
        description="Trailing stop offset",
    )
    flags: List[str] = Field(default_factory=list, description="Additional Kraken oflags")
    post_only: bool = Field(False, description="Whether the order is post-only")
    reduce_only: bool = Field(False, description="Whether the order is reduce-only")
    expected_fee_bps: Optional[Decimal] = Field(
        None,
        ge=Decimal("0"),
        description="Fee estimate in basis points provided by the caller",
    )
    expected_slippage_bps: Optional[Decimal] = Field(
        None,
        ge=Decimal("0"),
        description="Slippage estimate in basis points provided by the caller",
    )

    @field_validator("side")
    @classmethod
    def validate_side(cls, value: str) -> str:
        normalised = value.lower()
        if normalised not in {"buy", "sell"}:
            raise ValueError("side must be BUY or SELL")
        return normalised

    @field_validator("tif")
    @classmethod
    def validate_tif(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        normalised = value.lower()
        if normalised not in {"gtc", "ioc", "fok"}:
            raise ValueError("tif must be one of GTC, IOC, FOK")
        return normalised

    @field_validator("flags", mode="before")
    @classmethod
    def normalise_flags(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            return list(value)
        if isinstance(value, (set, tuple)):
            return list(value)
        raise ValueError("flags must be a list of strings")

    @field_validator("qty", mode="before")
    @classmethod
    def parse_qty(cls, value: Any) -> Decimal:
        return _coerce_decimal(value)

    @field_validator("limit_px", "tp", "sl", "trailing", mode="before")
    @classmethod
    def parse_optional_price(cls, value: Any) -> Optional[Decimal]:
        return _coerce_optional_decimal(value)

    @field_validator("expected_fee_bps", "expected_slippage_bps", mode="before")
    @classmethod
    def parse_optional_bps(cls, value: Any) -> Optional[Decimal]:
        return _coerce_optional_decimal(value)


class CancelOrderRequest(BaseModel):
    account_id: str = Field(..., description="Account identifier")
    order_id: str = Field(..., description="Exchange order identifier")


class OrderStatus(BaseModel):
    order_id: str
    status: str
    filled_qty: Decimal | None = Field(default=None, description="Executed quantity")
    avg_price: Decimal | None = Field(default=None, description="Average execution price")
    errors: Optional[List[str]] = Field(default=None, description="Transport errors if any")


class PlaceOrderResponse(OrderStatus):
    transport: str = Field(..., description="Transport used (websocket or rest)")
    reused: bool = Field(False, description="True when the idempotency cache satisfied the request")


class CancelOrderResponse(BaseModel):
    order_id: str
    status: str
    transport: str
    reused: bool = False


class OrderStatusResponse(OrderStatus):
    updated_at: datetime


class CredentialProvider:
    """Resolves Kraken API credentials from the environment."""

    def __init__(self) -> None:
        self._cache: Dict[str, Dict[str, str]] = {}
        self._lock = asyncio.Lock()

    async def get(self, account_id: str) -> Dict[str, str]:
        async with self._lock:
            cached = self._cache.get(account_id)
            if cached is not None:
                return dict(cached)

            prefix = account_id.upper().replace("-", "_")
            credentials = {
                "api_key": os.getenv(f"KRAKEN_{prefix}_API_KEY", ""),
                "api_secret": os.getenv(f"KRAKEN_{prefix}_API_SECRET", ""),
            }
            ws_token = os.getenv(f"KRAKEN_{prefix}_WS_TOKEN")
            if ws_token:
                credentials["ws_token"] = ws_token
            self._cache[account_id] = credentials
            return dict(credentials)


class IdempotencyCache:
    """Idempotency coordinator backed by Redis with TTL-based eviction."""

    _namespace = "oms:idempotency"

    def __init__(
        self,
        *,
        ttl_seconds: float = 300.0,
        redis_factory: Callable[[str], Any] | None = None,
    ) -> None:
        self._ttl_seconds = max(ttl_seconds, 0.0)
        self._lock = asyncio.Lock()
        self._entries: Dict[str, Tuple[asyncio.Future[PlaceOrderResponse | CancelOrderResponse], float]] = {}
        self._redis_factory = redis_factory or self._default_redis_factory
        self._redis_clients: Dict[str, Any] = {}

    @staticmethod
    def _default_redis_factory(account_id: str) -> Any:
        from redis.asyncio import Redis  # type: ignore import-not-found

        from services.common.config import get_redis_client

        client = get_redis_client(account_id)
        return Redis.from_url(client.dsn, decode_responses=False)

    async def _redis(self, account_id: str) -> Any:
        client = self._redis_clients.get(account_id)
        if client is None:
            created = self._redis_factory(account_id)
            if asyncio.iscoroutine(created) or isinstance(created, asyncio.Future):
                client = await created  # type: ignore[assignment]
            else:
                client = created
            self._redis_clients[account_id] = client
        return client

    @staticmethod
    def _serialize(result: PlaceOrderResponse | CancelOrderResponse) -> bytes:
        if isinstance(result, PlaceOrderResponse):
            kind = "place"
        elif isinstance(result, CancelOrderResponse):
            kind = "cancel"
        else:  # pragma: no cover - defensive
            raise TypeError(f"Unsupported idempotency payload: {type(result)!r}")

        payload = {
            "kind": kind,
            "timestamp": time.time(),
            "data": result.model_dump(mode="json"),
        }
        return json.dumps(payload).encode("utf-8")

    @staticmethod
    def _deserialize(payload: bytes) -> Tuple[PlaceOrderResponse | CancelOrderResponse, float]:
        data = json.loads(payload.decode("utf-8"))
        timestamp = float(data.get("timestamp", 0.0))
        kind = data.get("kind")
        body = data.get("data", {})
        if kind == "place":
            return PlaceOrderResponse.model_validate(body), timestamp
        if kind == "cancel":
            return CancelOrderResponse.model_validate(body), timestamp
        raise ValueError(f"Unknown idempotency entry kind: {kind}")

    async def _purge_expired_locked(self, *, now: Optional[float] = None) -> None:
        current = now or time.monotonic()
        expired = [
            key
            for key, (future, expires_at) in self._entries.items()
            if future.done() and expires_at <= current
        ]
        for key in expired:
            self._entries.pop(key, None)

    def _key(self, account_id: str, client_id: str) -> str:
        return f"{self._namespace}:{account_id}:{client_id}"

    async def get_or_create(
        self,
        account_id: str,
        client_id: str,
        factory: Callable[[], Awaitable[PlaceOrderResponse | CancelOrderResponse]],
    ) -> Tuple[PlaceOrderResponse | CancelOrderResponse, bool]:
        key = self._key(account_id, client_id)
        await self._purge_expired()

        task, reuse = await self._peek_local(key)
        if task is not None:
            result = await task
            return result, reuse

        redis = await self._redis(account_id)
        cached = await redis.get(key)
        if cached:
            try:
                result, timestamp = self._deserialize(cached)
            except Exception:  # pragma: no cover - defensive
                await redis.delete(key)
            else:
                if self._ttl_seconds == 0.0 or time.time() - timestamp <= self._ttl_seconds:
                    loop = asyncio.get_running_loop()
                    future: asyncio.Future[PlaceOrderResponse | CancelOrderResponse] = loop.create_future()
                    future.set_result(result)
                    async with self._lock:
                        await self._purge_expired_locked()
                        self._entries[key] = (future, time.monotonic() + self._ttl_seconds)
                    return result, True
                await redis.delete(key)

        loop = asyncio.get_running_loop()
        task_future = loop.create_task(factory())
        async with self._lock:
            await self._purge_expired_locked()
            self._entries[key] = (task_future, float("inf"))

        try:
            result = await task_future
        except Exception:
            async with self._lock:
                self._entries.pop(key, None)
            raise

        return result, False

    async def store(
        self,
        account_id: str,
        client_id: str,
        result: PlaceOrderResponse | CancelOrderResponse,
    ) -> None:
        key = self._key(account_id, client_id)
        redis = await self._redis(account_id)
        payload = self._serialize(result)
        ttl = self._ttl_seconds if self._ttl_seconds > 0 else None
        await redis.set(key, payload, ex=ttl)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[PlaceOrderResponse | CancelOrderResponse] = loop.create_future()
        future.set_result(result)
        expires_at = time.monotonic() + self._ttl_seconds if self._ttl_seconds > 0 else time.monotonic()
        async with self._lock:
            await self._purge_expired_locked()
            self._entries[key] = (future, expires_at)

    async def purge(self, account_id: str, client_id: str) -> None:
        key = self._key(account_id, client_id)
        redis = await self._redis(account_id)
        await redis.delete(key)
        async with self._lock:
            self._entries.pop(key, None)

    async def _peek_local(
        self, key: str
    ) -> Tuple[
        Optional[asyncio.Future[PlaceOrderResponse | CancelOrderResponse]],
        bool,
    ]:
        async with self._lock:
            await self._purge_expired_locked()
            entry = self._entries.get(key)
            if not entry:
                return None, False
            future, expires_at = entry
            reuse = future.done() and expires_at > time.monotonic()
            return future, reuse

    async def _purge_expired(self) -> None:
        async with self._lock:
            await self._purge_expired_locked()


@dataclass
class OrderContext:
    account_id: str
    symbol: str
    side: str
    qty: Decimal
    client_id: str
    post_only: bool
    reduce_only: bool
    tif: Optional[str]
    price: Optional[Decimal] = None
    expected_fee_bps: Decimal = Decimal("0")
    expected_slippage_bps: Decimal = Decimal("0")
    last_filled: Decimal = Decimal("0")
    last_fee: Decimal = Decimal("0")


@dataclass
class _SnappedOrderFields:
    qty: Decimal
    limit_px: Optional[Decimal]
    take_profit: Optional[Decimal]
    stop_loss: Optional[Decimal]
    trailing: Optional[Decimal]


@dataclass
class OrderRecord:
    status: str
    filled_qty: Decimal | None
    avg_price: Decimal | None
    errors: Optional[List[str]]
    updated_at: datetime


class KrakenSession:
    """Manages Kraken websocket and REST transports for an account."""

    def __init__(
        self,
        account_id: str,
        credential_provider: CredentialProvider,
        *,
        rate_limit_guard: RateLimitGuard | None = None,
    ) -> None:
        self.account_id = account_id
        self._credential_provider = credential_provider
        self._rate_limit_guard: RateLimitGuard = rate_limit_guard or shared_rate_limit_guard
        self._rest_client = KrakenRESTClient(
            credential_getter=lambda: self._credential_provider.get(self.account_id)
        )
        self._ws_client = KrakenWSClient(
            credential_getter=lambda: self._credential_provider.get(self.account_id),
            stream_update_cb=self._on_state,
            rest_client=self._rest_client,
            rate_limit_guard=self._rate_limit_guard,
            account_id=self.account_id,
        )
        self._ws_task: Optional[asyncio.Task[None]] = None
        self._ready = asyncio.Event()
        self._lock = asyncio.Lock()
        self._orders: Dict[str, OrderRecord] = {}
        self._contexts: Dict[str, OrderContext] = {}
        self._client_lookup: Dict[str, str] = {}
        self._kafka = KafkaNATSAdapter(account_id=self.account_id)
        self._metadata: Optional[Dict[str, Any]] = None
        self._metadata_lock = asyncio.Lock()

    async def ensure_started(self) -> None:
        if self._ready.is_set():
            return
        async with self._lock:
            if self._ready.is_set():
                return
            await self._ws_client.ensure_connected()
            await self._ws_client.subscribe_private(["openOrders", "ownTrades"])
            if self._ws_task is None or self._ws_task.done():
                self._ws_task = asyncio.create_task(self._ws_client.stream_handler(), name=f"kraken-ws-stream-{self.account_id}")
            self._ready.set()

    async def close(self) -> None:
        if self._ws_task:
            self._ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ws_task
            self._ws_task = None
        await self._ws_client.close()
        await self._rest_client.close()
        self._ready.clear()
        self._metadata = None

    async def place_order(self, payload: Dict[str, Any], context: OrderContext) -> Tuple[OrderAck, str]:
        await self.ensure_started()
        start = time.perf_counter()
        await self._rate_limit_guard.acquire(
            self.account_id,
            "add_order",
            transport="websocket",
            urgent=False,
        )
        try:
            ack = await self._ws_client.add_order(payload)
        except (KrakenWSError, KrakenWSTimeout) as exc:
            await self._rate_limit_guard.release(
                self.account_id,
                transport="websocket",
                successful=False,
            )
            increment_oms_error_count(self.account_id, context.symbol, "websocket")
            ack = await self._place_via_rest(payload)
            transport = "rest"
        else:
            await self._rate_limit_guard.release(
                self.account_id,
                transport="websocket",
                successful=True,
            )
            transport = "websocket"
        latency_ms = (time.perf_counter() - start) * 1000.0
        record_oms_latency(self.account_id, context.symbol, transport, latency_ms)
        exchange_id = ack.exchange_order_id or context.client_id
        self._contexts[exchange_id] = context
        self._client_lookup[context.client_id] = exchange_id
        self._orders[exchange_id] = OrderRecord(
            status=ack.status or "pending",
            filled_qty=ack.filled_qty,
            avg_price=ack.avg_price,
            errors=ack.errors,
            updated_at=datetime.now(timezone.utc),
        )
        oms_log(exchange_id, self.account_id, self._orders[exchange_id].status)
        return ack, transport

    async def cancel_order(self, order_id: str, symbol: Optional[str]) -> Tuple[OrderAck, str]:
        await self.ensure_started()
        payload = {"order_id": order_id}
        start = time.perf_counter()
        await self._rate_limit_guard.acquire(
            self.account_id,
            "cancel_order",
            transport="websocket",
            urgent=True,
        )
        try:
            ack = await self._ws_client.cancel_order(payload)
        except (KrakenWSError, KrakenWSTimeout):
            await self._rate_limit_guard.release(
                self.account_id,
                transport="websocket",
                successful=False,
            )
            increment_oms_error_count(self.account_id, symbol or "unknown", "websocket")
            ack = await self._cancel_via_rest(payload)
            transport = "rest"
        else:
            await self._rate_limit_guard.release(
                self.account_id,
                transport="websocket",
                successful=True,
            )
            transport = "websocket"
        latency_ms = (time.perf_counter() - start) * 1000.0
        record_oms_latency(self.account_id, symbol or "unknown", transport, latency_ms)
        exchange_id = ack.exchange_order_id or order_id
        self._orders[exchange_id] = OrderRecord(
            status=ack.status or "canceled",
            filled_qty=ack.filled_qty,
            avg_price=ack.avg_price,
            errors=ack.errors,
            updated_at=datetime.now(timezone.utc),
        )
        oms_log(exchange_id, self.account_id, self._orders[exchange_id].status)
        return ack, transport

    def get_order(self, order_id: str) -> Optional[OrderRecord]:
        return self._orders.get(order_id)

    async def get_metadata(self) -> Optional[Dict[str, Any]]:
        if self._metadata is not None:
            return self._metadata

        async with self._metadata_lock:
            if self._metadata is not None:
                return self._metadata

            fetcher = getattr(self._rest_client, "asset_pairs", None)
            if fetcher is None:
                logger.debug(
                    "Kraken REST client missing asset_pairs() method; skipping metadata preload",
                )
                self._metadata = None
                return None

            try:
                result = await fetcher()
            except Exception as exc:  # pragma: no cover - network errors
                logger.warning(
                    "Failed to load Kraken metadata for %s: %s", self.account_id, exc
                )
                self._metadata = None
            else:
                self._metadata = result if isinstance(result, dict) else None

        return self._metadata

    def resolve_order_id(self, client_id: str) -> Optional[str]:
        return self._client_lookup.get(client_id)

    def get_context(self, order_id: str) -> Optional[OrderContext]:
        context = self._contexts.get(order_id)
        if context is not None:
            return context
        resolved = self._client_lookup.get(order_id)
        if resolved:
            return self._contexts.get(resolved)
        return None

    async def _place_via_rest(self, payload: Dict[str, Any]) -> OrderAck:
        await self._rate_limit_guard.acquire(
            self.account_id,
            "add_order",
            transport="rest",
            urgent=False,
        )
        try:
            result = await self._rest_client.add_order(payload)
        except KrakenRESTError as exc:
            await self._rate_limit_guard.release(
                self.account_id,
                transport="rest",
                successful=False,
            )
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        else:
            await self._rate_limit_guard.release(
                self.account_id,
                transport="rest",
                successful=True,
            )
            return result

    async def _cancel_via_rest(self, payload: Dict[str, Any]) -> OrderAck:
        await self._rate_limit_guard.acquire(
            self.account_id,
            "cancel_order",
            transport="rest",
            urgent=True,
        )
        try:
            result = await self._rest_client.cancel_order(payload)
        except KrakenRESTError as exc:
            await self._rate_limit_guard.release(
                self.account_id,
                transport="rest",
                successful=False,
            )
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        else:
            await self._rate_limit_guard.release(
                self.account_id,
                transport="rest",
                successful=True,
            )
            return result

    async def _on_state(self, state: OrderState) -> None:
        exchange_id = state.exchange_order_id or state.client_order_id
        if exchange_id is None:
            return
        record = self._orders.get(exchange_id)
        updated_at = datetime.now(timezone.utc)
        if record:
            record.status = state.status or record.status
            record.filled_qty = state.filled_qty if state.filled_qty is not None else record.filled_qty
            record.avg_price = state.avg_price if state.avg_price is not None else record.avg_price
            record.errors = state.errors or record.errors
            record.updated_at = updated_at
        else:
            self._orders[exchange_id] = OrderRecord(
                status=state.status or "open",
                filled_qty=state.filled_qty,
                avg_price=state.avg_price,
                errors=state.errors,
                updated_at=updated_at,
            )
        context = self._contexts.get(exchange_id)
        if context is None and state.client_order_id:
            resolved = self._client_lookup.get(state.client_order_id)
            if resolved:
                context = self._contexts.get(resolved)
                exchange_id = resolved
        if context:
            self._maybe_publish_fill(exchange_id, context, state)
        oms_log(exchange_id, self.account_id, self._orders[exchange_id].status, updated_at)

    def _maybe_publish_fill(self, exchange_id: str, context: OrderContext, state: OrderState) -> None:
        filled_decimal = _coerce_optional_decimal(state.filled_qty)
        if filled_decimal is None:
            return
        if filled_decimal <= context.last_filled:
            return
        delta = filled_decimal - context.last_filled
        context.last_filled = filled_decimal
        fee = Decimal("0")
        liquidity = "maker" if context.post_only else "taker"
        own_trades = getattr(self._ws_client, "_own_trades", {})
        trade = own_trades.get(exchange_id)
        if isinstance(trade, dict):
            fee_value = trade.get("fee") or trade.get("fee_paid")
            try:
                total_fee = _coerce_decimal(fee_value)
            except ValueError:
                total_fee = context.last_fee
            fee_delta = total_fee - context.last_fee
            fee = fee_delta if fee_delta > 0 else Decimal("0")
            if total_fee > context.last_fee:
                context.last_fee = total_fee
            liquidity_hint = trade.get("liquidity") or trade.get("type")
            if isinstance(liquidity_hint, str):
                liquidity_hint = liquidity_hint.lower()
                if liquidity_hint in {"maker", "m"}:
                    liquidity = "maker"
                elif liquidity_hint in {"taker", "t"}:
                    liquidity = "taker"
        avg_price_decimal = _coerce_optional_decimal(state.avg_price) or context.price or Decimal("0")
        notional = delta * avg_price_decimal
        if notional < 0:
            notional = -notional
        estimated_fee_bps = context.expected_fee_bps if context.expected_fee_bps > 0 else Decimal("0")
        estimated_fee_usd = (
            notional * (estimated_fee_bps / Decimal("10000"))
            if notional > 0
            else Decimal("0")
        )
        actual_fee_usd = fee if fee > 0 else Decimal("0")
        actual_fee_bps = (
            actual_fee_usd / notional * Decimal("10000")
            if notional > 0
            else Decimal("0")
        )
        discrepancy_bps = actual_fee_bps - estimated_fee_bps
        logger.info(
            "oms_fee_reconciliation",
            extra={
                "order_id": exchange_id,
                "account_id": self.account_id,
                "symbol": context.symbol,
                "liquidity": liquidity,
                "notional_usd": float(notional),
                "estimated_fee_bps": float(estimated_fee_bps),
                "actual_fee_bps": float(actual_fee_bps),
                "estimated_fee_usd": float(estimated_fee_usd),
                "actual_fee_usd": float(actual_fee_usd),
                "expected_slippage_bps": float(context.expected_slippage_bps),
                "discrepancy_bps": float(discrepancy_bps),
            },
        )
        event = FillEvent(
            account_id=self.account_id,
            symbol=context.symbol,
            qty=float(delta),
            price=float(avg_price_decimal),
            fee=float(actual_fee_usd),
            liquidity=liquidity,
            ts=datetime.now(timezone.utc),
        )
        self._kafka.publish("fill-events", event.model_dump(mode="json"))


class OMSService:
    def __init__(self) -> None:
        self._credential_provider = CredentialProvider()
        self._sessions: Dict[str, KrakenSession] = {}
        self._idempotency = IdempotencyCache()
        self._lock = asyncio.Lock()

    async def _session(self, account_id: str) -> KrakenSession:
        async with self._lock:
            session = self._sessions.get(account_id)
            if session is None:
                session = KrakenSession(account_id, self._credential_provider)
                self._sessions[account_id] = session
            return session

    async def drain(self) -> None:
        async with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            with contextlib.suppress(Exception):
                await session.close()

    async def place_order(self, request: PlaceOrderRequest) -> PlaceOrderResponse:
        _enforce_stablecoin_guard()
        session = await self._session(request.account_id)
        metadata = await session.get_metadata()
        snapped = self._snap_order_fields(request, metadata)
        payload = self._build_payload(request, snapped)
        context = OrderContext(
            account_id=request.account_id,
            symbol=request.symbol,
            side=request.side,
            qty=snapped.qty,
            client_id=request.client_id,
            post_only=request.post_only,
            reduce_only=request.reduce_only,
            tif=request.tif,
            price=snapped.limit_px,
            expected_fee_bps=request.expected_fee_bps or Decimal("0"),
            expected_slippage_bps=request.expected_slippage_bps or Decimal("0"),
        )
        async def execute() -> PlaceOrderResponse:
            with traced_span(
                "oms.place_order",
                account_id=request.account_id,
                symbol=request.symbol,
                client_id=request.client_id,
            ):
                ack, transport = await session.place_order(payload, context)
            exchange_id = ack.exchange_order_id or session.resolve_order_id(request.client_id) or request.client_id
            response = PlaceOrderResponse(
                order_id=exchange_id,
                status=ack.status or "pending",
                filled_qty=ack.filled_qty,
                avg_price=ack.avg_price,
                errors=ack.errors,
                transport=transport,
                reused=False,
            )
            return response

        try:
            result, reused = await self._idempotency.get_or_create(
                request.account_id, request.client_id, execute
            )
        except Exception:
            increment_oms_error_count(request.account_id, request.symbol, "websocket")
            raise
        if reused:
            result.reused = True
            return result
        await self._idempotency.store(request.account_id, request.client_id, result)
        return result

    async def cancel_order(self, request: CancelOrderRequest) -> CancelOrderResponse:
        session = await self._session(request.account_id)
        context = session.get_context(request.order_id)
        symbol = context.symbol if context else "unknown"

        async def execute() -> CancelOrderResponse:
            with traced_span(
                "oms.cancel_order",
                account_id=request.account_id,
                order_id=request.order_id,
                symbol=symbol,
            ):
                ack, transport = await session.cancel_order(request.order_id, symbol)
            exchange_id = ack.exchange_order_id or request.order_id
            response = CancelOrderResponse(
                order_id=exchange_id,
                status=ack.status or "canceled",
                transport=transport,
                reused=False,
            )
            return response

        scoped_client_id = f"cancel:{request.order_id}"
        result, reused = await self._idempotency.get_or_create(
            request.account_id, scoped_client_id, execute
        )
        if reused:
            result.reused = True
            return result
        await self._idempotency.store(request.account_id, scoped_client_id, result)
        return result

    async def get_status(self, account_id: str, order_id: str) -> OrderStatusResponse:
        session = await self._session(account_id)
        record = session.get_order(order_id)
        if record is None:
            client_resolved = session.resolve_order_id(order_id)
            if client_resolved:
                record = session.get_order(client_resolved)
                order_id = client_resolved
        if record is None:
            raise HTTPException(status_code=404, detail="order not found")
        return OrderStatusResponse(
            order_id=order_id,
            status=record.status,
            filled_qty=record.filled_qty,
            avg_price=record.avg_price,
            errors=record.errors,
            updated_at=record.updated_at,
        )

    def _snap_order_fields(
        self,
        request: PlaceOrderRequest,
        metadata: Optional[Dict[str, Any]],
    ) -> _SnappedOrderFields:
        snapped_qty = request.qty
        snapped_limit = request.limit_px
        snapped_tp = request.tp
        snapped_sl = request.sl
        snapped_trailing = request.trailing

        pair_meta: Optional[Dict[str, Any]] = None
        if metadata:
            pair_meta = _resolve_pair_metadata(request.symbol, metadata)
            try:
                snapped_qty, snapped_limit = _PrecisionValidator.validate(
                    request.symbol,
                    request.qty,
                    request.limit_px,
                    pair_meta or metadata,
                )
            except HTTPException:
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning(
                    "Precision validation failed for %s: %s", request.symbol, exc
                )
                snapped_qty = request.qty
                snapped_limit = request.limit_px

        price_step: Optional[Decimal] = None
        if pair_meta:
            price_step = _PrecisionValidator._step(  # type: ignore[attr-defined]
                pair_meta,
                ["price_increment", "pair_decimals", "tick_size"],
            )
        if price_step:
            if snapped_tp is not None:
                snapped_tp = _PrecisionValidator._snap(snapped_tp, price_step)
            if snapped_sl is not None:
                snapped_sl = _PrecisionValidator._snap(snapped_sl, price_step)
            if snapped_trailing is not None:
                snapped_trailing = _PrecisionValidator._snap(snapped_trailing, price_step)

        return _SnappedOrderFields(
            qty=snapped_qty,
            limit_px=snapped_limit,
            take_profit=snapped_tp,
            stop_loss=snapped_sl,
            trailing=snapped_trailing,
        )

    def _build_payload(
        self, request: PlaceOrderRequest, snapped: _SnappedOrderFields
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "clientOrderId": request.client_id,
            "pair": _normalize_symbol(request.symbol),
            "type": request.side,
            "ordertype": request.order_type.lower(),
            "volume": _format_decimal(snapped.qty),
        }
        if snapped.limit_px is not None:
            payload["price"] = _format_decimal(snapped.limit_px)

        oflags = {flag.lower() for flag in request.flags}
        if request.post_only:
            oflags.add("post")
        if request.reduce_only:
            oflags.add("reduce_only")
        if oflags:
            payload["oflags"] = ",".join(sorted(oflags))

        if request.tif:
            payload["timeInForce"] = request.tif.upper()
        if snapped.take_profit is not None:
            payload["takeProfit"] = _format_decimal(snapped.take_profit)
        if snapped.stop_loss is not None:
            payload["stopLoss"] = _format_decimal(snapped.stop_loss)
        if snapped.trailing is not None:
            payload["trailingStopOffset"] = _format_decimal(snapped.trailing)

        return payload


oms_service = OMSService()


@app.on_event("shutdown")
async def _drain_sessions() -> None:
    await oms_service.drain()


@app.post("/oms/place", response_model=PlaceOrderResponse)
async def place_order_endpoint(payload: PlaceOrderRequest) -> PlaceOrderResponse:
    try:
        return await oms_service.place_order(payload)
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive catch
        logger.exception("failed to place order")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/oms/cancel", response_model=CancelOrderResponse)
async def cancel_order_endpoint(payload: CancelOrderRequest) -> CancelOrderResponse:
    try:
        return await oms_service.cancel_order(payload)
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive catch
        logger.exception("failed to cancel order")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/oms/status", response_model=OrderStatusResponse)
async def order_status_endpoint(account_id: str = Query(...), order_id: str = Query(...)) -> OrderStatusResponse:
    return await oms_service.get_status(account_id, order_id)
