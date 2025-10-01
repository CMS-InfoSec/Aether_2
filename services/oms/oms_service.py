from __future__ import annotations


import asyncio
import contextlib
import json
import logging
import os

import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_EVEN, ROUND_UP

from pathlib import Path
from typing import Any, Awaitable, Dict, Iterable, List, Optional, Set, Tuple

from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator

from services.oms.impact_store import ImpactAnalyticsStore, impact_store
from services.oms.kraken_rest import KrakenRESTClient, KrakenRESTError
from services.oms.kraken_ws import (
    KrakenWSError,
    KrakenWSTimeout,
    KrakenWSClient,
    OrderAck,
    OrderState,
)

from services.oms.routing import LatencyRouter
from services.oms.warm_start import WarmStartCoordinator


import websockets

from metrics import (
    increment_oms_child_orders_total,
    increment_oms_error_count,
    record_oms_latency,
    setup_metrics,
)



logger = logging.getLogger(__name__)


app = FastAPI(title="Kraken OMS Async Service")
setup_metrics(app)


class OMSPlaceRequest(BaseModel):
    account_id: str = Field(..., description="Account identifier for credential lookup")
    client_id: str = Field(..., description="Client supplied idempotency key")
    symbol: str = Field(..., description="Trading symbol (e.g. BTC/USD)")
    side: str = Field(..., description="BUY or SELL")
    order_type: str = Field(..., alias="type", description="Order type (limit, market, stop-limit, etc)")
    qty: Decimal = Field(..., gt=0, description="Base quantity to trade")
    limit_px: Optional[Decimal] = Field(
        default=None,
        gt=Decimal("0"),
        description="Limit price when applicable",
    )
    tif: Optional[str] = Field(
        default=None,
        description="Explicit time-in-force (GTC, IOC, FOK)",
    )
    flags: List[str] = Field(default_factory=list, description="Additional Kraken oflags")
    post_only: bool = Field(False, description="Convenience flag for post-only")
    reduce_only: bool = Field(False, description="Convenience flag for reduce-only")
    take_profit: Optional[Decimal] = Field(
        default=None,
        gt=Decimal("0"),
        description="Exchange-native take-profit trigger",
    )
    stop_loss: Optional[Decimal] = Field(
        default=None,
        gt=Decimal("0"),
        description="Exchange-native stop-loss trigger",
    )
    trailing_offset: Optional[Decimal] = Field(
        default=None,
        gt=Decimal("0"),
        description="Trailing stop offset",
    )

    pre_trade_mid_px: Optional[Decimal] = Field(
        default=None,
        gt=Decimal("0"),
        description="Observed mid price immediately before order placement",

    )

    @field_validator("side")
    @classmethod
    def _validate_side(cls, value: str) -> str:
        normalized = value.lower()
        if normalized not in {"buy", "sell"}:
            raise ValueError("side must be BUY or SELL")
        return normalized

    @field_validator("tif")
    @classmethod
    def _validate_tif(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        normalized = value.lower()
        if normalized not in {"gtc", "ioc", "fok"}:
            raise ValueError("tif must be one of GTC, IOC, FOK")
        return normalized

    @field_validator("flags", mode="before")
    @classmethod
    def _normalize_flags(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, Iterable):
            return list(value)
        raise ValueError("flags must be a list or string")


class OMSCancelRequest(BaseModel):
    account_id: str = Field(..., description="Account identifier")
    client_id: str = Field(..., description="Idempotent client identifier")
    exchange_order_id: Optional[str] = Field(
        default=None,
        description="Exchange provided txid (preferred). When omitted the last placed order for the client_id is cancelled.",
    )


class OMSOrderStatusResponse(BaseModel):
    exchange_order_id: str
    status: str
    filled_qty: Decimal = Field(default=Decimal("0"))
    avg_price: Decimal = Field(default=Decimal("0"))
    errors: Optional[List[str]] = None


class OMSPlaceResponse(OMSOrderStatusResponse):
    transport: str = Field(default="websocket")
    reused: bool = Field(
        default=False, description="True when the idempotency cache satisfied the request"
    )
    shadow: bool = Field(default=False, description="True when the order executed in shadow mode")


class ImpactCurvePoint(BaseModel):
    size: float
    impact_bps: float


class ImpactCurveResponse(BaseModel):
    symbol: str
    points: List[ImpactCurvePoint]
    as_of: datetime


class _IdempotencyStore:
    """Cooperative idempotency cache used per-account."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._entries: Dict[str, asyncio.Future[OMSOrderStatusResponse]] = {}

    async def get_or_create(
        self, key: str, factory: Awaitable[OMSOrderStatusResponse]
    ) -> Tuple[OMSOrderStatusResponse, bool]:
        async with self._lock:
            future = self._entries.get(key)
            if future is None:
                loop = asyncio.get_event_loop()
                future = loop.create_future()
                self._entries[key] = future
                create_future = True
            else:
                create_future = False

        if create_future:
            try:
                result = await factory
            except Exception as exc:  # pragma: no cover - propagate to awaiting callers
                future.set_exception(exc)
                raise
            else:
                future.set_result(result)
                return result, False

        return await future, True


@dataclass
class OrderRecord:
    client_id: str
    result: OMSOrderStatusResponse
    transport: str

    children: List["ChildOrderRecord"] | None = None
    symbol: Optional[str] = None
    side: Optional[str] = None
    pre_trade_mid: Optional[Decimal] = None
    recorded_qty: Decimal = Decimal("0")
    requested_qty: Optional[Decimal] = None


@dataclass
class ChildOrderRecord:
    client_id: str
    exchange_order_id: str
    transport: str
    quantity: Decimal = Decimal("0")



class _PrecisionValidator:
    """Validates order precision using metadata from Kraken asset pairs."""

    @staticmethod
    def _snap(value: Decimal, step: Decimal | None) -> Decimal:
        if step is None or step <= 0:
            return value
        quant = step if isinstance(step, Decimal) else Decimal(str(step))
        operand = value if isinstance(value, Decimal) else Decimal(str(value))
        # quantize using bankers rounding
        snapped = (operand / quant).to_integral_value(rounding=ROUND_HALF_EVEN) * quant
        return snapped

    @classmethod
    def validate(
        cls,
        symbol: str,
        qty: Decimal,
        price: Optional[Decimal],
        metadata: Dict[str, Any] | None,
    ) -> Tuple[Decimal, Optional[Decimal]]:
        if not metadata:
            return qty, price

        pair_meta = metadata.get(symbol) if isinstance(metadata, dict) else None
        if not isinstance(pair_meta, dict):
            return qty, price

        qty_step = cls._step(pair_meta, ["lot_step", "lot_decimals", "step_size"])
        px_step = cls._step(pair_meta, ["price_increment", "pair_decimals", "tick_size"])

        snapped_qty = cls._snap(qty, qty_step) if qty_step else qty
        snapped_px = cls._snap(price, px_step) if price is not None else None

        min_qty = cls._maybe_decimal(pair_meta.get("ordermin"), pair_meta.get("min_qty"))
        if min_qty is not None and snapped_qty < min_qty:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Quantity {snapped_qty} below exchange minimum {min_qty}",
            )

        min_price = cls._maybe_decimal(pair_meta.get("min_price"))
        if min_price is not None and snapped_px is not None and snapped_px < min_price:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Price {snapped_px} below exchange minimum {min_price}",
            )

        return snapped_qty, snapped_px

    @staticmethod
    def _maybe_decimal(*values: Any) -> Optional[Decimal]:
        for value in values:
            if value is None:
                continue
            if isinstance(value, Decimal):
                return value
            try:
                return Decimal(str(value))
            except Exception:  # pragma: no cover - defensive
                continue
        return None

    @classmethod
    def _step(cls, metadata: Dict[str, Any], keys: List[str]) -> Optional[Decimal]:
        for key in keys:
            if key not in metadata:
                continue
            value = metadata[key]
            if value is None:
                continue
            if key.endswith("decimals"):
                try:
                    decimals = int(value)
                except (TypeError, ValueError):
                    continue
                return Decimal("1") / (Decimal("10") ** decimals)
            try:
                return Decimal(str(value))
            except Exception:  # pragma: no cover - defensive
                continue
        return None


def _normalize_symbol(symbol: str) -> str:
    return symbol.replace("-", "/").replace("_", "/").upper()


def _resolve_pair_metadata(symbol: str, metadata: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if not metadata:
        return None

    normalized = _normalize_symbol(symbol)
    candidates = [symbol, normalized, normalized.replace("/", ""), symbol.replace("/", "")]
    for candidate in candidates:
        value = metadata.get(candidate)
        if isinstance(value, dict):
            return value

    for value in metadata.values():
        if isinstance(value, dict):
            wsname = str(value.get("wsname") or "")
            altname = str(value.get("altname") or "")
            if wsname == normalized or altname == normalized.replace("/", ""):
                return value
    return None


def _resolve_book_symbol(symbol: str, metadata: Dict[str, Any] | None) -> str:
    pair_meta = _resolve_pair_metadata(symbol, metadata)
    if pair_meta:
        for key in ("wsname", "altname"):
            value = pair_meta.get(key)
            if value:
                return str(value)
    return _normalize_symbol(symbol)


class _PublicOrderBookState:
    def __init__(self, symbol: str, depth: int = 10) -> None:
        self.symbol = symbol
        self._depth = depth
        self._bids: Dict[Decimal, Decimal] = {}
        self._asks: Dict[Decimal, Decimal] = {}
        self._ordered_bids: List[Tuple[Decimal, Decimal]] = []
        self._ordered_asks: List[Tuple[Decimal, Decimal]] = []
        self._lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._last_ts: float | None = None

    async def apply(self, payload: Dict[str, Any]) -> None:
        bids_snapshot = payload.get("bs") or payload.get("bids")
        asks_snapshot = payload.get("as") or payload.get("asks")
        bids_update = payload.get("b") or []
        asks_update = payload.get("a") or []

        async with self._lock:
            if bids_snapshot or asks_snapshot:
                if isinstance(bids_snapshot, list):
                    self._update_side(self._bids, bids_snapshot, is_bid=True, replace=True)
                if isinstance(asks_snapshot, list):
                    self._update_side(self._asks, asks_snapshot, is_bid=False, replace=True)
            if bids_update:
                self._update_side(self._bids, bids_update, is_bid=True)
            if asks_update:
                self._update_side(self._asks, asks_update, is_bid=False)
            self._ordered_bids = self._sorted_levels(self._bids, is_bid=True)
            self._ordered_asks = self._sorted_levels(self._asks, is_bid=False)
            self._last_ts = time.time()
            if self._ordered_bids or self._ordered_asks:
                self._ready.set()

    def _update_side(
        self,
        book: Dict[Decimal, Decimal],
        updates: Iterable[Iterable[Any]],
        *,
        is_bid: bool,
        replace: bool = False,
    ) -> None:
        if replace:
            book.clear()
        for level in updates:
            try:
                price_value = level[0]
                size_value = level[1]
            except IndexError:
                continue
            try:
                price = Decimal(str(price_value))
                size = Decimal(str(size_value))
            except Exception:
                continue
            if size <= 0:
                book.pop(price, None)
            else:
                book[price] = size

    def _sorted_levels(
        self, book: Dict[Decimal, Decimal], *, is_bid: bool
    ) -> List[Tuple[Decimal, Decimal]]:
        ordered = sorted(book.items(), key=lambda item: item[0], reverse=is_bid)
        return ordered[: self._depth]

    async def depth(self, side: str, levels: int = 10) -> Optional[Decimal]:
        if not self._ready.is_set():
            return None
        async with self._lock:
            if side.lower() == "buy":
                book = self._ordered_asks
            else:
                book = self._ordered_bids
            relevant = book[:levels]
            depth = sum(size for _, size in relevant)
        return depth if depth > 0 else None

    async def last_update(self) -> Optional[float]:
        async with self._lock:
            return self._last_ts


class KrakenOrderBookStore:
    def __init__(self, depth: int = 10) -> None:
        self._depth = depth
        self._books: Dict[str, _PublicOrderBookState] = {}
        self._tasks: Dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()
        self._url = os.environ.get("KRAKEN_PUBLIC_WS_URL", "wss://ws.kraken.com")

    async def ensure_book(self, symbol: str) -> _PublicOrderBookState:
        normalized = _normalize_symbol(symbol)
        async with self._lock:
            book = self._books.get(normalized)
            if book is None:
                book = _PublicOrderBookState(normalized, depth=self._depth)
                self._books[normalized] = book
                task = asyncio.create_task(
                    self._run_stream(book), name=f"kraken-public-{normalized}"
                )
                self._tasks[normalized] = task
        return book

    async def _run_stream(self, book: _PublicOrderBookState) -> None:
        subscription = {
            "event": "subscribe",
            "pair": [book.symbol],
            "subscription": {"name": "book", "depth": max(self._depth, 10)},
        }
        while True:
            try:
                async with websockets.connect(self._url, ping_interval=None) as ws:
                    await ws.send(json.dumps(subscription))
                    while True:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode()
                        try:
                            message = json.loads(raw)
                        except json.JSONDecodeError:
                            logger.debug("Invalid public WS payload: %s", raw)
                            continue
                        if isinstance(message, dict):
                            event = message.get("event")
                            if event == "subscriptionStatus" and message.get("status") != "subscribed":
                                logger.warning(
                                    "Order book subscription failed for %s: %s",
                                    book.symbol,
                                    message,
                                )
                            continue
                        if isinstance(message, list) and len(message) >= 2:
                            data = message[1]
                            if isinstance(data, dict):
                                await book.apply(data)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Public order book stream error for %s: %s", book.symbol, exc
                )
                await asyncio.sleep(1.0)

    async def stop(self) -> None:
        async with self._lock:
            tasks = list(self._tasks.values())
            self._tasks.clear()
        for task in tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


order_book_store = KrakenOrderBookStore()

AGGREGATE_PRICE_PRECISION = Decimal("0.00000001")


def _split_quantities(
    total: Decimal,
    max_child: Decimal,
    qty_step: Optional[Decimal],
    min_qty: Optional[Decimal],
) -> List[Decimal]:
    if max_child <= 0 or total <= 0:
        return [total]
    if total <= max_child:
        return [total]

    quantities: List[Decimal] = []

    if qty_step and qty_step > 0:
        step = qty_step
        total_units = int((total / step).to_integral_value(rounding=ROUND_HALF_EVEN))
        max_units = int((max_child / step).to_integral_value(rounding=ROUND_FLOOR))
        if max_units <= 0:
            max_units = 1
        if min_qty and min_qty > 0:
            min_units = int((min_qty / step).to_integral_value(rounding=ROUND_UP))
            if min_units <= 0:
                min_units = 1
            max_units = max(max_units, min_units)
        else:
            min_units = 1

        units_remaining = total_units
        while units_remaining > 0:
            units = min(units_remaining, max_units)
            if units < min_units and quantities:
                quantities[-1] += Decimal(units_remaining) * step
                units_remaining = 0
                break
            if units < min_units:
                units = min_units
            quantity = Decimal(units) * step
            if quantity > total:
                quantity = total
            quantities.append(quantity)
            units_remaining -= units

        allocated = sum(quantities)
        if allocated < total and quantities:
            quantities[-1] += total - allocated
        elif allocated > total and quantities:
            quantities[-1] -= allocated - total
        return [qty for qty in quantities if qty > 0]

    remaining = total
    while remaining > 0:
        child = min(remaining, max_child)
        if min_qty and min_qty > 0 and child < min_qty:
            if quantities:
                quantities[-1] += remaining
                remaining = Decimal("0")
                break
            child = min_qty
        quantities.append(child)
        remaining -= child

    if remaining > 0 and quantities:
        quantities[-1] += remaining

    return [qty for qty in quantities if qty > 0]


class CredentialWatcher:
    """Watches Kubernetes mounted secrets for key rotation."""

    _instances: Dict[str, "CredentialWatcher"] = {}
    _base_path = Path(os.environ.get("KRAKEN_SECRETS_BASE", "/var/run/secrets/kraken"))

    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self._secret_path = self._resolve_secret_path(account_id)
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition()
        self._credentials: Dict[str, Any] = {}
        self._metadata: Dict[str, Any] | None = None
        self._version = 0
        self._last_mtime: float | None = None
        self._task: Optional[asyncio.Task[None]] = None
        self._poll_interval = float(os.environ.get("KRAKEN_SECRET_POLL", "5"))

    @classmethod
    def instance(cls, account_id: str) -> "CredentialWatcher":
        if account_id not in cls._instances:
            cls._instances[account_id] = cls(account_id)
        return cls._instances[account_id]

    @classmethod
    def _resolve_secret_path(cls, account_id: str) -> Path:
        directory = cls._base_path / account_id
        file_path = directory / "credentials.json"
        if file_path.exists():
            return file_path
        # fallback to single file per account
        fallback = cls._base_path / f"{account_id}.json"
        return fallback

    async def start(self) -> None:
        if self._task is None:
            await self._load()
            self._task = asyncio.create_task(self._watch(), name=f"{self.account_id}-credential-watch")

    async def _watch(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._poll_interval)
                await self._maybe_reload()
            except asyncio.CancelledError:  # pragma: no cover - cancellation path
                raise
            except Exception as exc:  # pragma: no cover - log and continue
                logger.exception("Credential watcher error for %s: %s", self.account_id, exc)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    async def _maybe_reload(self) -> None:
        try:
            mtime = self._secret_path.stat().st_mtime
        except FileNotFoundError:
            logger.warning("Credentials not found for account %s at %s", self.account_id, self._secret_path)
            return

        if self._last_mtime is None or mtime > self._last_mtime:
            await self._load()
            async with self._condition:
                self._condition.notify_all()

    async def _load(self) -> None:
        try:
            raw = self._secret_path.read_text()
        except FileNotFoundError:
            logger.warning("Credential file missing for account %s", self.account_id)
            return

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error("Invalid credential payload for %s: %s", self.account_id, exc)
            return
        credentials = {
            "api_key": data.get("api_key") or data.get("key"),
            "api_secret": data.get("api_secret") or data.get("secret"),
        }
        metadata = data.get("asset_pairs") or data.get("metadata")

        async with self._lock:
            self._credentials = credentials
            self._metadata = metadata
            self._version += 1
            self._last_mtime = self._secret_path.stat().st_mtime
        logger.info("Loaded credentials for account %s (version=%s)", self.account_id, self._version)

    async def get_credentials(self) -> Dict[str, Any]:
        async with self._lock:
            return dict(self._credentials)

    async def get_metadata(self) -> Dict[str, Any] | None:
        async with self._lock:
            return self._metadata.copy() if isinstance(self._metadata, dict) else None


class AccountContext:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.credentials = CredentialWatcher.instance(account_id)
        self.ws_client: Optional[KrakenWSClient] = None
        self.rest_client: Optional[KrakenRESTClient] = None
        self.idempotency = _IdempotencyStore()
        self._orders: Dict[str, OrderRecord] = {}
        self._orders_lock = asyncio.Lock()
        self._startup_lock = asyncio.Lock()
        self._stream_task: Optional[asyncio.Task[None]] = None
        self._child_parent: Dict[str, str] = {}
        self._child_results: Dict[str, OMSOrderStatusResponse] = {}
        self._positions: Dict[str, Dict[str, Decimal]] = {}
        self._positions_lock = asyncio.Lock()
        self.routing = LatencyRouter(account_id)

        self._impact_store: ImpactAnalyticsStore = impact_store
        self._reconcile_task: Optional[asyncio.Task[None]] = None
        self._reconcile_interval = max(float(os.environ.get("OMS_RECONCILE_INTERVAL", "30")), 0.0)
        self._reconcile_lock = asyncio.Lock()


    async def start(self) -> None:
        async with self._startup_lock:
            await self.credentials.start()
            if self.ws_client is None:
                self.ws_client = KrakenWSClient(
                    credential_getter=self.credentials.get_credentials,
                    stream_update_cb=self._apply_stream_state,
                )
                self._stream_task = asyncio.create_task(self.ws_client.stream_handler())
            if self.rest_client is None:
                self.rest_client = KrakenRESTClient(credential_getter=self.credentials.get_credentials)

            await self.ws_client.ensure_connected()
            await self.ws_client.subscribe_private(["openOrders", "ownTrades"])
            await self.routing.start(self.ws_client, self.rest_client)
            if (
                self._reconcile_interval > 0
                and (self._reconcile_task is None or self._reconcile_task.done())
            ):
                self._reconcile_task = asyncio.create_task(
                    self._reconcile_loop(), name=f"{self.account_id}-reconcile"
                )

    async def close(self) -> None:
        await self.routing.stop()
        if self.ws_client is not None:
            await self.ws_client.close()
        if self._stream_task is not None:
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stream_task
            self._stream_task = None
        if self.rest_client is not None:
            await self.rest_client.close()
        self._child_parent.clear()
        self._child_results.clear()
        if self._reconcile_task is not None:
            self._reconcile_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reconcile_task
            self._reconcile_task = None

    async def _reconcile_loop(self) -> None:
        interval = max(self._reconcile_interval, 1.0)
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self._perform_reconciliation()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "Periodic reconciliation failed for account %s: %s",
                        self.account_id,
                        exc,
                    )
        finally:
            logger.debug("Reconciliation loop stopped for account %s", self.account_id)

    async def _perform_reconciliation(self) -> None:
        async with self._reconcile_lock:
            orders = await self.resync_from_exchange(replace=False)
            fills = await self.resync_trades()
        if orders or fills:
            logger.info(
                "Reconciled Kraken state for account %s (orders=%s, fills=%s)",
                self.account_id,
                orders,
                fills,
            )

    async def _apply_stream_state(self, state: OrderState) -> None:
        if not state.client_order_id:
            return
        key = state.client_order_id
        parent_key = self._child_parent.get(key, key)
        result = OMSOrderStatusResponse(
            exchange_order_id=state.exchange_order_id or state.client_order_id,
            status=state.status,
            filled_qty=Decimal(str(state.filled_qty or 0)),
            avg_price=Decimal(str(state.avg_price or 0)),
            errors=state.errors or None,
        )
        async with self._orders_lock:

            existing = self._orders.get(parent_key)
            children = list(existing.children) if existing and existing.children else None
            self._child_results[key] = result
            if children:
                for idx, child in enumerate(children):
                    if child.client_id == key:
                        children[idx] = ChildOrderRecord(
                            client_id=child.client_id,
                            exchange_order_id=result.exchange_order_id,
                            transport=state.transport,
                            quantity=child.quantity,
                        )
                        break
            transports: Set[str] = set()
            if children:
                transports = {child.transport for child in children if child.transport}
            transport = (
                self._aggregate_transport(transports)
                if transports
                else (existing.transport if existing else state.transport)
            )

            aggregate_result: OMSOrderStatusResponse
            if children:
                child_results: List[OMSOrderStatusResponse] = []
                for child in children:
                    child_result = self._child_results.get(child.client_id)
                    if child_result is None:
                        child_results = []
                        break
                    child_results.append(child_result)
                if child_results and len(child_results) == len(children):
                    aggregate_result = self._aggregate_child_results(
                        parent_key, child_results, children
                    )
                else:
                    aggregate_result = existing.result if existing else result
            else:
                aggregate_result = result

            if parent_key != key:
                self._orders[key] = OrderRecord(
                    client_id=key,
                    result=result,
                    transport=state.transport,
                    children=None,
                    requested_qty=existing.requested_qty if existing else None,
                )
            requested_qty = existing.requested_qty if existing else None
            if requested_qty is None and children:
                requested_qty = sum((child.quantity for child in children), Decimal("0"))
            self._orders[parent_key] = OrderRecord(
                client_id=parent_key,
                result=aggregate_result,
                transport=transport,
                children=children,
                requested_qty=requested_qty,
            )


    async def resync_from_exchange(self, *, replace: bool = True) -> int:
        await self.start()

        if self.ws_client is None:
            return 0

        snapshot: Iterable[Dict[str, Any]] = []

        if self.ws_client is not None:
            try:
                snapshot = await self.ws_client.fetch_open_orders_snapshot()
            except (KrakenWSError, KrakenWSTimeout) as exc:
                logger.warning(
                    "Warm start failed to fetch open orders via websocket for account %s: %s",
                    self.account_id,
                    exc,
                )

        if not snapshot and self.rest_client is not None:
            try:
                payload = await self.rest_client.open_orders()
                snapshot = self._parse_rest_open_orders(payload)
            except KrakenRESTError as exc:
                logger.warning(
                    "Warm start failed to fetch open orders via REST for account %s: %s",
                    self.account_id,
                    exc,
                )
                snapshot = []

        return await self._apply_open_order_snapshot(snapshot, replace=replace)

    async def resync_positions(self) -> int:
        await self.start()

        if self.rest_client is None:
            return 0

        try:
            payload = await self.rest_client.open_positions()
        except KrakenRESTError as exc:
            logger.warning(
                "Warm start failed to fetch open positions for account %s: %s",
                self.account_id,
                exc,
            )
            return 0

        positions = self._parse_rest_open_positions(payload)
        return await self._apply_open_positions_snapshot(positions)

    async def resync_trades(self) -> int:
        await self.start()

        trades: Iterable[Dict[str, Any]] = []

        if self.ws_client is not None:
            try:
                trades = await self.ws_client.fetch_own_trades_snapshot()
            except (KrakenWSError, KrakenWSTimeout) as exc:
                logger.warning(
                    "Warm start failed to fetch own trades via websocket for account %s: %s",
                    self.account_id,
                    exc,
                )

        if not trades and self.rest_client is not None:
            try:
                payload = await self.rest_client.own_trades()
                trades = self._parse_rest_trades(payload)
            except KrakenRESTError as exc:
                logger.warning(
                    "Warm start failed to fetch own trades via REST for account %s: %s",
                    self.account_id,
                    exc,
                )
                trades = []

        applied = 0
        for trade in trades:
            try:
                if await self.apply_fill_event(trade):
                    applied += 1
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning(
                    "Warm start failed to apply trade snapshot for account %s: %s",
                    self.account_id,
                    exc,
                )
        return applied

    async def apply_fill_event(self, payload: Dict[str, Any]) -> bool:
        state = self._state_from_payload(payload, default_status="filled", transport="kafka")
        if state is None:
            return False

        await self._apply_stream_state(state)

        parent_key = self._child_parent.get(state.client_order_id, state.client_order_id)

        record: OrderRecord | None = None
        async with self._orders_lock:
            record = self._orders.get(parent_key)
            if record:
                symbol = self._extract_symbol(payload)
                side = self._extract_side(payload)
                pre_trade_mid = self._extract_pre_trade_mid(payload)
                if symbol:
                    record.symbol = symbol
                if side:
                    record.side = side
                if pre_trade_mid is not None:
                    record.pre_trade_mid = pre_trade_mid

        if record is not None:
            await self._record_trade_impact(record)

        return True

    async def _apply_open_order_snapshot(
        self, orders: Iterable[Dict[str, Any]] | None, *, replace: bool
    ) -> int:
        if replace:
            async with self._orders_lock:
                self._orders.clear()
                self._child_parent.clear()
                self._child_results.clear()

        if not orders:
            return 0

        applied = 0
        for order in orders:
            state = self._state_from_payload(order, default_status="open", transport="websocket")
            if state is None:
                continue
            await self._apply_stream_state(state)
            parent_key = self._child_parent.get(state.client_order_id, state.client_order_id)
            async with self._orders_lock:
                record = self._orders.get(parent_key)
                if record:
                    symbol = self._extract_symbol(order)
                    side = self._extract_side(order)
                    pre_trade_mid = self._extract_pre_trade_mid(order)
                    if symbol:
                        record.symbol = symbol
                    if side:
                        record.side = side
                    if pre_trade_mid is not None:
                        record.pre_trade_mid = pre_trade_mid
            applied += 1
        return applied

    async def _apply_open_positions_snapshot(self, positions: Iterable[Dict[str, Any]] | None) -> int:
        if not positions:
            async with self._positions_lock:
                self._positions.clear()
            return 0

        applied = 0
        snapshot: Dict[str, Dict[str, Decimal]] = {}
        for entry in positions:
            if not isinstance(entry, dict):
                continue
            symbol = self._extract_symbol(entry)
            if not symbol:
                symbol = str(entry.get("pair") or entry.get("symbol") or "")
            if not symbol:
                continue
            quantity_value: Any = (
                entry.get("volume")
                or entry.get("vol")
                or entry.get("possize")
                or entry.get("quantity")
            )
            quantity = self._extract_decimal(quantity_value)
            if quantity is None:
                continue
            avg_price_value: Any = entry.get("avg_price") or entry.get("price")
            if avg_price_value is None:
                cost = self._extract_decimal(entry.get("cost"))
                if cost is not None and quantity != 0:
                    avg_price_value = cost / quantity
            avg_price = self._extract_decimal(avg_price_value)
            if avg_price is None:
                avg_price = Decimal("0")
            snapshot[symbol] = {
                "quantity": quantity,
                "avg_price": avg_price,
            }
            applied += 1

        async with self._positions_lock:
            self._positions.clear()
            self._positions.update(snapshot)
        return applied

    def _parse_rest_open_orders(self, payload: Dict[str, Any] | None) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []

        orders: List[Dict[str, Any]] = []
        result = payload.get("result")
        if isinstance(result, dict):
            open_section = result.get("open")
            if isinstance(open_section, list):
                orders.extend(order for order in open_section if isinstance(order, dict))
            elif isinstance(open_section, dict):
                orders.extend(order for order in open_section.values() if isinstance(order, dict))
        else:
            open_section = payload.get("open")
            if isinstance(open_section, list):
                orders.extend(order for order in open_section if isinstance(order, dict))
            elif isinstance(open_section, dict):
                orders.extend(order for order in open_section.values() if isinstance(order, dict))
        return orders

    def _parse_rest_trades(self, payload: Dict[str, Any] | None) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []

        trades: List[Dict[str, Any]] = []
        result = payload.get("result")
        if isinstance(result, dict):
            trades_section = result.get("trades")
            if isinstance(trades_section, list):
                trades.extend(trade for trade in trades_section if isinstance(trade, dict))
            elif isinstance(trades_section, dict):
                for trade in trades_section.values():
                    if isinstance(trade, dict):
                        trades.append(trade)
        else:
            trades_section = payload.get("trades")
            if isinstance(trades_section, list):
                trades.extend(trade for trade in trades_section if isinstance(trade, dict))
            elif isinstance(trades_section, dict):
                trades.extend(trade for trade in trades_section.values() if isinstance(trade, dict))
        return trades

    def _parse_rest_open_positions(self, payload: Dict[str, Any] | None) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []

        positions: List[Dict[str, Any]] = []
        result = payload.get("result")
        if isinstance(result, dict):
            for key, value in result.items():
                if isinstance(value, dict):
                    enriched = dict(value)
                    enriched.setdefault("position_id", key)
                    positions.append(enriched)
        else:
            positions_section = payload.get("positions") or payload.get("open_positions")
            if isinstance(positions_section, list):
                positions.extend(
                    position for position in positions_section if isinstance(position, dict)
                )
            elif isinstance(positions_section, dict):
                positions.extend(
                    position for position in positions_section.values() if isinstance(position, dict)
                )
        return positions

    def _state_from_payload(
        self,
        payload: Dict[str, Any],
        *,
        default_status: str,
        transport: str,
    ) -> OrderState | None:
        client_id = self._extract_client_id(payload)
        if client_id is None:
            return None
        exchange_id = self._extract_exchange_id(payload)
        status_value = str(payload.get("status") or payload.get("state") or default_status)
        filled = self._extract_float(payload, ["filled", "filled_qty", "vol_exec", "quantity", "volume"])
        avg_price = self._extract_float(payload, ["avg_price", "price", "avg"])
        return OrderState(
            client_order_id=client_id,
            exchange_order_id=exchange_id,
            status=status_value,
            filled_qty=filled,
            avg_price=avg_price,
            errors=None,
            transport=transport,
        )

    def _extract_client_id(self, payload: Dict[str, Any]) -> str | None:
        keys = [
            "clientOrderId",
            "client_order_id",
            "client_id",
            "userref",
            "order_id",
        ]
        for key in keys:
            value = payload.get(key)
            if value is not None:
                return str(value)
        data = payload.get("order")
        if isinstance(data, dict):
            for key in keys:
                value = data.get(key)
                if value is not None:
                    return str(value)
        return None

    def _extract_exchange_id(self, payload: Dict[str, Any]) -> str | None:
        keys = ["order_id", "txid", "ordertxid", "orderid", "id"]
        for key in keys:
            value = payload.get(key)
            if value is not None:
                return str(value)
        data = payload.get("order")
        if isinstance(data, dict):
            for key in keys:
                value = data.get(key)
                if value is not None:
                    return str(value)
        return None

    def _extract_symbol(self, payload: Dict[str, Any]) -> str | None:
        candidates = [
            payload.get("symbol"),
            payload.get("pair"),
            payload.get("instrument"),
        ]
        descr = payload.get("descr")
        if isinstance(descr, dict):
            candidates.extend([descr.get("pair"), descr.get("symbol")])
        order_payload = payload.get("order")
        if isinstance(order_payload, dict):
            candidates.extend([order_payload.get("symbol"), order_payload.get("pair")])
        for value in candidates:
            if value:
                return str(value)
        return None

    def _extract_side(self, payload: Dict[str, Any]) -> str | None:
        candidates = [payload.get("side"), payload.get("type")]
        descr = payload.get("descr")
        if isinstance(descr, dict):
            candidates.append(descr.get("type"))
        order_payload = payload.get("order")
        if isinstance(order_payload, dict):
            candidates.extend([order_payload.get("side"), order_payload.get("type")])
        for value in candidates:
            if value:
                return str(value)
        return None

    def _extract_pre_trade_mid(self, payload: Dict[str, Any]) -> Decimal | None:
        candidates = [
            payload.get("pre_trade_mid"),
            payload.get("pre_trade_mid_px"),
            payload.get("mid_price"),
            payload.get("mid_px"),
            payload.get("reference_price"),
        ]
        for value in candidates:
            if value is None:
                continue
            decimal_value = self._extract_decimal(value)
            if decimal_value is not None:
                return decimal_value
        return None

    def _extract_float(self, payload: Dict[str, Any], keys: List[str]) -> float | None:
        for key in keys:
            if key not in payload:
                continue
            value = payload.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    def _extract_decimal(self, value: Any) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except Exception:
            return None

    async def place_order(self, request: OMSPlaceRequest) -> OMSPlaceResponse:
        await self.start()

        async def _execute() -> OMSOrderStatusResponse:

            assert self.ws_client is not None
            assert self.rest_client is not None

            metadata = await self.credentials.get_metadata()
            qty, px = _PrecisionValidator.validate(
                request.symbol,
                request.qty,
                request.limit_px,
                metadata,
            )
            book_symbol = _resolve_book_symbol(request.symbol, metadata)
            depth: Optional[Decimal] = None
            try:
                book = await order_book_store.ensure_book(book_symbol)
                depth = await book.depth(request.side, levels=10)
            except Exception as exc:
                logger.debug(
                    "Unable to fetch order book depth for %s on account %s: %s",
                    book_symbol,
                    self.account_id,
                    exc,
                )

            child_quantities = self._plan_child_quantities(request, qty, metadata, depth)
            if not child_quantities:
                child_quantities = [qty]
            increment_oms_child_orders_total(
                self.account_id, request.symbol, len(child_quantities)
            )
            if len(child_quantities) > 1:
                logger.info(
                    "Slicing order for account %s symbol %s into %d child orders (depth=%s)",
                    self.account_id,
                    request.symbol,
                    len(child_quantities),
                    depth,
                )

            child_results: List[OMSOrderStatusResponse] = []
            child_records: List[ChildOrderRecord] = []
            transports_used: Set[str] = set()

            for child_id, parent_id in list(self._child_parent.items()):
                if parent_id == request.client_id:
                    self._child_parent.pop(child_id, None)

            for index, child_qty in enumerate(child_quantities):
                child_client_base = self._child_client_id(request.client_id, index)
                payload = self._build_payload(
                    request, child_qty, px, client_id=child_client_base
                )
                ack, transport, client_id_used, _ = await self._submit_order_with_preference(
                    payload, request.symbol, child_client_base
                )
                transports_used.add(transport)
                child_result = self._order_result_from_ack(client_id_used, ack)
                child_results.append(child_result)
                child_records.append(
                    ChildOrderRecord(
                        client_id=client_id_used,
                        exchange_order_id=child_result.exchange_order_id,
                        transport=transport,
                        quantity=child_qty,
                    )
                )

            aggregated_result = self._aggregate_child_results(
                request.client_id, child_results, child_records
            )
            aggregate_transport = self._aggregate_transport(transports_used)


            async with self._orders_lock:

                self._orders[request.client_id] = OrderRecord(
                    client_id=request.client_id,

                    result=aggregated_result,
                    transport=aggregate_transport,
                    children=child_records,
                    requested_qty=sum((child.quantity for child in child_records), Decimal("0")),
                )
                for child_result, child_record in zip(child_results, child_records):
                    self._child_results[child_record.client_id] = child_result
                    if child_record.client_id != request.client_id:
                        self._orders[child_record.client_id] = OrderRecord(
                            client_id=child_record.client_id,
                            result=child_result,
                            transport=child_record.transport,
                            children=None,
                            requested_qty=child_record.quantity,
                        )
                self._update_child_mapping(request.client_id, child_records)
            return aggregated_result


        cache_key = f"place:{request.client_id}"
        result, reused = await self.idempotency.get_or_create(cache_key, _execute())
        async with self._orders_lock:
            record = self._orders.get(request.client_id)
        transport = record.transport if record else "websocket"
        if record is not None:
            await self._record_trade_impact(record)
        return OMSPlaceResponse(
            exchange_order_id=result.exchange_order_id,
            status=result.status,
            filled_qty=result.filled_qty,
            avg_price=result.avg_price,
            errors=result.errors,
            transport=transport,
            reused=reused,
            shadow=request.shadow,
        )

    async def cancel_order(self, request: OMSCancelRequest) -> OMSOrderStatusResponse:
        await self.start()

        async def _execute_cancel() -> OMSOrderStatusResponse:
            assert self.ws_client is not None
            assert self.rest_client is not None

            async with self._orders_lock:
                record = self._orders.get(request.client_id)

            target_txids: List[str] = []
            if request.exchange_order_id is not None:
                target_txids = [request.exchange_order_id]
            else:
                if record is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Unknown order for cancellation",
                    )
                if record.children:
                    target_txids = [
                        child.exchange_order_id
                        for child in record.children
                        if child.exchange_order_id
                    ]
                if not target_txids and record.result.exchange_order_id:
                    target_txids = [record.result.exchange_order_id]
                if not target_txids:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="No exchange order id available for cancellation",
                    )

            cancel_results: List[OMSOrderStatusResponse] = []
            transports_used: List[str] = []
            for txid in target_txids:
                ack, transport = await self._cancel_single_txid(txid)
                transports_used.append(transport)
                status_value = ack.status or ("rejected" if ack.errors else "canceled")
                cancel_results.append(
                    OMSOrderStatusResponse(
                        exchange_order_id=ack.exchange_order_id or txid,
                        status=status_value,
                        filled_qty=Decimal(str(ack.filled_qty or 0)),
                        avg_price=Decimal(str(ack.avg_price or 0)),
                        errors=ack.errors or None,
                    )
                )

            child_records: List[ChildOrderRecord] = []
            client_lookup = {}
            if record and record.children:
                client_lookup = {
                    child.exchange_order_id: child.client_id for child in record.children
                }
            for result, transport in zip(cancel_results, transports_used):
                child_client_id = client_lookup.get(result.exchange_order_id, request.client_id)
                child_records.append(
                    ChildOrderRecord(
                        client_id=child_client_id,
                        exchange_order_id=result.exchange_order_id,
                        transport=transport,
                    )
                )

            aggregated = self._aggregate_child_results(
                request.client_id, cancel_results, child_records
            )
            aggregate_transport = self._aggregate_transport(set(transports_used))

            async with self._orders_lock:

                existing = self._orders.get(request.client_id)
                self._orders[request.client_id] = OrderRecord(
                    client_id=request.client_id,

                    result=aggregated,
                    transport=aggregate_transport,
                    children=(record.children if record and record.children else child_records),
                )
                for child_result, child_record in zip(cancel_results, child_records):
                    self._child_results[child_record.client_id] = child_result
                    if child_record.client_id != request.client_id:
                        self._orders[child_record.client_id] = OrderRecord(
                            client_id=child_record.client_id,
                            result=child_result,
                            transport=child_record.transport,
                            children=None,
                        )
                if record and record.children:
                    self._update_child_mapping(request.client_id, record.children)
                else:
                    self._update_child_mapping(request.client_id, child_records)
            return aggregated


        cache_key = f"cancel:{request.client_id}"
        result, _ = await self.idempotency.get_or_create(cache_key, _execute_cancel())
        return result

    def _build_payload(
        self,
        request: OMSPlaceRequest,
        qty: Decimal,
        price: Optional[Decimal],
        client_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "clientOrderId": client_id or request.client_id,
            "pair": _normalize_symbol(request.symbol),
            "type": request.side,
            "ordertype": request.order_type.lower(),
            "volume": str(qty),
        }
        payload["idempotencyKey"] = client_id or request.client_id
        if price is not None:
            payload["price"] = str(price)

        oflags = set(flag.lower() for flag in request.flags)
        if request.post_only:
            oflags.add("post")
        if request.reduce_only:
            oflags.add("reduce_only")
        if oflags:
            payload["oflags"] = ",".join(sorted(oflags))

        if request.tif:
            payload["timeInForce"] = request.tif.upper()
        if request.take_profit is not None:
            payload["takeProfit"] = str(request.take_profit)
        if request.stop_loss is not None:
            payload["stopLoss"] = str(request.stop_loss)
        if request.trailing_offset is not None:
            payload["trailingStopOffset"] = str(request.trailing_offset)

        return payload

    def _plan_child_quantities(
        self,
        request: OMSPlaceRequest,
        qty: Decimal,
        metadata: Dict[str, Any] | None,
        depth: Optional[Decimal],
    ) -> List[Decimal]:
        if depth is None or depth <= 0:
            return [qty]
        max_child_qty = depth * Decimal("0.1")
        if max_child_qty <= 0 or qty <= max_child_qty:
            return [qty]

        pair_meta = _resolve_pair_metadata(request.symbol, metadata)
        min_qty = (
            _PrecisionValidator._maybe_decimal(
                pair_meta.get("ordermin"), pair_meta.get("min_qty")
            )
            if pair_meta
            else None
        )
        qty_step = (
            _PrecisionValidator._step(
                pair_meta,
                ["lot_step", "lot_decimals", "step_size"],
            )
            if pair_meta
            else None
        )

        if min_qty is not None and max_child_qty < min_qty:
            max_child_qty = min_qty

        child_quantities = _split_quantities(qty, max_child_qty, qty_step, min_qty)
        if not child_quantities:
            child_quantities = [qty]

        total_allocated = sum(child_quantities)
        if total_allocated != qty and child_quantities:
            difference = qty - total_allocated
            child_quantities[-1] += difference
        return [child for child in child_quantities if child > 0]

    def _child_client_id(self, base: str, index: int) -> str:
        return base if index == 0 else f"{base}-{index + 1}"

    def _derive_retry_client_id(self, base: str) -> str:
        return f"{base}-{uuid.uuid4().hex[:8]}"

    def _update_child_mapping(self, parent: str, children: List[ChildOrderRecord]) -> None:
        for key, value in list(self._child_parent.items()):
            if value == parent:
                self._child_parent.pop(key, None)
        for child in children:
            self._child_parent[child.client_id] = parent

    def _aggregate_child_results(
        self,
        parent_client_id: str,
        child_results: List[OMSOrderStatusResponse],
        child_records: List[ChildOrderRecord],
    ) -> OMSOrderStatusResponse:
        if not child_results:
            return OMSOrderStatusResponse(
                exchange_order_id=parent_client_id,
                status="accepted",
                filled_qty=Decimal("0"),
                avg_price=Decimal("0"),
                errors=None,
            )

        total_filled = sum((result.filled_qty for result in child_results), Decimal("0"))
        requested_total = sum((child.quantity for child in child_records), Decimal("0"))
        notional = Decimal("0")
        errors: List[str] = []
        statuses: List[str] = []
        for result in child_results:
            statuses.append(str(result.status))
            if result.avg_price and result.filled_qty:
                notional += result.avg_price * result.filled_qty
            if result.errors:
                for err in result.errors:
                    if err not in errors:
                        errors.append(err)

        avg_price = Decimal("0")
        if total_filled > 0 and notional > 0:
            avg_price = (notional / total_filled).quantize(
                AGGREGATE_PRICE_PRECISION, rounding=ROUND_HALF_EVEN
            )

        lowered = [status.lower() for status in statuses]
        status_value = child_results[-1].status
        if any(status.startswith("reject") for status in lowered):
            status_value = "rejected"
        elif any(status.startswith("cancel") for status in lowered):
            status_value = "canceled"
        elif requested_total > 0 and total_filled >= requested_total:
            status_value = "filled"
        elif (
            total_filled > 0
            and requested_total > 0
            and not any(
                status in {"filled", "closed", "canceled", "cancelled", "rejected"}
                for status in lowered
            )
        ):
            status_value = "partially_filled"

        exchange_ids = [child.exchange_order_id for child in child_records if child.exchange_order_id]
        exchange_value = ",".join(exchange_ids) if exchange_ids else parent_client_id

        return OMSOrderStatusResponse(
            exchange_order_id=exchange_value,
            status=status_value,
            filled_qty=total_filled,
            avg_price=avg_price,
            errors=errors or None,
        )

    def _aggregate_transport(self, transports: Set[str]) -> str:
        if not transports:
            return "websocket"
        if len(transports) == 1:
            return next(iter(transports))
        return "mixed"

    async def _submit_order_with_preference(
        self,
        payload: Dict[str, Any],
        symbol: str,
        base_client_id: str,
    ) -> Tuple[OrderAck, str, str, float]:
        assert self.ws_client is not None
        assert self.rest_client is not None

        base_payload = dict(payload)
        base_payload.setdefault("idempotencyKey", base_client_id)
        self.routing.update_probe_template(base_payload)
        preferred = self.routing.preferred_path
        transports = [preferred]
        fallback = "rest" if preferred == "websocket" else "websocket"
        if fallback not in transports:
            transports.append(fallback)

        ws_failed = False
        rest_after_ws_attempted = False
        last_ws_error: Exception | None = None
        last_rest_error: Exception | None = None

        for transport in transports:
            attempt_payload = dict(base_payload)
            if transport == "rest" and ws_failed:
                rest_after_ws_attempted = True
                attempt_payload["clientOrderId"] = self._derive_retry_client_id(base_client_id)
            attempt_payload.setdefault("idempotencyKey", base_client_id)

            start = time.perf_counter()
            try:
                if transport == "websocket":
                    ack = await self.ws_client.add_order(attempt_payload)
                else:
                    ack = await self.rest_client.add_order(attempt_payload)
            except (KrakenWSTimeout, KrakenWSError) as exc:
                increment_oms_error_count(self.account_id, symbol, "websocket")
                logger.warning(
                    "Websocket add_order failed for account %s (%s): %s",
                    self.account_id,
                    symbol,
                    exc,
                )
                ws_failed = True
                last_ws_error = exc
                continue
            except KrakenRESTError as rest_exc:
                increment_oms_error_count(self.account_id, symbol, "rest")
                logger.warning(
                    "REST add_order failed for account %s (%s): %s",
                    self.account_id,
                    symbol,
                    rest_exc,
                )
                last_rest_error = rest_exc
                continue

            latency_ms = (time.perf_counter() - start) * 1000.0
            self.routing.record_latency(transport, latency_ms)
            record_oms_latency(self.account_id, symbol, transport, latency_ms)
            logger.info(
                "Order latency account=%s symbol=%s transport=%s latency=%.2fms",
                self.account_id,
                symbol,
                transport,
                latency_ms,
            )
            client_id_used = str(attempt_payload.get("clientOrderId", base_client_id))
            return ack, transport, client_id_used, latency_ms

        if ws_failed and not rest_after_ws_attempted:
            rest_payload = dict(base_payload)
            rest_payload["clientOrderId"] = self._derive_retry_client_id(base_client_id)
            rest_payload.setdefault("idempotencyKey", base_client_id)
            start = time.perf_counter()
            try:
                ack = await self.rest_client.add_order(rest_payload)
            except KrakenRESTError as rest_exc:
                increment_oms_error_count(self.account_id, symbol, "rest")
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=str(rest_exc),
                ) from rest_exc
            latency_ms = (time.perf_counter() - start) * 1000.0
            self.routing.record_latency("rest", latency_ms)
            record_oms_latency(self.account_id, symbol, "rest", latency_ms)
            logger.info(
                "Order latency account=%s symbol=%s transport=rest latency=%.2fms",
                self.account_id,
                symbol,
                latency_ms,
            )
            client_id_used = str(rest_payload["clientOrderId"])
            return ack, "rest", client_id_used, latency_ms

        if last_rest_error is not None:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=str(last_rest_error),
            ) from last_rest_error

        if last_ws_error is not None:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=str(last_ws_error),
            ) from last_ws_error

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Order submission failed: no transport available",
        )

    async def _cancel_single_txid(self, txid: str) -> Tuple[OrderAck, str]:
        assert self.ws_client is not None
        assert self.rest_client is not None

        try:
            ack = await self.ws_client.cancel_order({"txid": txid})
            return ack, "websocket"
        except (KrakenWSTimeout, KrakenWSError) as exc:
            logger.warning(
                "Websocket cancel failed for account %s (txid=%s): %s",
                self.account_id,
                txid,
                exc,
            )
            try:
                ack = await self.rest_client.cancel_order({"txid": txid})
            except KrakenRESTError as rest_exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=str(rest_exc),
                ) from rest_exc
            return ack, "rest"

    def _order_result_from_ack(
        self,
        client_id: str,
        ack: OrderAck,
    ) -> OMSOrderStatusResponse:
        status_value = ack.status or ("rejected" if ack.errors else "accepted")
        filled = Decimal(str(ack.filled_qty or 0))
        avg_price = Decimal(str(ack.avg_price or 0))
        return OMSOrderStatusResponse(
            exchange_order_id=ack.exchange_order_id or client_id,
            status=status_value,
            filled_qty=filled,
            avg_price=avg_price,
            errors=ack.errors or None,
        )

    async def lookup(self, client_id: str) -> OrderRecord | None:
        async with self._orders_lock:
            return self._orders.get(client_id)

    def routing_status(self) -> Dict[str, Optional[float] | str]:
        return self.routing.status()


    async def _record_trade_impact(self, record: OrderRecord) -> None:
        if record.pre_trade_mid is None:
            return
        if record.result.filled_qty <= 0:
            return
        if record.result.avg_price <= 0:
            return
        if record.recorded_qty >= record.result.filled_qty:
            return
        if not record.symbol:
            return
        if not record.side:
            return

        filled_qty = record.result.filled_qty
        avg_price = record.result.avg_price
        mid_px = record.pre_trade_mid
        if mid_px is None or mid_px <= 0:
            return

        normalized_side = record.side.lower()
        if normalized_side not in {"buy", "sell"}:
            return

        direction = Decimal("1") if normalized_side == "buy" else Decimal("-1")
        impact_ratio = (avg_price - mid_px) / mid_px
        impact_bps = float((impact_ratio * direction * Decimal("10000")))

        await self._impact_store.record_fill(
            account_id=self.account_id,
            client_order_id=record.client_id,
            symbol=record.symbol,
            side=record.side,
            filled_qty=float(filled_qty),
            avg_price=float(avg_price),
            pre_trade_mid=float(mid_px),
            impact_bps=impact_bps,
            recorded_at=datetime.now(timezone.utc),
        )

        async with self._orders_lock:
            current = self._orders.get(record.client_id)
            if current:
                current.recorded_qty = filled_qty



class OMSManager:
    def __init__(self) -> None:
        self._accounts: Dict[str, AccountContext] = {}
        self._lock = asyncio.Lock()

    async def get_account(self, account_id: str) -> AccountContext:
        async with self._lock:
            ctx = self._accounts.get(account_id)
            if ctx is None:
                ctx = AccountContext(account_id)
                self._accounts[account_id] = ctx
        await ctx.start()
        return ctx

    async def shutdown(self) -> None:
        async with self._lock:
            accounts = list(self._accounts.values())
        await asyncio.gather(*(account.close() for account in accounts), return_exceptions=True)


manager = OMSManager()
warm_start = WarmStartCoordinator(lambda: manager)


async def require_account_id(request: Request) -> str:
    header = request.headers.get("X-Account-ID")
    if not header:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing X-Account-ID header")
    return header


@app.on_event("shutdown")
async def _shutdown() -> None:
    await manager.shutdown()
    await order_book_store.stop()


@app.on_event("startup")
async def _startup() -> None:
    await warm_start.run()


@app.post("/oms/place", response_model=OMSPlaceResponse)
async def place_order(
    payload: OMSPlaceRequest,
    account_id: str = Depends(require_account_id),
) -> OMSPlaceResponse:
    if payload.account_id != account_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account mismatch")

    if payload.order_type.lower() == "limit" and payload.limit_px is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="limit_px required for limit orders")

    account = await manager.get_account(payload.account_id)
    result = await account.place_order(payload)
    return result



@app.post("/oms/cancel", response_model=OMSOrderStatusResponse)
async def cancel_order(
    payload: OMSCancelRequest,
    account_id: str = Depends(require_account_id),
) -> OMSOrderStatusResponse:
    if payload.account_id != account_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account mismatch")

    account = await manager.get_account(payload.account_id)
    result = await account.cancel_order(payload)
    return result



@app.get("/oms/status", response_model=OMSOrderStatusResponse)
async def get_status(
    account_id: str,
    client_id: str,
    header_account: str = Depends(require_account_id),
) -> OMSOrderStatusResponse:
    if account_id != header_account:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account mismatch")

    account = await manager.get_account(account_id)
    record = await account.lookup(client_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown order")
    return record.result


@app.get("/oms/routing/status")
async def get_routing_status(
    account_id: str,
    header_account: str = Depends(require_account_id),
) -> Dict[str, Optional[float] | str]:
    if account_id != header_account:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account mismatch")

    account = await manager.get_account(account_id)
    return account.routing_status()


@app.get("/oms/warm_start/status")
async def get_warm_start_status() -> Dict[str, int]:
    return await warm_start.status()


@app.get("/oms/impact_curve", response_model=ImpactCurveResponse)
async def get_impact_curve(
    symbol: str,
    account_id: str = Depends(require_account_id),
) -> ImpactCurveResponse:
    points_raw = await impact_store.impact_curve(account_id=account_id, symbol=symbol)
    points = [
        ImpactCurvePoint(size=point["size"], impact_bps=point["impact_bps"])
        for point in points_raw
    ]
    return ImpactCurveResponse(symbol=symbol, points=points, as_of=datetime.now(timezone.utc))



