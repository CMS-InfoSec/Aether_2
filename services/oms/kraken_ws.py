from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, List, Optional
from uuid import uuid4


import websockets
from websockets import WebSocketClientProtocol
from websockets.exceptions import WebSocketException


from metrics import get_request_id
from services.oms.rate_limit_guard import (
    RateLimitGuard,
    rate_limit_guard as shared_rate_limit_guard,
)

logger = logging.getLogger(__name__)

KRAKEN_WS_URL = "wss://ws-auth.kraken.com/v2"


class KrakenWSError(RuntimeError):
    """Generic websocket failure."""


class KrakenWSTimeout(asyncio.TimeoutError):
    """Raised when a websocket operation exceeds its deadline."""


@dataclass(slots=True)
class OrderAck:
    exchange_order_id: Optional[str]
    status: Optional[str]
    filled_qty: Optional[Decimal]
    avg_price: Optional[Decimal]
    errors: Optional[List[str]]


@dataclass(slots=True)
class OrderState:
    client_order_id: Optional[str]
    exchange_order_id: Optional[str]
    status: str
    filled_qty: Optional[Decimal]
    avg_price: Optional[Decimal]
    errors: Optional[List[str]]
    transport: str = "websocket"


class _JitterBackoff:
    def __init__(self, base: float = 0.5, factor: float = 2.0, maximum: float = 20.0) -> None:
        self._base = base
        self._factor = factor
        self._maximum = maximum
        self._current = base

    def next(self) -> float:
        jitter = random.random() * self._current
        delay = min(self._current + jitter, self._maximum)
        self._current = min(self._current * self._factor, self._maximum)
        return delay

    def reset(self) -> None:
        self._current = self._base


class _WebsocketTransport:
    """Adapter around websockets for dependency injection."""

    def __init__(self, protocol: WebSocketClientProtocol) -> None:
        self._protocol = protocol

    @property
    def closed(self) -> bool:
        return self._protocol.closed

    async def send_json(self, payload: Dict[str, Any]) -> None:
        await self._protocol.send(json.dumps(payload))

    async def recv_json(self) -> Dict[str, Any]:
        raw = await self._protocol.recv()
        if isinstance(raw, bytes):
            raw = raw.decode()
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            raise KrakenWSError(f"invalid json payload: {raw}") from exc

    async def close(self) -> None:
        await self._protocol.close()



def _log_extra(*, request_id: Optional[str] = None, **extra: Any) -> Dict[str, Any]:
    """Return logging extras enriched with the active request identifier."""

    resolved = dict(extra)
    current_id = request_id or get_request_id()
    if current_id:
        resolved.setdefault("request_id", current_id)
    return resolved



class KrakenWSClient:
    """High level async client for Kraken WebSocket v2 private API."""

    def __init__(
        self,
        *,
        credential_getter: Callable[[], Awaitable[Dict[str, Any]]],
        url: str = KRAKEN_WS_URL,
        transport_factory: Optional[
            Callable[..., Awaitable[_WebsocketTransport]]
        ] = None,
        stream_update_cb: Optional[Callable[[OrderState], Awaitable[None]]] = None,
        rest_client: Optional["KrakenRESTClient"] = None,
        request_timeout: float = 5.0,
        rate_limit_guard: Optional[RateLimitGuard] = None,
        account_id: Optional[str] = None,
    ) -> None:
        self._credential_getter = credential_getter
        self._url = url
        self._transport_factory = transport_factory or self._default_transport
        self._stream_update_cb = stream_update_cb
        self._request_timeout = request_timeout
        self._rest_client = rest_client
        self._rate_limit_guard = rate_limit_guard or shared_rate_limit_guard
        self._account_id = account_id

        self._transport: Optional[_WebsocketTransport] = None
        self._receiver_task: Optional[asyncio.Task[None]] = None
        self._pending: Dict[int, asyncio.Future[Dict[str, Any]]] = {}
        self._open_orders: Dict[str, Dict[str, Any]] = {}
        self._own_trades: Dict[str, Dict[str, Any]] = {}
        self._queue: asyncio.Queue[OrderState] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._reqid = 1
        self._backoff = _JitterBackoff()
        self._subscriptions: List[List[str]] = []
        self._last_private_heartbeat: Optional[float] = None
        self._ws_token: Optional[str] = None
        self._ws_token_expiry: Optional[float] = None

    async def _default_transport(
        self, url: str, *, headers: Optional[Dict[str, str]] = None
    ) -> _WebsocketTransport:
        protocol = await websockets.connect(
            url, ping_interval=None, extra_headers=headers
        )
        return _WebsocketTransport(protocol)

    def set_rest_client(self, rest_client: "KrakenRESTClient") -> None:
        """Attach a REST client used for obtaining websocket tokens."""

        self._rest_client = rest_client

    async def ensure_connected(self) -> None:
        async with self._lock:
            if self._transport and not self._transport.closed:
                return
            await self._connect_locked()

    async def _connect_locked(self) -> None:
        request_id = get_request_id() or str(uuid4())
        headers = {"X-Request-ID": request_id}
        while True:
            try:
                logger.info(
                    "Connecting to Kraken websocket at %s",
                    self._url,
                    extra=_log_extra(request_id=request_id, url=self._url),
                )
                transport = await self._transport_factory(
                    self._url, headers=headers
                )
                self._transport = transport
                if self._receiver_task is None or self._receiver_task.done():
                    self._receiver_task = asyncio.create_task(
                        self._receiver_loop(), name="kraken-ws-recv"
                    )
                    self._receiver_task.add_done_callback(self._on_receiver_done)
                self._backoff.reset()
                for channels in self._subscriptions:
                    await self._send_subscribe(channels)
                return
            except (OSError, WebSocketException) as exc:
                delay = self._backoff.next()
                logger.warning(
                    "Websocket connection failed: %s. retrying in %.2fs",
                    exc,
                    delay,
                    extra=_log_extra(request_id=request_id, delay=delay),
                )
                await asyncio.sleep(delay)

    async def close(self) -> None:
        if self._receiver_task:
            self._receiver_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._receiver_task
            self._receiver_task = None
        if self._transport:
            await self._transport.close()
            self._transport = None

    def _on_receiver_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except Exception:  # pragma: no cover - defensive
            exc = None
        if exc:
            logger.warning(
                "Kraken websocket receiver terminated: %s",
                exc,
                extra=_log_extra(),
            )
        self._receiver_task = None

    async def subscribe_private(self, channels: List[str]) -> None:
        await self._send_subscribe(channels)
        if channels not in self._subscriptions:
            self._subscriptions.append(channels)

    async def _send_subscribe(self, channels: List[str]) -> None:
        payload = {
            "event": "subscribe",
            "subscription": {
                "name": "private",
                "token": await self._sign_auth(),
                "channels": channels,
            },
        }
        await self._send(payload)

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        response = await self._request("add_order", payload)
        return self._ack_from_payload(response)

    async def cancel_order(self, payload: Dict[str, Any]) -> OrderAck:
        response = await self._request("cancel_order", payload)
        return self._ack_from_payload(response)

    async def _request(self, channel: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        await self.ensure_connected()
        reqid = self._next_reqid()
        message = {
            "method": channel,
            "params": payload,
            "req_id": reqid,
            "token": await self._sign_auth(),
        }
        future: asyncio.Future[Dict[str, Any]] = asyncio.get_event_loop().create_future()
        self._pending[reqid] = future
        try:
            await self._transport.send_json(message)  # type: ignore[union-attr]
        except Exception as exc:
            self._pending.pop(reqid, None)
            raise KrakenWSError(str(exc)) from exc

        try:
            return await asyncio.wait_for(future, timeout=self._request_timeout)
        except asyncio.TimeoutError as exc:
            self._pending.pop(reqid, None)
            raise KrakenWSTimeout("websocket request timed out") from exc

    async def _send(self, payload: Dict[str, Any]) -> None:
        await self.ensure_connected()
        try:
            await self._transport.send_json(payload)  # type: ignore[union-attr]
        except Exception as exc:  # pragma: no cover - network failure
            raise KrakenWSError(str(exc)) from exc

    async def fetch_open_orders_snapshot(self) -> List[Dict[str, Any]]:
        await self.ensure_connected()

        methods = ["open_orders", "openOrders", "openOrdersStatus", "open_orders_status"]
        last_error: Exception | None = None
        for method in methods:
            try:
                payload = await self._request(method, {})
            except (KrakenWSError, KrakenWSTimeout) as exc:
                last_error = exc
                continue
            orders = self._parse_open_orders(payload)
            if orders is not None:
                return orders
        if last_error is not None:
            raise last_error
        return list(self._open_orders.values())

    async def fetch_own_trades_snapshot(self) -> List[Dict[str, Any]]:
        await self.ensure_connected()

        methods = ["own_trades", "ownTrades", "ownTradesStatus", "own_trades_status"]
        last_error: Exception | None = None
        for method in methods:
            try:
                payload = await self._request(method, {})
            except (KrakenWSError, KrakenWSTimeout) as exc:
                last_error = exc
                continue
            trades = self._parse_own_trades(payload)
            if trades is not None:
                return trades
        if last_error is not None:
            raise last_error
        return list(self._own_trades.values())

    def _parse_open_orders(self, payload: Dict[str, Any] | None) -> List[Dict[str, Any]] | None:
        if not isinstance(payload, dict):
            return None

        candidates: List[Any] = []
        data = payload.get("data")
        if isinstance(data, list):
            candidates.extend(data)

        open_section = payload.get("open")
        if isinstance(open_section, list):
            candidates.extend(open_section)
        elif isinstance(open_section, dict):
            candidates.extend(open_section.values())

        result = payload.get("result")
        if isinstance(result, dict):
            open_result = result.get("open")
            if isinstance(open_result, list):
                candidates.extend(open_result)
            elif isinstance(open_result, dict):
                candidates.extend(open_result.values())

        orders: List[Dict[str, Any]] = []
        for entry in candidates:
            if isinstance(entry, dict):
                orders.append(entry)

        if orders:
            self._open_orders.clear()
            for order in orders:
                txid = order.get("order_id") or order.get("txid") or order.get("id")
                if txid is not None:
                    self._open_orders[str(txid)] = order
        return orders

    def _parse_own_trades(self, payload: Dict[str, Any] | None) -> List[Dict[str, Any]] | None:
        if not isinstance(payload, dict):
            return None

        candidates: List[Any] = []
        data = payload.get("data")
        if isinstance(data, list):
            candidates.extend(data)

        trades_section = payload.get("trades")
        if isinstance(trades_section, list):
            candidates.extend(trades_section)
        elif isinstance(trades_section, dict):
            candidates.extend(trades_section.values())

        result = payload.get("result")
        if isinstance(result, dict):
            trades_result = result.get("trades")
            if isinstance(trades_result, list):
                candidates.extend(trades_result)
            elif isinstance(trades_result, dict):
                candidates.extend(trades_result.values())

        trades: List[Dict[str, Any]] = []
        for entry in candidates:
            if isinstance(entry, dict):
                trades.append(entry)

        if trades:
            self._own_trades.clear()
            for trade in trades:
                txid = trade.get("order_id") or trade.get("ordertxid") or trade.get("txid")
                if txid is not None:
                    self._own_trades[str(txid)] = trade
        return trades

    async def stream_handler(self) -> None:
        while True:
            state = await self._queue.get()
            if self._stream_update_cb:
                await self._stream_update_cb(state)

    async def _receiver_loop(self) -> None:
        while True:
            try:
                payload = await self._transport.recv_json()  # type: ignore[union-attr]
            except asyncio.CancelledError:  # pragma: no cover - cancellation path
                raise
            except Exception as exc:
                logger.warning(
                    "Websocket receiver stopped: %s",
                    exc,
                    extra=_log_extra(),
                )
                if self._transport:
                    with contextlib.suppress(Exception):
                        await self._transport.close()
                    self._transport = None
                self._fail_pending(KrakenWSError(str(exc)))
                await self.ensure_connected()
                continue
            await self._handle_payload(payload)

    def _fail_pending(self, exc: Exception) -> None:
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(exc)
        self._pending.clear()

    async def _handle_payload(self, payload: Dict[str, Any]) -> None:
        req_id = payload.get("req_id") or payload.get("reqid")
        if req_id and req_id in self._pending:
            future = self._pending.pop(req_id)
            if not future.done():
                future.set_result(payload)
            return

        channel = payload.get("channel") or payload.get("subscription", {}).get("name")
        if channel == "openOrders":
            await self._handle_open_orders(payload)
        elif channel == "ownTrades":
            await self._handle_own_trades(payload)
        elif channel == "heartbeat":
            self._last_private_heartbeat = time.time()
            return
        else:
            logger.debug(
                "Unhandled websocket payload: %s",
                payload,
                extra=_log_extra(),
            )

    async def _handle_open_orders(self, payload: Dict[str, Any]) -> None:
        data = payload.get("data") or payload.get("open") or []
        if isinstance(data, list):
            for order in data:
                txid = order.get("order_id") or order.get("txid")
                self._open_orders[str(txid)] = order
                await self._publish_state(order)

    async def _handle_own_trades(self, payload: Dict[str, Any]) -> None:
        trades = payload.get("data") or payload.get("trades") or []
        if isinstance(trades, list):
            for trade in trades:
                txid = trade.get("order_id") or trade.get("ordertxid")
                self._own_trades[str(txid)] = trade
                await self._publish_state(trade)

    async def _publish_state(self, data: Dict[str, Any]) -> None:
        if not isinstance(data, dict):
            return

        client = data.get("clientOrderId") or data.get("userref")
        exchange = data.get("order_id") or data.get("txid") or data.get("ordertxid")
        state = OrderState(
            client_order_id=str(client) if client is not None else None,
            exchange_order_id=str(exchange) if exchange is not None else None,
            status=str(data.get("status", "open")),
            filled_qty=_to_decimal(data.get("filled") or data.get("vol_exec")),
            avg_price=_to_decimal(data.get("avg_price") or data.get("price")),
            errors=None,
        )
        await self._queue.put(state)

    async def _sign_auth(self) -> str:
        credentials = await self._credential_getter()
        token = credentials.get("ws_token")
        if token:
            token_str = str(token)
            self._ws_token = token_str
            self._ws_token_expiry = None
            return token_str

        now = time.monotonic()
        if self._ws_token and self._ws_token_expiry is not None:
            if now < self._ws_token_expiry:
                return self._ws_token
        elif self._ws_token:
            return self._ws_token

        rest_client = self._rest_client
        if rest_client is None:
            raise KrakenWSError("ws_token missing and REST client unavailable")

        account_id = self._account_id or credentials.get("account_id")
        if account_id is None:
            account_id = credentials.get("api_key") or "default"
        account_id = str(account_id)
        self._account_id = account_id

        guard = self._rate_limit_guard
        if guard is not None:
            await guard.acquire(account_id, "websocket_token", transport="rest", urgent=False)

        try:
            token, ttl = await rest_client.websocket_token()
        except Exception as exc:
            logger.error("Failed to obtain Kraken websocket token: %s", exc)
            raise KrakenWSError("failed to obtain websocket token") from exc

        if not token:
            raise KrakenWSError("Kraken REST token response missing token")

        token_str = str(token)
        ttl_seconds = 0.0
        if ttl is not None:
            try:
                ttl_seconds = float(ttl)
            except (TypeError, ValueError):
                ttl_seconds = 0.0

        if ttl_seconds <= 0:
            expiry = time.monotonic()
        else:
            refresh_margin = max(ttl_seconds - 30.0, ttl_seconds * 0.5, 1.0)
            expiry = time.monotonic() + refresh_margin

        self._ws_token = token_str
        self._ws_token_expiry = expiry
        return token_str

    def _ack_from_payload(self, payload: Dict[str, Any]) -> OrderAck:
        status = payload.get("status") or payload.get("result", {}).get("status")
        txid = payload.get("txid") or payload.get("result", {}).get("txid")
        result = payload.get("result") or {}
        filled = _to_decimal(result.get("filled"))
        price = _to_decimal(result.get("avg_price"))
        errors = payload.get("error") or result.get("error")
        if isinstance(errors, str):
            errors = [errors]
        return OrderAck(
            exchange_order_id=str(txid) if txid else None,
            status=status,
            filled_qty=filled,
            avg_price=price,
            errors=errors,
        )

    def _next_reqid(self) -> int:
        self._reqid += 1
        return self._reqid

    def heartbeat_age(self) -> Optional[float]:
        if self._last_private_heartbeat is None:
            return None
        return max(time.time() - self._last_private_heartbeat, 0.0)


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return Decimal(stripped)
        except InvalidOperation:
            return None
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except InvalidOperation:  # pragma: no cover - defensive
            return None
    return None

