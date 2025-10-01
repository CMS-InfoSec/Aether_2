from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional
from urllib.parse import urlencode

import aiohttp
import websockets
from aiohttp import ClientError
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed, InvalidStatusCode

from services.common.adapters import KrakenSecretManager

logger = logging.getLogger(__name__)

WS_AUTH_URL = "wss://ws-auth.kraken.com/"
REST_BASE_URL = "https://api.kraken.com"
REST_TOKEN_PATH = "/0/private/GetWebSocketsToken"
REST_ADD_ORDER_PATH = "/0/private/AddOrder"
REST_CANCEL_ORDER_PATH = "/0/private/CancelOrder"

ACK_TIMEOUT = 5.0
HEARTBEAT = 10.0
DEFAULT_MONITOR_TIMEOUT = 60.0
RECONNECT_BACKOFF = 2.0
MAX_RECONNECT_ATTEMPTS = 3


@dataclass
class OrderState:
    """Mutable order state tracked while awaiting fills."""

    exchange_order_id: Optional[str] = None
    status: str = "pending"
    filled_qty: float = 0.0
    avg_price: float = 0.0
    errors: List[str] = field(default_factory=list)

    def register_fill(self, qty: float, price: float) -> None:
        if qty <= 0:
            return
        notional = self.avg_price * self.filled_qty
        notional += price * qty
        self.filled_qty += qty
        if self.filled_qty > 0:
            self.avg_price = notional / self.filled_qty

    def to_dict(self) -> Dict[str, Any]:
        return {
            "exchange_order_id": self.exchange_order_id,
            "status": self.status,
            "filled_qty": self.filled_qty,
            "avg_price": self.avg_price,
            "errors": list(self.errors),
        }


def _load_credentials(account_id: str) -> Dict[str, str]:
    api_key = os.getenv("KRAKEN_API_KEY")
    api_secret = os.getenv("KRAKEN_API_SECRET")
    if api_key and api_secret:
        return {"api_key": api_key, "api_secret": api_secret}

    manager = KrakenSecretManager(account_id=account_id)
    credentials = manager.get_credentials()
    return {
        "api_key": credentials.get("api_key") or "",
        "api_secret": credentials.get("api_secret") or "",
    }


def _kraken_signature(path: str, data: Mapping[str, Any], secret: str) -> str:
    postdata = urlencode(data)
    nonce = str(data.get("nonce", ""))
    message = (nonce + postdata).encode()
    try:
        decoded_secret = base64.b64decode(secret)
    except Exception:
        decoded_secret = secret.encode()
    sha_hash = hashlib.sha256(message).digest()
    mac = hmac.new(decoded_secret, path.encode() + sha_hash, hashlib.sha512)
    return base64.b64encode(mac.digest()).decode()


async def _private_request(
    session: aiohttp.ClientSession,
    path: str,
    credentials: Mapping[str, str],
    data: Optional[MutableMapping[str, Any]] = None,
) -> Dict[str, Any]:
    payload: MutableMapping[str, Any] = dict(data or {})
    payload["nonce"] = payload.get("nonce") or str(int(time.time() * 1000))
    headers = {
        "API-Key": credentials.get("api_key", ""),
        "API-Sign": _kraken_signature(path, payload, credentials.get("api_secret", "")),
        "User-Agent": "AetherOMS/1.0",
    }
    url = f"{REST_BASE_URL}{path}"
    try:
        async with session.post(url, data=payload, headers=headers) as response:
            try:
                response.raise_for_status()
            except aiohttp.ClientResponseError as exc:  # type: ignore[attr-defined]
                raise RuntimeError(f"Kraken REST error: {exc}") from exc
            try:
                return await response.json(content_type=None)
            except Exception as exc:  # pragma: no cover - defensive
                text = await response.text()
                raise RuntimeError(f"Invalid Kraken REST response: {text}") from exc
    except ClientError as exc:
        raise RuntimeError(f"Kraken REST transport error: {exc}") from exc


async def _fetch_ws_token(credentials: Mapping[str, str]) -> str:
    token = os.getenv("KRAKEN_WS_TOKEN")
    if token:
        return token

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        payload = await _private_request(session, REST_TOKEN_PATH, credentials, {})
    errors = payload.get("error") or []
    if errors:
        raise RuntimeError(
            "Failed to obtain Kraken websocket token: " + "; ".join(map(str, errors))
        )
    result = payload.get("result") or {}
    token_value = result.get("token")
    if not token_value:
        raise RuntimeError("Kraken websocket token missing in response")
    return str(token_value)


def _build_ws_payload(intent: Mapping[str, Any], client_order_id: str) -> Dict[str, Any]:
    pair = intent.get("instrument") or intent.get("pair")
    if not pair:
        raise ValueError("Order intent is missing 'instrument' or 'pair'.")
    if "-" in pair:
        pair = pair.replace("-", "/")

    quantity = float(intent.get("qty") or intent.get("quantity") or 0)
    if quantity <= 0:
        raise ValueError("Order quantity must be positive")

    payload: Dict[str, Any] = {
        "event": "addOrder",
        "ordertype": intent.get("order_type", "limit"),
        "type": str(intent.get("side", "")).lower(),
        "volume": str(quantity),
        "pair": pair,
        "clientOrderId": client_order_id,
    }
    price = intent.get("price")
    if price is not None:
        payload["price"] = str(price)

    tif = intent.get("tif") or intent.get("time_in_force")
    if tif:
        payload["timeInForce"] = tif

    flags = intent.get("flags")
    if isinstance(flags, str):
        payload["oflags"] = flags
    elif isinstance(flags, Iterable):
        payload["oflags"] = ",".join(str(flag) for flag in flags)

    post_only = intent.get("post_only")
    reduce_only = intent.get("reduce_only")
    if post_only and "oflags" not in payload:
        payload["oflags"] = "post"
    elif post_only and payload.get("oflags"):
        payload["oflags"] += ",post"
    if reduce_only and payload.get("oflags"):
        payload["oflags"] += ",reduce_only"
    elif reduce_only:
        payload["oflags"] = "reduce_only"

    return payload


def _prepare_rest_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    rest_payload = {
        "ordertype": payload.get("ordertype"),
        "type": payload.get("type"),
        "pair": payload.get("pair"),
        "volume": payload.get("volume"),
    }
    if payload.get("price") is not None:
        rest_payload["price"] = payload.get("price")
    if payload.get("timeInForce"):
        rest_payload["timeinforce"] = payload.get("timeInForce")
    if payload.get("oflags"):
        rest_payload["oflags"] = payload.get("oflags")
    if payload.get("clientOrderId"):
        rest_payload["userref"] = payload.get("clientOrderId")
    return rest_payload


async def _rest_add_order(credentials: Mapping[str, str], payload: Mapping[str, Any]) -> Dict[str, Any]:
    rest_payload = _prepare_rest_payload(payload)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        response = await _private_request(session, REST_ADD_ORDER_PATH, credentials, rest_payload)
    return response


async def _rest_cancel_order(
    credentials: Mapping[str, str],
    order_ids: Iterable[str],
) -> Dict[str, Any]:
    data: Dict[str, Any] = {"txid": ",".join(order_ids)}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        return await _private_request(session, REST_CANCEL_ORDER_PATH, credentials, data)


async def _connect_with_retry() -> WebSocketClientProtocol:
    delay = 1.0
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RECONNECT_ATTEMPTS + 1):
        try:
            return await websockets.connect(
                WS_AUTH_URL,
                ping_interval=HEARTBEAT,
                ping_timeout=HEARTBEAT,
                close_timeout=1,
                max_queue=None,
            )
        except (OSError, InvalidStatusCode, ConnectionClosed) as exc:
            last_error = exc
            logger.warning("Kraken websocket connection failed (attempt %s): %s", attempt, exc)
            await asyncio.sleep(delay)
            delay *= RECONNECT_BACKOFF
    raise RuntimeError(f"Unable to connect to Kraken websocket: {last_error}")


async def _send_json(ws: WebSocketClientProtocol, message: Mapping[str, Any]) -> None:
    await ws.send(json.dumps(message))


async def _subscribe(ws: WebSocketClientProtocol, token: str, *channels: str) -> None:
    for channel in channels:
        await _send_json(
            ws,
            {
                "event": "subscribe",
                "subscription": {"name": channel, "token": token},
            },
        )


def _is_order_message(message: Any, client_order_id: str, exchange_order_id: Optional[str]) -> bool:
    if isinstance(message, Mapping):
        if message.get("clientOrderId") == client_order_id:
            return True
        if exchange_order_id and message.get("txid") == exchange_order_id:
            return True
    return False


def _extract_fills(message: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    if "trades" in message and isinstance(message["trades"], Iterable):
        for trade in message["trades"]:
            if isinstance(trade, Mapping):
                yield trade
    if "fills" in message and isinstance(message["fills"], Iterable):
        for fill in message["fills"]:
            if isinstance(fill, Mapping):
                yield fill


def _update_state_from_message(
    state: OrderState,
    message: Mapping[str, Any],
    *,
    client_order_id: str,
    expected_qty: float,
) -> None:
    if not _is_order_message(message, client_order_id, state.exchange_order_id):
        return

    if message.get("txid") and not state.exchange_order_id:
        state.exchange_order_id = str(message["txid"])

    status = message.get("status") or message.get("orderStatus")
    if status:
        state.status = str(status).lower()

    filled = message.get("filled") or message.get("vol_exec")
    price = message.get("avg_price") or message.get("price")
    if filled is not None:
        try:
            qty = float(filled)
        except (TypeError, ValueError):
            qty = 0.0
        fill_price = 0.0
        if price is not None:
            try:
                fill_price = float(price)
            except (TypeError, ValueError):
                fill_price = 0.0
        if qty > 0:
            incremental = max(qty - state.filled_qty, 0.0)
            if incremental > 0:
                state.register_fill(incremental, fill_price or state.avg_price)

    for fill in _extract_fills(message):
        try:
            qty = float(fill.get("vol") or fill.get("qty") or 0.0)
        except (TypeError, ValueError):
            qty = 0.0
        try:
            price_value = float(fill.get("price") or 0.0)
        except (TypeError, ValueError):
            price_value = 0.0
        if qty > 0:
            state.register_fill(qty, price_value)

    if state.filled_qty >= expected_qty and expected_qty > 0:
        state.status = "filled"


async def _await_ack(
    ws: WebSocketClientProtocol,
    client_order_id: str,
    timeout: float = ACK_TIMEOUT,
) -> Mapping[str, Any]:
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            raise asyncio.TimeoutError("Timed out awaiting Kraken acknowledgement")
        message = await asyncio.wait_for(ws.recv(), timeout=remaining)
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping) and _is_order_message(payload, client_order_id, None):
            return payload
        if isinstance(payload, list) and len(payload) >= 2 and isinstance(payload[1], Mapping):
            if _is_order_message(payload[1], client_order_id, None):
                return payload[1]


async def _monitor_order(
    ws: WebSocketClientProtocol,
    *,
    token: str,
    state: OrderState,
    client_order_id: str,
    expected_qty: float,
    timeout: float,
    cancel_after: Optional[float] = None,
) -> None:
    await _subscribe(ws, token, "openOrders", "ownTrades")
    start = time.time()
    last_activity = time.time()
    while True:
        try:
            message = await asyncio.wait_for(ws.recv(), timeout=HEARTBEAT)
            last_activity = time.time()
        except asyncio.TimeoutError:
            await _send_json(ws, {"event": "ping"})
            if cancel_after and (time.time() - start) > cancel_after:
                state.status = "cancelled"
                state.errors.append("Canceled due to cancel_after timeout")
                return
            if (time.time() - start) > timeout:
                state.errors.append("Order monitoring timeout reached")
                return
            continue

        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            continue

        if isinstance(payload, Mapping):
            if payload.get("event") == "heartbeat":
                continue
            if payload.get("event") == "systemStatus":
                continue
            if payload.get("event") == "subscriptionStatus" and payload.get("status") != "subscribed":
                state.errors.append(f"Subscription failed: {payload}")
                continue
            _update_state_from_message(
                state,
                payload,
                client_order_id=client_order_id,
                expected_qty=expected_qty,
            )
        elif isinstance(payload, list) and len(payload) >= 2 and isinstance(payload[1], Mapping):
            _update_state_from_message(
                state,
                payload[1],
                client_order_id=client_order_id,
                expected_qty=expected_qty,
            )

        if state.status in {"filled", "canceled", "cancelled", "rejected", "expired"}:
            return

        if cancel_after and (time.time() - start) > cancel_after:
            state.status = "cancelled"
            state.errors.append("Canceled due to cancel_after timeout")
            return
        if (time.time() - start) > timeout:
            state.errors.append("Order monitoring timeout reached")
            return
        if (time.time() - last_activity) > HEARTBEAT * 3:
            await _send_json(ws, {"event": "ping"})


async def execute_order(account_id: str, intent: Mapping[str, Any]) -> Dict[str, Any]:
    credentials = _load_credentials(account_id)
    client_order_id = str(intent.get("client_order_id") or intent.get("clientOrderId") or uuid.uuid4())
    ws_payload = _build_ws_payload(intent, client_order_id)
    raw_expected_qty = intent.get("qty") or intent.get("quantity") or ws_payload.get("volume")
    try:
        expected_qty = float(raw_expected_qty) if raw_expected_qty is not None else 0.0
    except (TypeError, ValueError):
        expected_qty = 0.0

    state = OrderState(status="pending")
    cancel_ids: List[str] = []
    if intent.get("cancel_order_id"):
        cancel_ids.append(str(intent["cancel_order_id"]))
    if intent.get("cancel_order_ids"):
        cancel_ids.extend(str(value) for value in intent["cancel_order_ids"])
    if intent.get("replace_order_id"):
        cancel_ids.append(str(intent["replace_order_id"]))
    if cancel_ids:
        cancel_ids = list(dict.fromkeys(cancel_ids))

    timeout = float(intent.get("monitor_timeout") or DEFAULT_MONITOR_TIMEOUT)
    cancel_after = intent.get("cancel_after")
    cancel_after_value: Optional[float]
    if cancel_after is None:
        cancel_after_value = None
    else:
        try:
            cancel_after_value = float(cancel_after)
        except (TypeError, ValueError):
            state.errors.append(f"Invalid cancel_after value: {cancel_after}")
            cancel_after_value = None

    ws: Optional[WebSocketClientProtocol] = None
    token: Optional[str] = None
    ack: Optional[Mapping[str, Any]] = None

    try:
        token = await _fetch_ws_token(credentials)
        ws = await _connect_with_retry()

        if cancel_ids:
            cancel_message = {"event": "cancelOrder", "token": token, "txid": cancel_ids}
            await _send_json(ws, cancel_message)

        ws_payload_with_token = dict(ws_payload)
        ws_payload_with_token["token"] = token

        await _send_json(ws, ws_payload_with_token)
        try:
            ack = await _await_ack(ws, client_order_id)
        except asyncio.TimeoutError as exc:
            raise RuntimeError("Kraken websocket acknowledgement timeout") from exc

        _update_state_from_message(
            state,
            dict(ack),
            client_order_id=client_order_id,
            expected_qty=expected_qty,
        )

        if state.status in {"error", "rejected"}:
            state.errors.append(f"Order rejected by websocket: {ack}")
            return state.to_dict()

        await _monitor_order(
            ws,
            token=token,
            state=state,
            client_order_id=client_order_id,
            expected_qty=expected_qty,
            timeout=timeout,
            cancel_after=cancel_after_value,
        )
    except Exception as exc:
        logger.warning("Kraken websocket flow failed, attempting REST fallback: %s", exc)
        state.errors.append(str(exc))
        if cancel_ids:
            try:
                await _rest_cancel_order(credentials, cancel_ids)
            except Exception as cancel_exc:
                state.errors.append(f"Cancel fallback failed: {cancel_exc}")
        try:
            rest_response = await _rest_add_order(credentials, ws_payload)
        except Exception as rest_exc:
            state.errors.append(f"REST fallback failed: {rest_exc}")
            state.status = "error"
            return state.to_dict()

        result = rest_response.get("result") or {}
        txids = result.get("txid") or result.get("txids") or []
        if isinstance(txids, list) and txids:
            state.exchange_order_id = txids[0]
        elif isinstance(txids, str):
            state.exchange_order_id = txids
        state.status = "accepted" if not rest_response.get("error") else "error"
        errors = rest_response.get("error") or []
        state.errors.extend(str(err) for err in errors)
        return state.to_dict()
    finally:
        if ws is not None:
            try:
                await ws.close()
            except Exception:  # pragma: no cover - best effort
                logger.debug("Failed to close Kraken websocket cleanly", exc_info=True)

    if not state.exchange_order_id and ack:
        state.exchange_order_id = str(ack.get("txid") or ack.get("orderId") or "") or None

    if state.status in {"pending", "open"}:
        state.status = "working"

    return state.to_dict()
