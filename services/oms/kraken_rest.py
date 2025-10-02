from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import itertools
import json
import logging
import random
import time
import urllib.parse
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp

from services.oms.kraken_ws import OrderAck

logger = logging.getLogger(__name__)

KRAKEN_REST_URL = "https://api.kraken.com/0"


class KrakenRESTError(RuntimeError):
    """Raised when a Kraken REST request fails permanently."""


class KrakenRESTClient:
    def __init__(
        self,
        *,
        credential_getter: Callable[[], Awaitable[Dict[str, Any]]],
        session: Optional[aiohttp.ClientSession] = None,
        base_url: str = KRAKEN_REST_URL,
        max_retries: int = 3,
    ) -> None:
        self._credential_getter = credential_getter
        self._provided_session = session
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries
        self._nonce_lock = asyncio.Lock()
        self._nonce_counter = itertools.count(int(time.time() * 1000))

    async def _session_or_create(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and self._session is not self._provided_session:
            await self._session.close()
        self._session = None

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        response = await self._request("/private/AddOrder", payload)
        return self._parse_response(response)

    async def cancel_order(self, payload: Dict[str, Any]) -> OrderAck:
        response = await self._request("/private/CancelOrder", payload)
        return self._parse_response(response)

    async def open_orders(self) -> Dict[str, Any]:
        return await self._request("/private/OpenOrders", {})

    async def balance(self) -> Dict[str, Any]:
        """Return the latest balance snapshot for the authenticated account."""

        return await self._request("/private/Balance", {})

    async def own_trades(self, *, start: Optional[int] = None, end: Optional[int] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"trades": True}
        if start is not None:
            payload["start"] = int(start)
        if end is not None:
            payload["end"] = int(end)
        return await self._request("/private/TradesHistory", payload)

    async def open_positions(self, *, docalcs: bool = True) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"docalcs": bool(docalcs)}
        return await self._request("/private/OpenPositions", payload)

    async def websocket_token(self) -> str:
        """Fetch a websocket authentication token for the current credentials."""

        payload = await self._request("/private/GetWebSocketsToken", {})
        result = payload.get("result") or {}
        token = result.get("token")
        if not token:
            raise KrakenRESTError("Kraken REST token response missing token")
        return str(token)

    async def _request(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        credentials = await self._credential_getter()
        session = await self._session_or_create()

        nonce = await self._next_nonce()
        body = dict(payload)
        body["nonce"] = nonce
        encoded = urllib.parse.urlencode(body)

        signature = self._sign_request(path, nonce, encoded, credentials)
        headers = {
            "API-Key": credentials.get("api_key", ""),
            "API-Sign": signature,
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        }

        url = f"{self._base_url}{path}"
        attempt = 0
        backoff = 0.5
        while True:
            attempt += 1
            try:
                async with session.post(url, data=encoded, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    text = await resp.text()
                    if resp.status >= 500:
                        raise KrakenRESTError(f"server error {resp.status}: {text}")
                    payload = json.loads(text) if text else {}
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self._max_retries:
                    raise KrakenRESTError(f"REST request failed: {exc}") from exc
                delay = min(backoff * random.uniform(0.8, 1.2), 5.0)
                await asyncio.sleep(delay)
                backoff *= 2
                continue

            errors = payload.get("error", [])
            if errors:
                if attempt >= self._max_retries or not _is_retryable(errors):
                    raise KrakenRESTError(
                        f"Kraken API error: {errors}",
                    )
                delay = min(backoff * random.uniform(0.8, 1.2), 5.0)
                await asyncio.sleep(delay)
                backoff *= 2
                continue

            return payload

    async def _next_nonce(self) -> str:
        async with self._nonce_lock:
            nonce_value = next(self._nonce_counter)
        return str(nonce_value)

    def _parse_response(self, payload: Dict[str, Any]) -> OrderAck:
        result = payload.get("result") or {}
        txid = result.get("txid")
        if isinstance(txid, list):
            txid = txid[0]
        descr = result.get("descr", {})
        filled = result.get("filled")
        avg_price = result.get("avg_price") or descr.get("price")
        status_value = result.get("status")
        if status_value is None and "count" in result:
            status_value = "canceled" if result.get("count") else "not_found"
        return OrderAck(
            status=status_value or "ok",
            exchange_order_id=str(txid) if txid else None,
            filled_qty=_to_float(filled),
            avg_price=_to_float(avg_price),
            errors=None,
        )

    def _sign_request(
        self,
        path: str,
        nonce: str,
        encoded: str,
        credentials: Dict[str, Any],
    ) -> str:
        secret = credentials.get("api_secret", "")
        key = base64.b64decode(secret) if secret else b""
        message = path.encode() + hashlib.sha256((nonce + encoded).encode()).digest()
        mac = hmac.new(key, message, hashlib.sha512)
        return base64.b64encode(mac.digest()).decode()


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):  # pragma: no cover
        return None


def _is_retryable(errors: list[Any]) -> bool:
    retryable_prefixes = {"EAPI:Rate limit", "EService:Unavailable"}
    for error in errors:
        for prefix in retryable_prefixes:
            if str(error).startswith(prefix):
                return True
    return False

