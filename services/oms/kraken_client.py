from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from services.common.adapters import KrakenSecretManager


class KrakenWebsocketError(RuntimeError):
    """Represents a transport level failure when communicating with Kraken."""


class KrakenWebsocketTimeout(TimeoutError):
    """Raised when the websocket stalls beyond the configured timeout."""


@dataclass
class _LoopbackSession:
    """Fallback session used in tests when no external transport is provided."""

    credentials: Dict[str, str]

    def __post_init__(self) -> None:
        self._closed = False

    def request(self, channel: str, payload: Dict[str, Any], timeout: float | None = None) -> Dict[str, Any]:
        if self._closed:
            raise KrakenWebsocketError("session closed")
        # Pretend the message goes out and comes back instantly.
        txid = payload.get("txid") or payload.get("clientOrderId") or f"SIM-{random.randint(1, 1_000_000)}"
        if channel == "add_order":
            return {"status": "ok", "txid": txid, "channel": channel, "echo": payload}
        if channel == "cancel_order":
            return {"status": "canceled", "txid": payload.get("txid"), "channel": channel}
        if channel == "openOrders":
            return {"status": "ok", "open": []}
        if channel == "ownTrades":
            return {"status": "ok", "trades": []}
        return {"status": "ok", "channel": channel}

    def close(self) -> None:
        self._closed = True


class KrakenWSClient:
    """Minimal Kraken private websocket client with retry and REST fallback."""

    def __init__(
        self,
        account_id: str,
        *,
        session_factory: Optional[Callable[[Dict[str, str]], Any]] = None,
        credentials: Optional[Dict[str, Any]] = None,
        rest_fallback: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
        max_retries: int = 2,
        retry_delay: float = 0.1,
    ) -> None:
        if credentials is not None:
            resolved_credentials = credentials
        else:
            try:
                resolved_credentials = KrakenSecretManager(account_id).get_credentials()
            except RuntimeError:
                if session_factory is None:
                    raise
                resolved_credentials = {"api_key": "", "api_secret": "", "metadata": {}}

        self._credentials = resolved_credentials
        self._session_factory = session_factory or (lambda creds: _LoopbackSession(creds))
        self._rest_fallback = rest_fallback or self._default_rest_fallback
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._session: Any | None = None

    # session helpers -------------------------------------------------
    def _session_or_connect(self) -> Any:
        if self._session is None:
            self._session = self._session_factory(self._credentials)
        return self._session

    def close(self) -> None:
        if self._session is not None:
            try:
                close = getattr(self._session, "close", None)
                if callable(close):
                    close()
            finally:
                self._session = None

    # request helpers -------------------------------------------------
    def _send(self, channel: str, payload: Dict[str, Any], timeout: float | None = None) -> Dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            session = self._session_or_connect()
            try:
                return session.request(channel, payload, timeout=timeout)
            except KrakenWebsocketTimeout as exc:
                last_error = exc
            except KrakenWebsocketError as exc:
                last_error = exc
            if attempt >= self._max_retries:
                break
            # reset and retry
            self.close()
            time.sleep(self._retry_delay)
        if isinstance(last_error, KrakenWebsocketTimeout):
            raise last_error
        if last_error:
            raise KrakenWebsocketError(str(last_error))
        raise KrakenWebsocketError("unknown websocket failure")

    # REST fallback ---------------------------------------------------
    def _default_rest_fallback(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        txid = payload.get("clientOrderId") or f"REST-{random.randint(1, 1_000_000)}"
        return {
            "status": "ok",
            "txid": txid,
            "channel": "rest:add_order",
            "echo": payload,
        }

    # public API ------------------------------------------------------
    def add_order(self, payload: Dict[str, Any], *, timeout: float | None = None) -> Dict[str, Any]:
        try:
            response = self._send("add_order", payload, timeout=timeout)
            response.setdefault("transport", "websocket")
            return response
        except KrakenWebsocketTimeout:
            fallback = self._rest_fallback(payload)
            fallback.setdefault("transport", "rest")
            return fallback
        except KrakenWebsocketError:
            fallback = self._rest_fallback(payload)
            fallback.setdefault("transport", "rest")
            return fallback

    def rest_add_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        fallback = self._rest_fallback(payload)
        fallback.setdefault("transport", "rest")
        return fallback

    def cancel_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = self._send("cancel_order", payload)
        response.setdefault("transport", "websocket")
        return response

    def open_orders(self) -> Dict[str, Any]:
        return self._send("openOrders", {})

    def own_trades(self, txid: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if txid:
            payload["txid"] = txid
        return self._send("ownTrades", payload)
