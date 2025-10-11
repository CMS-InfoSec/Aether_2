"""Shared spot-market adapter scaffolding mirroring the Kraken implementation."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional
from uuid import uuid4

from exchange_adapter import ExchangeAdapter
from shared.spot import is_spot_symbol, normalize_spot_symbol


def _resolve_multi_exchange_enabled() -> bool:
    flag = os.getenv("ENABLE_MULTI_EXCHANGE_ROUTING", "")
    return flag.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_url(candidate: Optional[str], env_var: str, default: str = "") -> str:
    if candidate:
        return str(candidate).strip()
    env_value = os.getenv(env_var, default)
    return env_value.strip()


class KrakenSpotAdapter(ExchangeAdapter):
    """Base class for spot adapters that provides Kraken-aligned behaviour."""

    def __init__(
        self,
        name: str,
        *,
        rest_url: Optional[str] = None,
        websocket_url: Optional[str] = None,
    ) -> None:
        super().__init__(
            name,
            capabilities={
                "place_order": True,
                "cancel_order": True,
            },
        )
        self._rest_url = rest_url or ""
        self._websocket_url = websocket_url or ""
        self._connected = False

    @staticmethod
    def resolve_url(candidate: Optional[str], env_var: str, default: str = "") -> str:
        """Expose the internal URL resolution helper for subclasses."""

        return _resolve_url(candidate, env_var, default)

    @staticmethod
    def multi_exchange_enabled() -> bool:
        """Return ``True`` when cross-exchange routing is enabled."""

        return _resolve_multi_exchange_enabled()

    def supports(self, operation: str) -> bool:  # pragma: no cover - trivial override
        if operation in {"place_order", "cancel_order"} and not self.multi_exchange_enabled():
            return False
        return super().supports(operation)

    async def connect(self) -> Mapping[str, Any]:
        self._connected = True
        return {
            "exchange": self.name,
            "status": "connected",
            "rest_url": self._rest_url or None,
            "websocket_url": self._websocket_url or None,
            "trading_enabled": self.multi_exchange_enabled(),
        }

    async def fetch_orderbook(
        self,
        symbol: str,
        *,
        depth: int = 10,
    ) -> Mapping[str, Any]:
        normalized = self._require_spot_symbol(symbol)
        return {
            "exchange": self.name,
            "symbol": normalized,
            "depth": max(1, int(depth)),
            "bids": [],
            "asks": [],
            "as_of": datetime.now(timezone.utc).isoformat(),
        }

    async def submit_order(
        self,
        account_id: str,
        payload: Mapping[str, Any],
        *,
        shadow: bool = False,
    ) -> Mapping[str, Any]:
        normalized = self._require_spot_symbol(
            payload.get("symbol")
            or payload.get("instrument")
            or payload.get("instrument_id")
            or payload.get("pair")
        )
        client_id = str(
            payload.get("client_id")
            or payload.get("client_order_id")
            or uuid4()
        )
        ack: Dict[str, Any] = {
            "exchange": self.name,
            "account_id": account_id,
            "symbol": normalized,
            "client_order_id": client_id,
            "shadow": bool(shadow),
        }
        if self.multi_exchange_enabled():
            ack.update({"status": "accepted", "note": "stubbed_execution"})
        else:
            ack.update({"status": "noop", "reason": "multi_exchange_routing_disabled"})
        return ack

    async def cancel_order(
        self,
        account_id: str,
        client_id: str,
        *,
        exchange_order_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        ack: Dict[str, Any] = {
            "exchange": self.name,
            "account_id": account_id,
            "client_order_id": client_id,
            "exchange_order_id": exchange_order_id,
        }
        if self.multi_exchange_enabled():
            ack.update({"status": "accepted", "note": "stubbed_cancellation"})
        else:
            ack.update({"status": "noop", "reason": "multi_exchange_routing_disabled"})
        return ack

    async def place_order(
        self,
        account_id: str,
        payload: Mapping[str, Any],
        *,
        shadow: bool = False,
    ) -> Mapping[str, Any]:
        return await self.submit_order(account_id, payload, shadow=shadow)

    async def get_balance(self, account_id: str) -> Mapping[str, Any]:  # pragma: no cover - trivial stub
        return {
            "exchange": self.name,
            "account_id": account_id,
            "balances": {},
        }

    async def get_trades(
        self,
        account_id: str,
        *,
        limit: int = 50,
    ) -> List[Mapping[str, Any]]:  # pragma: no cover - trivial stub
        return []

    async def status(self) -> Mapping[str, Any]:
        return {
            "exchange": self.name,
            "available": True,
            "connected": self._connected,
            "rest_url": self._rest_url or None,
            "websocket_url": self._websocket_url or None,
            "trading_enabled": self.multi_exchange_enabled(),
        }

    @staticmethod
    def _require_spot_symbol(symbol: object) -> str:
        normalized = normalize_spot_symbol(symbol)
        if not normalized or not is_spot_symbol(normalized):
            raise ValueError("Only USD spot market instruments are supported")
        return normalized


__all__ = ["KrakenSpotAdapter", "_resolve_url", "_resolve_multi_exchange_enabled"]
