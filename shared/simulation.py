"""Shared utilities for toggling simulation mode and recording simulated orders."""
from __future__ import annotations

import asyncio
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Optional, Tuple

from services.oms.kraken_ws import OrderAck


@dataclass
class SimulatedOrder:
    """Representation of a simulated order acknowledgement."""

    ack: OrderAck
    payload: Dict[str, str]
    correlation_id: Optional[str]
    created_at: datetime
    transport: str = "simulation"

    @property
    def idempotency_key(self) -> Optional[str]:
        return self.payload.get("idempotencyKey")

    @property
    def client_order_id(self) -> Optional[str]:
        return self.payload.get("clientOrderId")


class SimBroker:
    """In-memory broker that fabricates deterministic acknowledgements."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._orders: Dict[Tuple[str, str], SimulatedOrder] = {}

    async def place_order(
        self,
        *,
        account_id: str,
        payload: Dict[str, str],
        correlation_id: Optional[str] = None,
    ) -> SimulatedOrder:
        """Return a simulated order acknowledgement for the supplied payload."""

        key = self._resolve_key(account_id, payload)
        async with self._lock:
            existing = self._orders.get(key)
            if existing is not None:
                return existing

            volume = Decimal(str(payload.get("volume", "0")))
            price = Decimal(str(payload.get("price", "0")))
            ack = OrderAck(
                exchange_order_id=f"SIM-{uuid.uuid4().hex[:12]}",
                status="filled",
                filled_qty=volume,
                avg_price=price,
                errors=None,
            )
            record = SimulatedOrder(
                ack=ack,
                payload=dict(payload),
                correlation_id=correlation_id,
                created_at=datetime.now(timezone.utc),
            )
            self._orders[key] = record
            return record

    def inspect(self, account_id: str, idempotency_key: str) -> Optional[SimulatedOrder]:
        """Return the cached simulated order for inspection in tests."""

        return self._orders.get((account_id, idempotency_key))

    async def clear(self) -> None:
        """Clear all cached orders."""

        async with self._lock:
            self._orders.clear()

    def _resolve_key(self, account_id: str, payload: Dict[str, str]) -> Tuple[str, str]:
        idempotency = payload.get("idempotencyKey") or payload.get("clientOrderId")
        if not idempotency:
            idempotency = uuid.uuid4().hex
        return account_id, idempotency


class SimModeState:
    """Thread-safe toggle indicating whether simulation mode is active."""

    def __init__(self) -> None:
        self._active = False
        self._lock = asyncio.Lock()

    @property
    def active(self) -> bool:
        return self._active

    def activate(self) -> None:
        self._active = True

    def deactivate(self) -> None:
        self._active = False

    async def set(self, value: bool) -> None:
        async with self._lock:
            self._active = value

    async def enable(self) -> None:
        await self.set(True)

    async def disable(self) -> None:
        await self.set(False)

    @contextmanager
    def override(self, value: bool):
        previous = self._active
        self._active = value
        try:
            yield self
        finally:
            self._active = previous


sim_mode_state = SimModeState()
sim_broker = SimBroker()

__all__ = ["SimBroker", "SimModeState", "SimulatedOrder", "sim_broker", "sim_mode_state"]
