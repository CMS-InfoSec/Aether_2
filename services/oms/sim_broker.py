"""Simulated broker for order placement in offline environments.

The :class:`SimBroker` implements a lightweight execution model that mirrors
the behaviour of the production OMS closely enough for unit tests and local
development. Orders are persisted to a dedicated ``sim_orders`` table (or an
in-memory fallback when TimescaleDB is unavailable) and fills are persisted to
``sim_fills`` while also being mirrored into the regular OMS fill telemetry
with a ``simulated`` flag.

Market orders are filled immediately using a simple slippage model based on
the configured baseline plus contributions from spread and short-term
volatility. Limit orders are filled when the simulated market trades through
the price and support partial fills based on a provided liquidity estimate.

Kafka ``OrderEvent`` and ``FillEvent`` payloads are published using the
``KafkaNATSAdapter`` so downstream components observe the same lifecycle events
as they would from the real OMS.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import logging
from threading import Lock
from typing import Any, Dict, Iterable, List, Mapping, Optional
from uuid import uuid4

from common.schemas.contracts import FillEvent, OrderEvent
from config.simulation import SimulationConfig, get_simulation_config
from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.common.config import get_timescale_session
from shared.async_utils import dispatch_async

try:  # pragma: no cover - optional dependency during CI
    import psycopg
    from psycopg import sql
except Exception:  # pragma: no cover - gracefully degrade without psycopg
    psycopg = None  # type: ignore
    sql = None  # type: ignore


LOGGER = logging.getLogger(__name__)


@dataclass
class SimulatedFill:
    """Container for simulated fill details."""

    fill_id: str
    order_id: str
    quantity: Decimal
    price: Decimal
    liquidity: str
    ts: datetime


@dataclass
class SimulatedOrder:
    """State tracked for every simulated order."""

    order_id: str
    client_order_id: str
    symbol: str
    side: str
    order_type: str
    quantity: Decimal
    remaining_qty: Decimal
    limit_price: Optional[Decimal]
    status: str
    created_at: datetime
    updated_at: datetime
    filled_qty: Decimal = Decimal("0")
    avg_price: Decimal = Decimal("0")
    metadata: Dict[str, Any] = field(default_factory=dict)
    fills: List[SimulatedFill] = field(default_factory=list)


class SimBroker:
    """Simulated broker supporting market and limit orders."""

    def __init__(
        self,
        account_id: str,
        *,
        kafka_factory: type[KafkaNATSAdapter] = KafkaNATSAdapter,
        timescale_factory: type[TimescaleAdapter] = TimescaleAdapter,
    ) -> None:
        self.account_id = account_id
        self._lock = Lock()
        self._orders: Dict[str, SimulatedOrder] = {}
        self._memory_orders: Dict[str, Dict[str, Any]] = {}
        self._memory_fills: List[Dict[str, Any]] = []
        self._kafka = kafka_factory(account_id=account_id)
        self._timescale = timescale_factory(account_id=account_id)
        self._config: SimulationConfig = get_simulation_config(account_id)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def place_order(
        self,
        *,
        client_order_id: str,
        symbol: str,
        side: str,
        quantity: float | Decimal,
        order_type: str,
        market_data: Mapping[str, Any],
        limit_price: float | Decimal | None = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Place a simulated order and return the resulting execution summary."""

        normalized_side = self._normalize_side(side)
        normalized_type = self._normalize_type(order_type)

        limit = Decimal(str(limit_price)) if limit_price is not None else None
        quantity_decimal = Decimal(str(quantity))
        now = datetime.now(timezone.utc)
        order = SimulatedOrder(
            order_id=str(uuid4()),
            client_order_id=client_order_id,
            symbol=symbol,
            side=normalized_side,
            order_type=normalized_type,
            quantity=quantity_decimal,
            remaining_qty=quantity_decimal,
            limit_price=limit,
            status="open",
            created_at=now,
            updated_at=now,
            metadata=dict(metadata or {}),
        )

        with self._lock:
            self._orders[order.order_id] = order
            self._persist_order(order)

        self._emit_order_event(order, status="open")

        if normalized_type == "market":
            self._fill_market_order(order, market_data)
        else:
            self._fill_limit_order(order, market_data)

        response = self._format_order_response(order)
        self._persist_order(order)
        return response

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open simulated order."""

        with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                return False
            if order.status in {"filled", "cancelled"}:
                return False
            order.status = "cancelled"
            order.updated_at = datetime.now(timezone.utc)
            self._persist_order(order)

        self._emit_order_event(order, status="cancelled")
        return True

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """Return open or partially filled orders."""

        with self._lock:
            orders = [
                self._format_order_response(order)
                for order in self._orders.values()
                if order.status in {"open", "partial"}
            ]
        return orders

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fill_market_order(
        self, order: SimulatedOrder, market_data: Mapping[str, Any]
    ) -> None:
        qty = order.remaining_qty
        if qty <= 0:
            return
        price = self._apply_slippage(order, market_data)
        self._apply_fill(order, qty, price, liquidity="taker")

    def _fill_limit_order(
        self, order: SimulatedOrder, market_data: Mapping[str, Any]
    ) -> None:
        if order.limit_price is None:
            return

        if not self._limit_touched(order, market_data):
            return

        available_liquidity = Decimal(
            str(market_data.get("available_liquidity", order.remaining_qty))
        )
        if available_liquidity <= 0:
            return

        qty = min(order.remaining_qty, available_liquidity)
        price = self._limit_execution_price(order, market_data)
        liquidity = "maker"
        if qty < order.remaining_qty:
            order.status = "partial"
        self._apply_fill(order, qty, price, liquidity)

    def _apply_fill(
        self, order: SimulatedOrder, qty: Decimal, price: Decimal, liquidity: str
    ) -> None:
        qty = qty.quantize(Decimal("0.00000001"))
        price = price.quantize(Decimal("0.00000001"))
        prior_filled = order.filled_qty
        remaining = order.remaining_qty

        if qty <= 0 or remaining <= 0:
            return

        new_filled = prior_filled + qty
        if new_filled <= 0:
            return

        avg_price = (
            (order.avg_price * prior_filled + price * qty) / new_filled
            if new_filled > 0
            else Decimal("0")
        )

        order.filled_qty = new_filled
        order.remaining_qty = max(Decimal("0"), remaining - qty)
        order.avg_price = avg_price
        order.status = "filled" if order.remaining_qty == 0 else "partial"
        order.updated_at = datetime.now(timezone.utc)

        fill = SimulatedFill(
            fill_id=str(uuid4()),
            order_id=order.order_id,
            quantity=qty,
            price=price,
            liquidity=liquidity,
            ts=order.updated_at,
        )
        order.fills.append(fill)

        self._persist_order(order)
        self._persist_fill(order, fill)
        self._emit_fill_event(order, fill)

        if order.status == "filled":
            self._emit_order_event(order, status="filled")

    def _format_order_response(self, order: SimulatedOrder) -> Dict[str, Any]:
        return {
            "order_id": order.order_id,
            "client_order_id": order.client_order_id,
            "symbol": order.symbol,
            "side": order.side,
            "order_type": order.order_type,
            "status": order.status,
            "quantity": float(order.quantity),
            "filled_qty": float(order.filled_qty),
            "remaining_qty": float(order.remaining_qty),
            "avg_price": float(order.avg_price) if order.filled_qty > 0 else 0.0,
            "limit_price": float(order.limit_price) if order.limit_price else None,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
            "fills": [
                {
                    "fill_id": fill.fill_id,
                    "qty": float(fill.quantity),
                    "price": float(fill.price),
                    "liquidity": fill.liquidity,
                    "ts": fill.ts,
                    "simulated": True,
                }
                for fill in order.fills
            ],
        }

    def _emit_order_event(self, order: SimulatedOrder, *, status: str) -> None:
        event = OrderEvent(
            account_id=self.account_id,
            symbol=order.symbol,
            order_id=order.order_id,
            status=status,
            ts=datetime.now(timezone.utc),
        )
        payload = event.model_dump(mode="json")
        payload["simulated"] = True
        payload["client_order_id"] = order.client_order_id
        dispatch_async(
            self._kafka.publish(topic="oms.simulated.orders", payload=payload),
            context="simulated order event",
            logger=LOGGER,
        )

    def _emit_fill_event(self, order: SimulatedOrder, fill: SimulatedFill) -> None:
        event = FillEvent(
            account_id=self.account_id,
            symbol=order.symbol,
            qty=float(fill.quantity),
            price=float(fill.price),
            fee=0.0,
            liquidity=fill.liquidity,
            ts=fill.ts,
        )
        payload = event.model_dump(mode="json")
        payload["simulated"] = True
        payload["order_id"] = order.order_id
        payload["client_order_id"] = order.client_order_id
        dispatch_async(
            self._kafka.publish(topic="oms.simulated.fills", payload=payload),
            context="simulated fill event",
            logger=LOGGER,
        )

    def _persist_order(self, order: SimulatedOrder) -> None:
        record = {
            "order_id": order.order_id,
            "account_id": self.account_id,
            "client_order_id": order.client_order_id,
            "symbol": order.symbol,
            "side": order.side,
            "order_type": order.order_type,
            "quantity": float(order.quantity),
            "limit_price": float(order.limit_price) if order.limit_price else None,
            "status": order.status,
            "filled_qty": float(order.filled_qty),
            "avg_price": float(order.avg_price) if order.filled_qty > 0 else 0.0,
            "metadata": json.dumps(order.metadata) if order.metadata else None,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
        }
        self._memory_orders[order.order_id] = record

        if psycopg is None or sql is None:  # pragma: no cover - DB unavailable
            return

        session = get_timescale_session(self.account_id)
        try:
            with psycopg.connect(session.dsn, autocommit=True) as conn:
                self._ensure_schema(conn, session.account_schema)
                insert_sql = sql.SQL(
                    """
                    INSERT INTO {}.sim_orders (
                        order_id,
                        account_id,
                        client_order_id,
                        symbol,
                        side,
                        order_type,
                        quantity,
                        limit_price,
                        status,
                        filled_qty,
                        avg_price,
                        metadata,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %(order_id)s,
                        %(account_id)s,
                        %(client_order_id)s,
                        %(symbol)s,
                        %(side)s,
                        %(order_type)s,
                        %(quantity)s,
                        %(limit_price)s,
                        %(status)s,
                        %(filled_qty)s,
                        %(avg_price)s,
                        %(metadata)s,
                        %(created_at)s,
                        %(updated_at)s
                    )
                    ON CONFLICT (order_id) DO UPDATE
                    SET
                        status = EXCLUDED.status,
                        filled_qty = EXCLUDED.filled_qty,
                        avg_price = EXCLUDED.avg_price,
                        updated_at = EXCLUDED.updated_at
                """
                ).format(sql.Identifier(session.account_schema))
                with conn.cursor() as cursor:
                    cursor.execute(insert_sql, record)
        except Exception as exc:  # pragma: no cover - logging only
            LOGGER.warning("Failed to persist simulated order %s: %s", order.order_id, exc)

    def _persist_fill(self, order: SimulatedOrder, fill: SimulatedFill) -> None:
        payload = {
            "fill_id": fill.fill_id,
            "order_id": order.order_id,
            "account_id": self.account_id,
            "symbol": order.symbol,
            "side": order.side,
            "quantity": float(fill.quantity),
            "price": float(fill.price),
            "liquidity": fill.liquidity,
            "fee": 0.0,
            "simulated": True,
            "ts": fill.ts,
        }
        self._memory_fills.append(payload)

        # Mirror to standard fill telemetry for downstream consumers.
        self._timescale.record_fill(payload)

        if psycopg is None or sql is None:  # pragma: no cover - DB unavailable
            return

        session = get_timescale_session(self.account_id)
        try:
            with psycopg.connect(session.dsn, autocommit=True) as conn:
                self._ensure_schema(conn, session.account_schema)
                insert_sql = sql.SQL(
                    """
                    INSERT INTO {}.sim_fills (
                        fill_id,
                        order_id,
                        account_id,
                        symbol,
                        side,
                        quantity,
                        price,
                        liquidity,
                        fee,
                        simulated,
                        ts
                    )
                    VALUES (
                        %(fill_id)s,
                        %(order_id)s,
                        %(account_id)s,
                        %(symbol)s,
                        %(side)s,
                        %(quantity)s,
                        %(price)s,
                        %(liquidity)s,
                        %(fee)s,
                        %(simulated)s,
                        %(ts)s
                    )
                    ON CONFLICT (fill_id) DO NOTHING
                """
                ).format(sql.Identifier(session.account_schema))
                with conn.cursor() as cursor:
                    cursor.execute(insert_sql, payload)
        except Exception as exc:  # pragma: no cover - logging only
            LOGGER.warning("Failed to persist simulated fill %s: %s", fill.fill_id, exc)

    # ------------------------------------------------------------------
    # Market model helpers
    # ------------------------------------------------------------------
    def _apply_slippage(
        self, order: SimulatedOrder, market_data: Mapping[str, Any]
    ) -> Decimal:
        last_price = Decimal(str(market_data.get("last_price", order.limit_price or 0)))
        if last_price <= 0:
            last_price = Decimal("0")

        spread_bps = self._extract_bps(market_data, ["spread_bps"]) or self._derive_spread_bps(
            market_data
        )
        volatility_bps = self._extract_bps(
            market_data, ["volatility_bps", "volatility"]
        )
        slippage_bps = (
            self._config.base_bps
            + 0.5 * spread_bps
            + self._config.vol_multiplier * volatility_bps
        )
        adjustment = Decimal(str(slippage_bps / 10_000.0))
        if order.side == "buy":
            price = last_price * (Decimal("1") + adjustment)
        else:
            price = last_price * (Decimal("1") - adjustment)
        return price

    def _limit_touched(
        self, order: SimulatedOrder, market_data: Mapping[str, Any]
    ) -> bool:
        limit = order.limit_price
        if limit is None:
            return False

        last_price = market_data.get("last_price")
        low = market_data.get("low")
        high = market_data.get("high")
        best_bid = market_data.get("best_bid") or market_data.get("bid")
        best_ask = market_data.get("best_ask") or market_data.get("ask")

        def _to_decimal(value: Any) -> Optional[Decimal]:
            if value is None:
                return None
            try:
                return Decimal(str(value))
            except Exception:  # pragma: no cover - defensive
                return None

        last = _to_decimal(last_price)
        low_px = _to_decimal(low)
        high_px = _to_decimal(high)
        bid_px = _to_decimal(best_bid)
        ask_px = _to_decimal(best_ask)

        if order.side == "buy":
            candidates: Iterable[Decimal] = [
                px for px in [last, low_px, ask_px] if px is not None
            ]
            return any(px <= limit for px in candidates)

        candidates = [px for px in [last, high_px, bid_px] if px is not None]
        return any(px >= limit for px in candidates)

    def _limit_execution_price(
        self, order: SimulatedOrder, market_data: Mapping[str, Any]
    ) -> Decimal:
        limit = order.limit_price or Decimal("0")

        best_bid = market_data.get("best_bid") or market_data.get("bid")
        best_ask = market_data.get("best_ask") or market_data.get("ask")

        if order.side == "buy":
            best = Decimal(str(best_ask)) if best_ask is not None else limit
            return min(limit, best)

        best = Decimal(str(best_bid)) if best_bid is not None else limit
        return max(limit, best)

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_side(side: str) -> str:
        normalized = side.lower()
        if normalized not in {"buy", "sell"}:
            raise ValueError("side must be BUY or SELL")
        return normalized

    @staticmethod
    def _normalize_type(order_type: str) -> str:
        normalized = order_type.lower()
        if normalized not in {"market", "limit"}:
            raise ValueError("order_type must be market or limit")
        return normalized

    @staticmethod
    def _extract_bps(market_data: Mapping[str, Any], keys: Iterable[str]) -> float:
        for key in keys:
            value = market_data.get(key)
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            if key == "volatility" and numeric < 1:
                # Treat volatility expressed as a ratio instead of bps.
                numeric *= 10_000.0
            return max(numeric, 0.0)
        return 0.0

    @staticmethod
    def _derive_spread_bps(market_data: Mapping[str, Any]) -> float:
        bid = market_data.get("bid") or market_data.get("best_bid")
        ask = market_data.get("ask") or market_data.get("best_ask")
        if bid is None or ask is None:
            return 0.0
        try:
            bid_val = float(bid)
            ask_val = float(ask)
        except (TypeError, ValueError):
            return 0.0
        if bid_val <= 0 or ask_val <= 0:
            return 0.0
        mid = (bid_val + ask_val) / 2.0
        if mid <= 0:
            return 0.0
        return abs(ask_val - bid_val) / mid * 10_000.0

    def _ensure_schema(self, conn: Any, schema: str) -> None:
        if psycopg is None or sql is None:  # pragma: no cover - DB unavailable
            return

        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    "CREATE SCHEMA IF NOT EXISTS {}"  # noqa: S608 - identifier comes from config
                ).format(sql.Identifier(schema))
            )
            cursor.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.sim_orders (
                        order_id TEXT PRIMARY KEY,
                        account_id TEXT NOT NULL,
                        client_order_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        order_type TEXT NOT NULL,
                        quantity DOUBLE PRECISION NOT NULL,
                        limit_price DOUBLE PRECISION,
                        status TEXT NOT NULL,
                        filled_qty DOUBLE PRECISION NOT NULL,
                        avg_price DOUBLE PRECISION NOT NULL,
                        metadata JSONB,
                        created_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL
                    )
                """
                ).format(sql.Identifier(schema))
            )
            cursor.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.sim_fills (
                        fill_id TEXT PRIMARY KEY,
                        order_id TEXT NOT NULL,
                        account_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        quantity DOUBLE PRECISION NOT NULL,
                        price DOUBLE PRECISION NOT NULL,
                        liquidity TEXT NOT NULL,
                        fee DOUBLE PRECISION NOT NULL,
                        simulated BOOLEAN NOT NULL,
                        ts TIMESTAMPTZ NOT NULL
                    )
                """
                ).format(sql.Identifier(schema))
            )


__all__ = ["SimBroker"]
