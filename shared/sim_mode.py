"""Shared utilities for managing platform-wide simulation mode state.

This module now dynamically selects between the full SQLAlchemy-backed
implementation (which lives in :mod:`shared._sim_mode_sqlalchemy`) and a
lightweight in-memory fallback that keeps the rest of the codebase importable
when SQLAlchemy is unavailable.  The public API remains identical in both
scenarios so services can continue to rely on :mod:`shared.sim_mode` without
sprinkling dependency guards throughout their modules.
"""

from __future__ import annotations

from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

_SQLALCHEMY_FALLBACK = False
try:  # pragma: no cover - executed when SQLAlchemy (or the shim) is available
    import sqlalchemy as _sqlalchemy_module  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - executed when SQLAlchemy import fails outright
    _SQLALCHEMY_FALLBACK = True
else:
    _SQLALCHEMY_FALLBACK = bool(getattr(_sqlalchemy_module, "__aether_stub__", False))

if not _SQLALCHEMY_FALLBACK:
    from ._sim_mode_sqlalchemy import *  # noqa: F401,F403
    from ._sim_mode_sqlalchemy import __all__ as __all__
else:  # pragma: no cover - exercised in dependency-light environments
    import asyncio
    import logging
    import math
    import time
    from dataclasses import dataclass, field
    from datetime import datetime, timezone
    from decimal import Decimal
    from functools import partial
    from itertools import count
    from threading import Lock
    from typing import Dict, Iterable, List, Optional, Tuple

    from common.schemas.contracts import FillEvent
    from shared.async_utils import dispatch_async
    from shared.event_bus import KafkaNATSAdapter

    LOGGER = logging.getLogger(__name__)

    _STATE_CACHE_TTL = 2.0

    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)

    @dataclass
    class SimModeStatus:
        account_id: str
        active: bool
        reason: Optional[str]
        ts: datetime

    @dataclass
    class SimModeStateORM:
        account_id: str
        active: bool = False
        reason: Optional[str] = None
        ts: datetime = field(default_factory=_utcnow)

    @dataclass
    class SimBrokerOrderORM:
        id: int
        account_id: str
        client_id: str
        symbol: str
        side: str
        order_type: str
        qty: Decimal
        filled_qty: Decimal
        avg_price: Decimal
        status: str
        limit_px: Optional[Decimal] = None
        pre_trade_mid: Optional[Decimal] = None
        last_fill_ts: Optional[datetime] = None

    @dataclass
    class SimBrokerFillORM:
        order_id: int
        account_id: str
        client_id: str
        symbol: str
        qty: Decimal
        price: Decimal
        liquidity: str
        fee: Decimal
        ts: datetime

    @dataclass
    class SimPriceSnapshotORM:
        symbol: str
        price: Decimal
        updated_at: datetime

    class SimModeRepository:
        """In-memory simulation-mode store used when SQLAlchemy is unavailable."""

        def __init__(self) -> None:
            self._lock = Lock()
            self._cache: Dict[str, Tuple[SimModeStatus, float]] = {}
            self._state: Dict[str, SimModeStateORM] = {}

        def _load_row(self, account_id: str) -> SimModeStateORM:
            row = self._state.get(account_id)
            if row is None:
                row = SimModeStateORM(account_id=account_id)
                self._state[account_id] = row
            return row

        def get_status(self, account_id: str, *, use_cache: bool = True) -> SimModeStatus:
            if use_cache:
                with self._lock:
                    cached = self._cache.get(account_id)
                    if cached is not None:
                        status, expires = cached
                        if expires >= time.monotonic():
                            return status

            row = self._load_row(account_id)
            status = SimModeStatus(
                account_id=row.account_id,
                active=bool(row.active),
                reason=row.reason,
                ts=row.ts,
            )

            if use_cache:
                with self._lock:
                    self._cache[account_id] = (status, time.monotonic() + _STATE_CACHE_TTL)
            return status

        def get_many(self, account_ids: Iterable[str], *, use_cache: bool = True) -> List[SimModeStatus]:
            statuses: List[SimModeStatus] = []
            missing: List[str] = []
            now = time.monotonic()

            if use_cache:
                with self._lock:
                    for account_id in account_ids:
                        cached = self._cache.get(account_id)
                        if cached is not None:
                            status, expires = cached
                            if expires >= now:
                                statuses.append(status)
                                continue
                        missing.append(account_id)
            else:
                missing.extend(account_ids)

            for account_id in missing:
                row = self._load_row(account_id)
                status = SimModeStatus(
                    account_id=row.account_id,
                    active=bool(row.active),
                    reason=row.reason,
                    ts=row.ts,
                )
                statuses.append(status)
                if use_cache:
                    with self._lock:
                        self._cache[account_id] = (status, time.monotonic() + _STATE_CACHE_TTL)

            statuses.sort(key=lambda item: item.account_id)
            return statuses

        async def get_status_async(self, account_id: str, *, use_cache: bool = True) -> SimModeStatus:
            loop = asyncio.get_running_loop()
            func = partial(self.get_status, account_id, use_cache=use_cache)
            return await loop.run_in_executor(None, func)

        async def get_many_async(
            self, account_ids: Iterable[str], *, use_cache: bool = True
        ) -> List[SimModeStatus]:
            loop = asyncio.get_running_loop()
            func = partial(self.get_many, list(account_ids), use_cache=use_cache)
            return await loop.run_in_executor(None, func)

        def set_status(self, account_id: str, active: bool, reason: Optional[str]) -> SimModeStatus:
            row = self._load_row(account_id)
            row.active = active
            row.reason = reason
            row.ts = _utcnow()

            status = SimModeStatus(account_id=account_id, active=active, reason=reason, ts=row.ts)
            with self._lock:
                self._cache[account_id] = (status, time.monotonic() + _STATE_CACHE_TTL)
            return status

        async def set_status_async(
            self, account_id: str, active: bool, reason: Optional[str]
        ) -> SimModeStatus:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self.set_status, account_id, active, reason)

    sim_mode_repository = SimModeRepository()

    @dataclass
    class SimulatedOrderSnapshot:
        account_id: str
        client_id: str
        symbol: str
        side: str
        order_type: str
        qty: Decimal
        filled_qty: Decimal
        avg_price: Decimal
        status: str
        limit_px: Optional[Decimal]
        pre_trade_mid: Optional[Decimal]
        last_fill_ts: Optional[datetime]

    @dataclass
    class SimulatedExecution:
        snapshot: SimulatedOrderSnapshot
        fill_qty: Decimal
        fill_price: Decimal
        liquidity: str

    class MicrostructureSlippageModel:
        """Toy microstructure model returning slippage in basis points."""

        def estimate_bps(self, order_type: str, qty: Decimal) -> Decimal:
            base = Decimal("1.2") if order_type.lower() == "market" else Decimal("0.4")
            magnitude = Decimal(math.log10(max(float(qty), 1.0))) if qty > 0 else Decimal("0")
            size_penalty = Decimal("0.35") * magnitude
            return base + size_penalty

    class SimBroker:
        """In-memory execution simulator that mirrors the SQLAlchemy-backed API."""

        def __init__(self) -> None:
            self._lock = Lock()
            self._orders: Dict[Tuple[str, str], SimulatedOrderSnapshot] = {}
            self._prices: Dict[str, Decimal] = {}
            self._order_records: Dict[Tuple[str, str], SimBrokerOrderORM] = {}
            self._price_records: Dict[str, SimPriceSnapshotORM] = {}
            self._fills: List[SimBrokerFillORM] = []
            self._order_ids = count(1)
            self._slippage = MicrostructureSlippageModel()

        def _snapshot_from_order(self, row: SimBrokerOrderORM) -> SimulatedOrderSnapshot:
            return SimulatedOrderSnapshot(
                account_id=row.account_id,
                client_id=row.client_id,
                symbol=row.symbol,
                side=row.side,
                order_type=row.order_type,
                qty=row.qty,
                filled_qty=row.filled_qty,
                avg_price=row.avg_price,
                status=row.status,
                limit_px=row.limit_px,
                pre_trade_mid=row.pre_trade_mid,
                last_fill_ts=row.last_fill_ts,
            )

        def _persist_price(self, symbol: str, price: Decimal) -> None:
            snapshot = SimPriceSnapshotORM(symbol=symbol, price=price, updated_at=_utcnow())
            self._price_records[symbol] = snapshot
            self._prices[symbol] = price

        def _persist_order(self, snapshot: SimulatedOrderSnapshot) -> None:
            key = (snapshot.account_id, snapshot.client_id)
            existing = self._order_records.get(key)
            order_id = existing.id if existing is not None else next(self._order_ids)
            self._order_records[key] = SimBrokerOrderORM(
                id=order_id,
                account_id=snapshot.account_id,
                client_id=snapshot.client_id,
                symbol=snapshot.symbol,
                side=snapshot.side,
                order_type=snapshot.order_type,
                qty=snapshot.qty,
                filled_qty=snapshot.filled_qty,
                avg_price=snapshot.avg_price,
                status=snapshot.status,
                limit_px=snapshot.limit_px,
                pre_trade_mid=snapshot.pre_trade_mid,
                last_fill_ts=snapshot.last_fill_ts,
            )

        def _record_fill(
            self,
            snapshot: SimulatedOrderSnapshot,
            fill_qty: Decimal,
            fill_price: Decimal,
            liquidity: str,
        ) -> None:
            if fill_qty <= 0:
                return
            key = (snapshot.account_id, snapshot.client_id)
            order = self._order_records.get(key)
            order_id = order.id if order is not None else next(self._order_ids)
            self._fills.append(
                SimBrokerFillORM(
                    order_id=order_id,
                    account_id=snapshot.account_id,
                    client_id=snapshot.client_id,
                    symbol=snapshot.symbol,
                    qty=fill_qty,
                    price=fill_price,
                    liquidity=liquidity,
                    fee=Decimal("0"),
                    ts=_utcnow(),
                )
            )

        def _resolve_reference_price(
            self, symbol: str, pre_trade_mid: Optional[Decimal], limit_px: Optional[Decimal]
        ) -> Decimal:
            if pre_trade_mid and pre_trade_mid > 0:
                return pre_trade_mid
            if symbol in self._prices:
                return self._prices[symbol]
            if limit_px and limit_px > 0:
                return limit_px
            return Decimal("0")

        def _simulate_execution(
            self,
            account_id: str,
            client_id: str,
            symbol: str,
            side: str,
            order_type: str,
            qty: Decimal,
            limit_px: Optional[Decimal],
            pre_trade_mid: Optional[Decimal],
        ) -> SimulatedExecution:
            reference_price = self._resolve_reference_price(symbol, pre_trade_mid, limit_px)
            slippage_bps = self._slippage.estimate_bps(order_type, qty)
            direction = Decimal("1") if side.lower() == "buy" else Decimal("-1")
            adjustment = Decimal("1") + (slippage_bps / Decimal("10000")) * direction
            execution_price = reference_price * adjustment if reference_price > 0 else reference_price

            fill_fraction = Decimal("1")
            order_type_normalized = order_type.lower()
            if order_type_normalized == "limit" and limit_px is not None and reference_price > 0:
                if side.lower() == "buy" and limit_px < execution_price:
                    fill_fraction = Decimal("0.5")
                    execution_price = limit_px
                elif side.lower() == "sell" and limit_px > execution_price:
                    fill_fraction = Decimal("0.5")
                    execution_price = limit_px

            fill_qty = (qty * fill_fraction).quantize(Decimal("0.00000001"))
            remaining = qty - fill_qty
            avg_price = execution_price if fill_qty > 0 else Decimal("0")
            status = "filled" if remaining <= 0 else "partially_filled"
            liquidity = "taker" if order_type_normalized == "market" else (
                "maker" if fill_fraction < 1 else "taker"
            )

            snapshot = SimulatedOrderSnapshot(
                account_id=account_id,
                client_id=client_id,
                symbol=symbol,
                side=side,
                order_type=order_type,
                qty=qty,
                filled_qty=fill_qty,
                avg_price=avg_price,
                status=status,
                limit_px=limit_px,
                pre_trade_mid=pre_trade_mid,
                last_fill_ts=_utcnow(),
            )

            return SimulatedExecution(snapshot=snapshot, fill_qty=fill_qty, fill_price=avg_price, liquidity=liquidity)

        def place_order(
            self,
            account_id: str,
            client_id: str,
            symbol: str,
            side: str,
            order_type: str,
            qty: Decimal,
            limit_px: Optional[Decimal],
            pre_trade_mid: Optional[Decimal],
        ) -> SimulatedExecution:
            with self._lock:
                execution = self._simulate_execution(
                    account_id,
                    client_id,
                    symbol,
                    side,
                    order_type,
                    qty,
                    limit_px,
                    pre_trade_mid,
                )
                self._orders[(account_id, client_id)] = execution.snapshot
                if execution.fill_price > 0:
                    self._persist_price(symbol, execution.fill_price)
                self._persist_order(execution.snapshot)
                self._record_fill(execution.snapshot, execution.fill_qty, execution.fill_price, execution.liquidity)

            adapter = KafkaNATSAdapter(account_id=account_id)
            if execution.fill_qty > 0 and execution.fill_price > 0:
                event = FillEvent(
                    account_id=account_id,
                    symbol=symbol,
                    qty=float(execution.fill_qty),
                    price=float(execution.fill_price),
                    fee=0.0,
                    liquidity=execution.liquidity,
                    ts=_utcnow(),
                )
                payload: dict
                dumper = getattr(event, "model_dump", None)
                if callable(dumper):
                    try:
                        payload = dumper(mode="json")
                    except TypeError:
                        payload = dumper()
                else:
                    legacy = getattr(event, "dict", None)
                    payload = legacy() if callable(legacy) else dict(event.__dict__)

                dispatch_async(
                    adapter.publish("oms.fills.simulated", payload),
                    context="simulated fill event",
                    logger=LOGGER,
                )
            return execution

        def cancel_order(self, account_id: str, client_id: str) -> Optional[SimulatedOrderSnapshot]:
            with self._lock:
                snapshot = self._orders.get((account_id, client_id))
                if snapshot is None:
                    return None
                cancelled = SimulatedOrderSnapshot(
                    account_id=snapshot.account_id,
                    client_id=snapshot.client_id,
                    symbol=snapshot.symbol,
                    side=snapshot.side,
                    order_type=snapshot.order_type,
                    qty=snapshot.qty,
                    filled_qty=snapshot.filled_qty,
                    avg_price=snapshot.avg_price,
                    status="cancelled",
                    limit_px=snapshot.limit_px,
                    pre_trade_mid=snapshot.pre_trade_mid,
                    last_fill_ts=_utcnow(),
                )
                self._orders[(account_id, client_id)] = cancelled
                self._persist_order(cancelled)
                return cancelled

        def lookup(self, account_id: str, client_id: str) -> Optional[SimulatedOrderSnapshot]:
            with self._lock:
                snapshot = self._orders.get((account_id, client_id))
                if snapshot is not None:
                    return snapshot
                row = self._order_records.get((account_id, client_id))
                if row is None:
                    return None
                snapshot = self._snapshot_from_order(row)
                self._orders[(account_id, client_id)] = snapshot
                return snapshot

    sim_broker = SimBroker()

    __all__ = [
        "SimModeStatus",
        "SimModeRepository",
        "sim_mode_repository",
        "SimulatedOrderSnapshot",
        "SimulatedExecution",
        "sim_broker",
        "KafkaNATSAdapter",
    ]
