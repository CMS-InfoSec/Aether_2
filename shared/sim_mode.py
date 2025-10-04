"""Shared utilities for managing platform-wide simulation mode state.

This module centralises persistence and caching for the simulation mode flag so
that multiple services (e.g. ``sim_mode.py`` FastAPI service and the OMS) can
coordinate behaviour.  State is backed by a PostgreSQL/SQLAlchemy table while a
lightweight in-memory cache keeps read traffic low.  The module also provides a
simple in-memory/DB-backed SimBroker implementation that the OMS can use when
simulation mode is active.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from functools import partial
from threading import Lock
from typing import Dict, Iterator, Optional, Tuple

from sqlalchemy import Boolean, Column, DateTime, Integer, Numeric, String, Text, create_engine, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from common.schemas.contracts import FillEvent
from services.common.adapters import KafkaNATSAdapter


LOGGER = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _database_url() -> str:
    candidates = [os.getenv("SIM_MODE_DATABASE_URL"), os.getenv("DATABASE_URL")]
    for candidate in candidates:
        if not candidate:
            continue
        normalized = candidate
        if normalized.startswith("postgres://"):
            normalized = "postgresql://" + normalized.split("://", 1)[1]
        try:
            url_obj = make_url(normalized)
        except Exception as exc:  # pragma: no cover - defensive validation
            raise RuntimeError("Invalid simulation mode database URL") from exc

        driver = url_obj.drivername.lower()
        if driver.startswith("postgresql") or driver.startswith("timescale"):
            return str(url_obj)
        raise RuntimeError(
            "Simulation mode requires a PostgreSQL/TimescaleDB DSN; "
            f"received driver '{url_obj.drivername}'."
        )
    raise RuntimeError(
        "SIM_MODE_DATABASE_URL (or DATABASE_URL) must be set to a PostgreSQL/TimescaleDB DSN."
    )


def _engine() -> Engine:
    url = _database_url()
    url_obj = make_url(url)

    engine_kwargs = {"future": True, "pool_pre_ping": True}
    connect_args: Dict[str, object] = {}

    driver = url_obj.drivername.lower()
    if driver.startswith("postgresql") or "timescale" in driver:
        if "sslmode" not in url_obj.query:
            connect_args["sslmode"] = os.getenv("SIM_MODE_DB_SSLMODE", "require")
        engine_kwargs.update(
            pool_size=int(os.getenv("SIM_MODE_DB_POOL_SIZE", "10")),
            max_overflow=int(os.getenv("SIM_MODE_DB_MAX_OVERFLOW", "10")),
            pool_timeout=int(os.getenv("SIM_MODE_DB_POOL_TIMEOUT", "30")),
            pool_recycle=int(os.getenv("SIM_MODE_DB_POOL_RECYCLE", "1800")),
        )
    else:  # pragma: no cover - only exercised in tests with alternative engines
        connect_args["check_same_thread"] = False

    if connect_args:
        engine_kwargs["connect_args"] = connect_args

    return create_engine(url, **engine_kwargs)


ENGINE = _engine()
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)


class Base(DeclarativeBase):
    pass


class SimModeStateORM(Base):
    __tablename__ = "sim_mode_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    active = Column(Boolean, nullable=False, default=False)
    reason = Column(Text, nullable=True)
    ts = Column(DateTime(timezone=True), nullable=False, default=_utcnow, server_default=text("CURRENT_TIMESTAMP"))


class SimBrokerOrderORM(Base):
    __tablename__ = "sim_broker_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(128), nullable=False, index=True)
    client_id = Column(String(128), nullable=False, index=True)
    symbol = Column(String(64), nullable=False)
    side = Column(String(8), nullable=False)
    order_type = Column(String(16), nullable=False)
    qty = Column(Numeric(36, 18), nullable=False)
    filled_qty = Column(Numeric(36, 18), nullable=False, default=Decimal("0"))
    avg_price = Column(Numeric(36, 18), nullable=False, default=Decimal("0"))
    limit_px = Column(Numeric(36, 18), nullable=True)
    status = Column(String(32), nullable=False, default="open")
    pre_trade_mid = Column(Numeric(36, 18), nullable=True)
    last_fill_ts = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utcnow, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow, server_default=text("CURRENT_TIMESTAMP"))


class SimBrokerFillORM(Base):
    __tablename__ = "sim_broker_fills"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, nullable=False, index=True)
    account_id = Column(String(128), nullable=False, index=True)
    client_id = Column(String(128), nullable=False)
    symbol = Column(String(64), nullable=False)
    qty = Column(Numeric(36, 18), nullable=False)
    price = Column(Numeric(36, 18), nullable=False)
    liquidity = Column(String(16), nullable=False)
    fee = Column(Numeric(36, 18), nullable=False, default=Decimal("0"))
    ts = Column(DateTime(timezone=True), nullable=False, default=_utcnow, server_default=text("CURRENT_TIMESTAMP"))


class SimPriceSnapshotORM(Base):
    __tablename__ = "sim_price_snapshots"

    symbol = Column(String(64), primary_key=True)
    price = Column(Numeric(36, 18), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow, server_default=text("CURRENT_TIMESTAMP"))


Base.metadata.create_all(bind=ENGINE)


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:  # pragma: no cover - defensive cleanup
        session.rollback()
        raise
    finally:
        session.close()


@dataclass(frozen=True)
class SimModeStatus:
    active: bool
    reason: Optional[str]
    ts: datetime


class SimModeRepository:
    """Persistence and caching layer for the simulation mode flag."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._cache: Tuple[SimModeStatus, float] | None = None
        self._cache_ttl = 2.0
        self._ensure_row()

    def _ensure_row(self) -> None:
        with session_scope() as session:
            row = session.execute(select(SimModeStateORM).limit(1)).scalar_one_or_none()
            if row is None:
                session.add(SimModeStateORM(active=False, reason=None, ts=_utcnow()))

    def get_status(self, *, use_cache: bool = True) -> SimModeStatus:
        if use_cache:
            with self._lock:
                if self._cache is not None:
                    status, expires = self._cache
                    if expires >= time.monotonic():
                        return status

        with session_scope() as session:
            row = session.execute(select(SimModeStateORM).limit(1)).scalar_one()
            status = SimModeStatus(active=bool(row.active), reason=row.reason, ts=row.ts)

        if use_cache:
            with self._lock:
                expires = time.monotonic() + self._cache_ttl
                self._cache = (status, expires)
        return status

    async def get_status_async(self, *, use_cache: bool = True) -> SimModeStatus:
        loop = asyncio.get_running_loop()
        func = partial(self.get_status, use_cache=use_cache)
        return await loop.run_in_executor(None, func)

    def set_status(self, active: bool, reason: Optional[str]) -> SimModeStatus:
        now = _utcnow()
        with session_scope() as session:
            row = session.execute(select(SimModeStateORM).limit(1)).scalar_one()
            row.active = active
            row.reason = reason
            row.ts = now
            session.add(row)
        status = SimModeStatus(active=active, reason=reason, ts=now)
        with self._lock:
            expires = time.monotonic() + self._cache_ttl
            self._cache = (status, expires)
        return status

    async def set_status_async(self, active: bool, reason: Optional[str]) -> SimModeStatus:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.set_status, active, reason)


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
    """Simple in-memory/DB backed execution simulator used during sim mode."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._orders: Dict[Tuple[str, str], SimulatedOrderSnapshot] = {}
        self._prices: Dict[str, Decimal] = {}
        self._slippage = MicrostructureSlippageModel()
        self._load_state()

    def _load_state(self) -> None:
        with session_scope() as session:
            rows = session.execute(select(SimBrokerOrderORM)).scalars().all()
            for row in rows:
                snapshot = SimulatedOrderSnapshot(
                    account_id=row.account_id,
                    client_id=row.client_id,
                    symbol=row.symbol,
                    side=row.side,
                    order_type=row.order_type,
                    qty=Decimal(row.qty),
                    filled_qty=Decimal(row.filled_qty),
                    avg_price=Decimal(row.avg_price),
                    status=row.status,
                    limit_px=Decimal(row.limit_px) if row.limit_px is not None else None,
                    pre_trade_mid=Decimal(row.pre_trade_mid) if row.pre_trade_mid is not None else None,
                    last_fill_ts=row.last_fill_ts,
                )
                self._orders[(row.account_id, row.client_id)] = snapshot
            price_rows = session.execute(select(SimPriceSnapshotORM)).scalars().all()
            for price in price_rows:
                self._prices[price.symbol] = Decimal(price.price)

    def _persist_price(self, symbol: str, price: Decimal) -> None:
        with session_scope() as session:
            existing = session.get(SimPriceSnapshotORM, symbol)
            if existing is None:
                session.add(SimPriceSnapshotORM(symbol=symbol, price=price, updated_at=_utcnow()))
            else:
                existing.price = price
                existing.updated_at = _utcnow()
                session.add(existing)

    def _persist_order(self, snapshot: SimulatedOrderSnapshot) -> None:
        with session_scope() as session:
            row = session.execute(
                select(SimBrokerOrderORM).where(
                    SimBrokerOrderORM.account_id == snapshot.account_id,
                    SimBrokerOrderORM.client_id == snapshot.client_id,
                )
            ).scalar_one_or_none()
            if row is None:
                row = SimBrokerOrderORM(
                    account_id=snapshot.account_id,
                    client_id=snapshot.client_id,
                    symbol=snapshot.symbol,
                    side=snapshot.side,
                    order_type=snapshot.order_type,
                    qty=snapshot.qty,
                    filled_qty=snapshot.filled_qty,
                    avg_price=snapshot.avg_price,
                    limit_px=snapshot.limit_px,
                    status=snapshot.status,
                    pre_trade_mid=snapshot.pre_trade_mid,
                    last_fill_ts=snapshot.last_fill_ts,
                )
            else:
                row.qty = snapshot.qty
                row.filled_qty = snapshot.filled_qty
                row.avg_price = snapshot.avg_price
                row.limit_px = snapshot.limit_px
                row.status = snapshot.status
                row.pre_trade_mid = snapshot.pre_trade_mid
                row.last_fill_ts = snapshot.last_fill_ts
            session.add(row)

    def _record_fill(
        self,
        snapshot: SimulatedOrderSnapshot,
        fill_qty: Decimal,
        fill_price: Decimal,
        liquidity: str,
    ) -> None:
        if fill_qty <= 0:
            return
        with session_scope() as session:
            row = session.execute(
                select(SimBrokerOrderORM).where(
                    SimBrokerOrderORM.account_id == snapshot.account_id,
                    SimBrokerOrderORM.client_id == snapshot.client_id,
                )
            ).scalar_one_or_none()
            order_id = row.id if row is not None else None
            session.add(
                SimBrokerFillORM(
                    order_id=order_id or 0,
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
        liquidity = "taker" if order_type_normalized == "market" else ("maker" if fill_fraction < 1 else "taker")

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
                self._prices[symbol] = execution.fill_price
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
            asyncio.run(adapter.publish("oms.fills.simulated", event.model_dump(mode="json")))
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
        with session_scope() as session:
            row = session.execute(
                select(SimBrokerOrderORM).where(
                    SimBrokerOrderORM.account_id == account_id,
                    SimBrokerOrderORM.client_id == client_id,
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            snapshot = SimulatedOrderSnapshot(
                account_id=row.account_id,
                client_id=row.client_id,
                symbol=row.symbol,
                side=row.side,
                order_type=row.order_type,
                qty=Decimal(row.qty),
                filled_qty=Decimal(row.filled_qty),
                avg_price=Decimal(row.avg_price),
                status=row.status,
                limit_px=Decimal(row.limit_px) if row.limit_px is not None else None,
                pre_trade_mid=Decimal(row.pre_trade_mid) if row.pre_trade_mid is not None else None,
                last_fill_ts=row.last_fill_ts,
            )
        with self._lock:
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
]

