from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Tuple

from fastapi import APIRouter, FastAPI, HTTPException

from services.oms.kraken_rest import KrakenRESTError
from services.oms.kraken_ws import KrakenWSError, KrakenWSTimeout, OrderState

logger = logging.getLogger(__name__)

ZERO = Decimal("0")


router = APIRouter()


@dataclass
class ReconcileStats:
    orders_checked: int = 0
    mismatches_fixed: int = 0


class OMSReconciler:
    """Periodically compares Kraken snapshots with OMS local state."""

    def __init__(self, manager: "OMSManager", interval: float = 60.0) -> None:
        self._manager = manager
        self._interval = max(interval, 1.0)
        self._stats = ReconcileStats()
        self._stats_lock = asyncio.Lock()
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run_loop(), name="oms-reconcile-loop")

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def status(self) -> Dict[str, int]:
        async with self._stats_lock:
            return {
                "orders_checked": self._stats.orders_checked,
                "mismatches_fixed": self._stats.mismatches_fixed,
            }

    async def _run_loop(self) -> None:
        try:
            while True:
                try:
                    await self._run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.warning("OMS reconciliation iteration failed: %s", exc)
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            logger.debug("OMS reconciliation loop cancelled")
            raise

    async def _run_once(self) -> None:
        accounts = await self._manager.list_accounts()
        if not accounts:
            return

        total_checked = 0
        total_fixed = 0

        for account in accounts:
            try:
                checked, fixed = await self._reconcile_account(account)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning(
                    "Reconciliation failed for account %s: %s",
                    getattr(account, "account_id", "unknown"),
                    exc,
                )
                continue
            total_checked += checked
            total_fixed += fixed

        if total_checked or total_fixed:
            async with self._stats_lock:
                self._stats.orders_checked += total_checked
                self._stats.mismatches_fixed += total_fixed

    async def _reconcile_account(self, account: "AccountContext") -> Tuple[int, int]:
        await account.start()

        trades_payload = await self._fetch_trades(account)
        trade_mismatches = await self._apply_trade_mismatches(account, trades_payload)

        orders_payload = await self._fetch_open_orders(account)
        order_mismatches = await self._apply_order_mismatches(account, orders_payload)

        checked = len(trades_payload) + len(orders_payload)
        fixed = trade_mismatches + order_mismatches

        return checked, fixed

    async def _fetch_open_orders(self, account: "AccountContext") -> List[Dict[str, object]]:
        orders: List[Dict[str, object]] = []
        if account.ws_client is not None:
            try:
                snapshot = await account.ws_client.fetch_open_orders_snapshot()
                orders.extend(order for order in snapshot if isinstance(order, dict))
            except (KrakenWSError, KrakenWSTimeout) as exc:
                logger.warning(
                    "Reconcile open orders via websocket failed for account %s: %s",
                    account.account_id,
                    exc,
                )

        if not orders and account.rest_client is not None:
            try:
                payload = await account.rest_client.open_orders()
            except KrakenRESTError as exc:
                logger.warning(
                    "Reconcile open orders via REST failed for account %s: %s",
                    account.account_id,
                    exc,
                )
            else:
                orders.extend(account._parse_rest_open_orders(payload))

        return orders

    async def _fetch_trades(self, account: "AccountContext") -> List[Dict[str, object]]:
        trades: List[Dict[str, object]] = []
        if account.ws_client is not None:
            try:
                snapshot = await account.ws_client.fetch_own_trades_snapshot()
                trades.extend(trade for trade in snapshot if isinstance(trade, dict))
            except (KrakenWSError, KrakenWSTimeout) as exc:
                logger.warning(
                    "Reconcile own trades via websocket failed for account %s: %s",
                    account.account_id,
                    exc,
                )

        if not trades and account.rest_client is not None:
            try:
                payload = await account.rest_client.own_trades()
            except KrakenRESTError as exc:
                logger.warning(
                    "Reconcile own trades via REST failed for account %s: %s",
                    account.account_id,
                    exc,
                )
            else:
                trades.extend(account._parse_rest_trades(payload))

        return trades

    async def _apply_trade_mismatches(
        self,
        account: "AccountContext",
        trades: Iterable[Dict[str, object]],
    ) -> int:
        mismatches = 0
        for trade in trades:
            state = account._state_from_payload(
                trade,
                default_status="filled",
                transport="reconcile",
            )
            if state is None or not state.client_order_id:
                continue

            record = await account.lookup(state.client_order_id)
            if record is None:
                parent = account._child_parent.get(state.client_order_id)
                if parent:
                    record = await account.lookup(parent)

            filled_remote = _to_decimal(state.filled_qty)
            avg_remote = _to_decimal(state.avg_price)

            needs_update = False
            if record is None:
                needs_update = True
            else:
                filled_local = record.result.filled_qty
                avg_local = record.result.avg_price
                if filled_remote > filled_local:
                    needs_update = True
                elif filled_remote > ZERO and avg_remote != ZERO and avg_remote != avg_local:
                    needs_update = True

            if needs_update:
                applied = await account.apply_fill_event(trade)
                if applied:
                    mismatches += 1
                    logger.info(
                        "Reconciler applied fill update for %s on account %s",
                        state.client_order_id,
                        account.account_id,
                    )

        return mismatches

    async def _apply_order_mismatches(
        self,
        account: "AccountContext",
        orders: Iterable[Dict[str, object]],
    ) -> int:
        mismatches = 0

        remote_states: List[OrderState] = []
        for order in orders:
            state = account._state_from_payload(
                order,
                default_status="open",
                transport="reconcile",
            )
            if state is not None and state.client_order_id:
                remote_states.append(state)

        remote_ids = {state.client_order_id for state in remote_states if state.client_order_id}

        for state in remote_states:
            client_id = state.client_order_id
            if client_id is None:
                continue
            record = await account.lookup(client_id)
            if record is None:
                parent = account._child_parent.get(client_id)
                if parent:
                    record = await account.lookup(parent)

            filled_remote = _to_decimal(state.filled_qty)
            avg_remote = _to_decimal(state.avg_price)
            normalized_remote = _normalize_status(state.status)

            needs_update = False
            if record is None:
                needs_update = True
            else:
                normalized_local = _normalize_status(record.result.status)
                if normalized_local != normalized_remote:
                    needs_update = True
                elif record.result.filled_qty != filled_remote:
                    needs_update = True
                elif (
                    filled_remote > ZERO
                    and avg_remote != ZERO
                    and record.result.avg_price != avg_remote
                ):
                    needs_update = True

            if needs_update:
                mismatches += 1
                await account._apply_stream_state(state)
                logger.info(
                    "Reconciler corrected order %s on account %s (status=%s)",
                    client_id,
                    account.account_id,
                    state.status,
                )

        local_open_ids = await _collect_open_local_orders(account)
        stale_orders = [order_id for order_id in local_open_ids if order_id not in remote_ids]

        for order_id in stale_orders:
            record = await account.lookup(order_id)
            if record is None:
                continue
            mismatches += 1
            state = OrderState(
                client_order_id=order_id,
                exchange_order_id=record.result.exchange_order_id,
                status="closed",
                filled_qty=float(record.result.filled_qty),
                avg_price=float(record.result.avg_price),
                errors=None,
                transport="reconcile",
            )
            await account._apply_stream_state(state)
            logger.info(
                "Reconciler closed stale local order %s on account %s",
                order_id,
                account.account_id,
            )

        return mismatches


def _normalize_status(value: Optional[str]) -> str:
    if value is None:
        return ""
    return value.strip().lower()


def _to_decimal(value: Optional[float]) -> Decimal:
    if value is None:
        return ZERO
    return Decimal(str(value))


async def _collect_open_local_orders(account: "AccountContext") -> List[str]:
    open_statuses = {"open", "pending", "accepted", "new"}
    closed_statuses = {"closed", "filled", "canceled", "cancelled", "rejected"}
    async with account._orders_lock:  # type: ignore[attr-defined]
        open_ids: List[str] = []
        for client_id, record in account._orders.items():  # type: ignore[attr-defined]
            status = _normalize_status(record.result.status)
            if status in closed_statuses:
                continue
            if status in open_statuses or status:
                open_ids.append(client_id)
        return open_ids


_RECONCILER: Optional[OMSReconciler] = None


@router.get("/oms/reconcile/status")
async def reconcile_status() -> Dict[str, int]:
    if _RECONCILER is None:
        raise HTTPException(status_code=503, detail="Reconciler not initialised")
    return await _RECONCILER.status()


def register(app: FastAPI, manager: "OMSManager", interval: float = 60.0) -> OMSReconciler:
    global _RECONCILER
    reconciler = OMSReconciler(manager, interval=interval)
    _RECONCILER = reconciler
    app.add_event_handler("startup", reconciler.start)
    app.add_event_handler("shutdown", reconciler.stop)
    app.include_router(router)
    return reconciler


# Local import for type checking without creating circular dependencies at runtime.
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from services.oms.oms_service import AccountContext, OMSManager

