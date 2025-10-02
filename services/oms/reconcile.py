from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from fastapi import APIRouter, FastAPI, HTTPException

from services.alert_manager import OMSError, get_alert_manager_instance
from services.oms.kraken_rest import KrakenRESTError
from services.oms.kraken_ws import KrakenWSError, KrakenWSTimeout, OrderState

logger = logging.getLogger(__name__)

ZERO = Decimal("0")


router = APIRouter()


@dataclass
class ReconcileStats:
    balances_checked: int = 0
    orders_checked: int = 0
    mismatches_fixed: int = 0


@dataclass
class AccountReconcileResult:
    balances_checked: int = 0
    orders_checked: int = 0
    mismatches_fixed: int = 0
    mismatches_remaining: int = 0


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
                "balances_checked": self._stats.balances_checked,
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

        total_balances = 0
        total_orders = 0
        total_fixed = 0

        for account in accounts:
            try:
                result = await self._reconcile_account(account)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning(
                    "Reconciliation failed for account %s: %s",
                    getattr(account, "account_id", "unknown"),
                    exc,
                )
                continue

            total_balances += result.balances_checked
            total_orders += result.orders_checked
            total_fixed += result.mismatches_fixed

            timestamp = datetime.now(timezone.utc).isoformat()
            logger.info(
                "reconcile_log(%s, %s, mismatches=%s)",
                timestamp,
                account.account_id,
                result.mismatches_fixed,
            )

            if result.mismatches_remaining:
                self._raise_alert(account.account_id, result.mismatches_remaining)

        if total_balances or total_orders or total_fixed:
            async with self._stats_lock:
                self._stats.balances_checked += total_balances
                self._stats.orders_checked += total_orders
                self._stats.mismatches_fixed += total_fixed

    async def _reconcile_account(self, account: "AccountContext") -> AccountReconcileResult:
        await account.start()

        balances_payload = await self._fetch_balances(account)
        orders_payload, has_remote_snapshot = await self._fetch_open_orders(account)

        balances_checked = len(balances_payload)
        orders_checked = len(orders_payload)

        balance_fixed, balance_remaining = await self._reconcile_balances(account, balances_payload)
        order_fixed, order_remaining = await self._reconcile_orders(
            account, orders_payload, has_remote_snapshot
        )

        return AccountReconcileResult(
            balances_checked=balances_checked,
            orders_checked=orders_checked,
            mismatches_fixed=balance_fixed + order_fixed,
            mismatches_remaining=balance_remaining + order_remaining,
        )

    async def _fetch_balances(self, account: "AccountContext") -> Dict[str, Decimal]:
        balances: Dict[str, Decimal] = {}
        if account.rest_client is None:
            return balances
        try:
            payload = await account.rest_client.balance()
        except KrakenRESTError as exc:
            logger.warning(
                "Reconcile balances via REST failed for account %s: %s",
                account.account_id,
                exc,
            )
            return balances
        return account._parse_rest_balances(payload)

    async def _reconcile_balances(
        self, account: "AccountContext", remote_balances: Dict[str, Decimal]
    ) -> Tuple[int, int]:
        if not remote_balances:
            return 0, 0

        local_balances = await account.get_local_balances()
        mismatches = _diff_balances(local_balances, remote_balances)
        if mismatches:
            await account.update_local_balances(remote_balances)

        updated_local = await account.get_local_balances()
        remaining = _diff_balances(updated_local, remote_balances)
        return len(mismatches), len(remaining)

    async def _fetch_open_orders(
        self, account: "AccountContext"
    ) -> Tuple[List[Dict[str, object]], bool]:
        orders: List[Dict[str, object]] = []
        authoritative = False
        if account.ws_client is not None:
            try:
                snapshot = await account.ws_client.fetch_open_orders_snapshot()
            except (KrakenWSError, KrakenWSTimeout) as exc:
                logger.warning(
                    "Reconcile open orders via websocket failed for account %s: %s",
                    account.account_id,
                    exc,
                )
            else:
                orders.extend(order for order in snapshot if isinstance(order, dict))
                authoritative = True

        if not authoritative and account.rest_client is not None:
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
                authoritative = True

        return orders, authoritative

    async def _reconcile_orders(
        self,
        account: "AccountContext",
        orders: Iterable[Dict[str, object]],
        snapshot_authoritative: bool,
    ) -> Tuple[int, int]:
        remote_states: List[OrderState] = []
        mismatches_fixed = 0

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
                mismatches_fixed += 1
                await account._apply_stream_state(state)
                logger.info(
                    "Reconciler corrected order %s on account %s (status=%s)",
                    client_id,
                    account.account_id,
                    state.status,
                )

        if snapshot_authoritative:
            local_open_ids = await _collect_open_local_orders(account)
            stale_orders = [
                order_id for order_id in local_open_ids if order_id not in remote_ids
            ]

            for order_id in stale_orders:
                record = await account.lookup(order_id)
                if record is None:
                    continue
                mismatches_fixed += 1
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
        else:
            logger.debug(
                "Skipping stale-order pruning for account %s due to missing snapshot",
                account.account_id,
            )

        remaining = await _count_order_mismatches(
            account, remote_states, remote_ids, snapshot_authoritative
        )
        return mismatches_fixed, remaining

    def _raise_alert(self, account_id: str, mismatches: int) -> None:
        manager = get_alert_manager_instance()
        description = (
            f"Persistent reconciliation mismatch for account {account_id} after corrective actions"
        )
        labels = {"account_id": account_id, "mismatches": str(mismatches)}
        error = OMSError(
            error_code="OMS_RECONCILE_MISMATCH",
            description=description,
            severity="critical",
            labels=labels,
        )
        if manager is not None:
            manager.handle_oms_error(error)
        else:  # pragma: no cover - depends on Alertmanager configuration
            logger.error("%s (mismatches=%s)", description, mismatches)


def _normalize_status(value: Optional[str]) -> str:
    if value is None:
        return ""
    return value.strip().lower()


def _to_decimal(value: Optional[float]) -> Decimal:
    if value is None:
        return ZERO
    return Decimal(str(value))


def _diff_balances(
    local: Dict[str, Decimal], remote: Dict[str, Decimal], tolerance: Decimal = Decimal("0")
) -> List[str]:
    mismatches: List[str] = []
    for asset, remote_amount in remote.items():
        local_amount = local.get(asset)
        if local_amount is None:
            mismatches.append(asset)
            continue
        if abs(local_amount - remote_amount) > tolerance:
            mismatches.append(asset)
    for asset in local:
        if asset not in remote:
            mismatches.append(asset)
    return mismatches


async def _count_order_mismatches(
    account: "AccountContext",
    remote_states: Sequence[OrderState],
    remote_ids: Iterable[Optional[str]],
    snapshot_authoritative: bool,
) -> int:
    remaining = 0
    remote_id_set = {order_id for order_id in remote_ids if order_id}

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

        if record is None:
            remaining += 1
            continue

        normalized_local = _normalize_status(record.result.status)
        if normalized_local != normalized_remote:
            remaining += 1
            continue
        if record.result.filled_qty != filled_remote:
            remaining += 1
            continue
        if (
            filled_remote > ZERO
            and avg_remote != ZERO
            and record.result.avg_price != avg_remote
        ):
            remaining += 1
            continue

    if snapshot_authoritative:
        local_open_ids = await _collect_open_local_orders(account)
        for order_id in local_open_ids:
            if order_id not in remote_id_set:
                remaining += 1

    return remaining


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
