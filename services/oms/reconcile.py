from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import httpx
from fastapi import APIRouter, FastAPI, HTTPException

from services.oms.kraken_rest import KrakenRESTError
from services.oms.kraken_ws import KrakenWSError, KrakenWSTimeout, OrderState
from services.oms.rate_limit_guard import rate_limit_guard

logger = logging.getLogger(__name__)

ZERO = Decimal("0")


router = APIRouter()


try:  # pragma: no cover - psycopg may be unavailable in lightweight tests
    import psycopg
except Exception:  # pragma: no cover - allow graceful degradation when psycopg missing
    psycopg = None  # type: ignore[assignment]


class ReconcileLogStore:
    """Minimal persistence layer for reconciliation outcomes."""

    _DEFAULT_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS reconcile_log (
            ts TIMESTAMPTZ NOT NULL,
            account_id TEXT NOT NULL,
            mismatches INTEGER NOT NULL DEFAULT 0,
            mismatches_json JSONB NOT NULL DEFAULT '{}'::jsonb
        )
    """

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn or os.getenv("OMS_RECONCILE_DSN") or os.getenv("TIMESCALE_DSN")
        self._lock = asyncio.Lock()
        self._memory_fallback: List[Tuple[datetime, str, int, str]] = []

        if psycopg is None or not self._dsn:
            if not self._dsn:
                logger.debug(
                    "ReconcileLogStore initialised without Timescale DSN; falling back to in-memory log"
                )
            else:
                logger.warning(
                    "psycopg is unavailable; OMS reconciliation log will operate in-memory only"
                )
            return

        self._ensure_schema()

    def _ensure_schema(self) -> None:
        assert self._dsn is not None
        assert psycopg is not None  # nosec - guarded by __init__

        with psycopg.connect(self._dsn, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(self._DEFAULT_TABLE_SQL)
                cursor.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = 'reconcile_log'
                    """
                )
                columns = {row[0] for row in cursor.fetchall()}
                if "mismatches" not in columns:
                    cursor.execute(
                        "ALTER TABLE reconcile_log ADD COLUMN mismatches INTEGER NOT NULL DEFAULT 0"
                    )
                if "mismatches_json" not in columns:
                    cursor.execute(
                        "ALTER TABLE reconcile_log ADD COLUMN mismatches_json JSONB NOT NULL DEFAULT '{}'::jsonb"
                    )

    async def record(
        self, account_id: str, mismatches: Dict[str, Sequence[str]]
    ) -> None:
        timestamp = datetime.now(timezone.utc)
        normalized = {
            key: [str(entry) for entry in sequence]
            for key, sequence in mismatches.items()
        }
        payload = json.dumps(normalized, sort_keys=True)
        mismatch_count = sum(len(entries) for entries in normalized.values())

        use_fallback = False
        async with self._lock:
            use_fallback = psycopg is None or not self._dsn
            if use_fallback:
                self._memory_fallback.append((timestamp, account_id, mismatch_count, payload))

        if use_fallback:
            return

        await asyncio.to_thread(
            self._record_sync,
            timestamp,
            account_id,
            mismatch_count,
            payload,
        )

    def _record_sync(
        self,
        timestamp: datetime,
        account_id: str,
        mismatch_count: int,
        payload: str,
    ) -> None:
        assert psycopg is not None and self._dsn is not None  # nosec - validated by caller

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO reconcile_log (ts, account_id, mismatches, mismatches_json)
                    VALUES (%s, %s, %s, %s::jsonb)
                    """,
                    (timestamp, account_id, mismatch_count, payload),
                )
            conn.commit()


@dataclass
class ReconcileStats:
    balances_checked: int = 0
    orders_checked: int = 0
    mismatches_fixed: int = 0
    alerts: int = 0


@dataclass
class AccountReconcileResult:
    balances_checked: int = 0
    orders_checked: int = 0
    balances_fixed: List[str] = field(default_factory=list)
    balances_remaining: List[str] = field(default_factory=list)
    orders_fixed: List[str] = field(default_factory=list)
    orders_remaining: List[str] = field(default_factory=list)

    @property
    def mismatches_fixed(self) -> int:
        return len(self.balances_fixed) + len(self.orders_fixed)

    @property
    def mismatches_remaining(self) -> int:
        return len(self.balances_remaining) + len(self.orders_remaining)

    def log_payload(self) -> Dict[str, Sequence[str]]:
        return {
            "balances_fixed": list(self.balances_fixed),
            "balances_remaining": list(self.balances_remaining),
            "orders_fixed": list(self.orders_fixed),
            "orders_remaining": list(self.orders_remaining),
        }


class OMSReconciler:
    """Periodically compares Kraken snapshots with OMS local state."""

    def __init__(
        self,
        manager: "OMSManager",
        *,
        interval: float = 60.0,
        alert_base_url: Optional[str] = None,
        log_store: Optional[ReconcileLogStore] = None,
    ) -> None:
        self._manager = manager
        self._interval = max(interval, 1.0)
        self._stats = ReconcileStats()
        self._stats_lock = asyncio.Lock()
        self._task: Optional[asyncio.Task[None]] = None
        self._log_store = log_store or ReconcileLogStore()
        base_url = alert_base_url or os.getenv("ALERTS_SERVICE_URL", "http://localhost:8000")
        self._alerts_base_url = base_url.rstrip("/")
        self._mismatch_counters: Dict[str, int] = {}
        self._alerted_accounts: set[str] = set()

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
                "alerts": self._stats.alerts,
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

            mismatch_payload = result.log_payload()
            await self._log_store.record(account.account_id, mismatch_payload)
            logger.info(
                "reconcile_log(%s, %s, mismatches=%s)",
                datetime.now(timezone.utc).isoformat(),
                account.account_id,
                mismatch_payload,
            )

            await self._handle_persistent_mismatch(
                account.account_id, result.mismatches_remaining
            )

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
            balances_fixed=balance_fixed,
            balances_remaining=balance_remaining,
            orders_fixed=order_fixed,
            orders_remaining=order_remaining,
        )

    async def _fetch_balances(self, account: "AccountContext") -> Dict[str, Decimal]:
        balances: Dict[str, Decimal] = {}
        if account.rest_client is None:
            return balances
        try:
            await rate_limit_guard.acquire(
                account.account_id,
                "/private/Balance",
                transport="rest",
            )
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
    ) -> Tuple[List[str], List[str]]:
        if not remote_balances:
            return [], []

        local_balances = await account.get_local_balances()
        mismatches = _diff_balances(local_balances, remote_balances)
        if mismatches:
            logger.info(
                "Reconciler updating balances for account %s mismatches=%s",
                account.account_id,
                mismatches,
            )
            await account.update_local_balances(remote_balances)

        updated_local = await account.get_local_balances()
        remaining = _diff_balances(updated_local, remote_balances)
        fixed_assets = [asset for asset in mismatches if asset not in remaining]
        return fixed_assets, remaining

    async def _fetch_open_orders(
        self, account: "AccountContext"
    ) -> Tuple[List[Dict[str, object]], bool]:
        orders: List[Dict[str, object]] = []
        authoritative = False
        if account.ws_client is not None:
            try:
                await rate_limit_guard.acquire(
                    account.account_id,
                    "open_orders_snapshot",
                    transport="websocket",
                )
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
                await rate_limit_guard.acquire(
                    account.account_id,
                    "/private/OpenOrders",
                    transport="rest",
                )
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
    ) -> Tuple[List[str], List[str]]:
        remote_states: List[OrderState] = []
        mismatches_fixed: List[str] = []

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
                mismatches_fixed.append(client_id)
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
                mismatches_fixed.append(order_id)
                state = OrderState(
                    client_order_id=order_id,
                    exchange_order_id=record.result.exchange_order_id,
                    status="closed",
                    filled_qty=record.result.filled_qty,
                    avg_price=record.result.avg_price,
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

        remaining = await _identify_order_mismatches(
            account, remote_states, remote_ids, snapshot_authoritative
        )
        return mismatches_fixed, remaining

    async def _handle_persistent_mismatch(self, account_id: str, remaining: int) -> None:
        if remaining > 0:
            count = self._mismatch_counters.get(account_id, 0) + 1
            self._mismatch_counters[account_id] = count
            if count > 3 and account_id not in self._alerted_accounts:
                if await self._publish_alert(account_id, remaining):
                    async with self._stats_lock:
                        self._stats.alerts += 1
                    self._alerted_accounts.add(account_id)
        else:
            self._mismatch_counters.pop(account_id, None)
            self._alerted_accounts.discard(account_id)

    async def _publish_alert(self, account_id: str, mismatches: int) -> bool:
        payload = {
            "account_id": account_id,
            "type": "oms_reconcile_mismatch",
            "mismatches": mismatches,
            "description": "Persistent OMS reconciliation mismatch detected",
        }
        url = f"{self._alerts_base_url}/alerts/publish"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
            logger.warning(
                "Published persistent mismatch alert for account %s mismatches=%s",
                account_id,
                mismatches,
            )
            return True
        except httpx.HTTPError as exc:  # pragma: no cover - defensive logging
            logger.warning(
                "Failed to publish reconcile alert for account %s: %s", account_id, exc
            )
            return False


def _normalize_status(value: Optional[str]) -> str:
    if value is None:
        return ""
    return value.strip().lower()


def _to_decimal(value: Optional[Decimal | float | int | str]) -> Decimal:
    if value is None:
        return ZERO
    if isinstance(value, Decimal):
        return value
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


async def _identify_order_mismatches(
    account: "AccountContext",
    remote_states: Sequence[OrderState],
    remote_ids: Iterable[Optional[str]],
    snapshot_authoritative: bool,
) -> List[str]:
    remaining: List[str] = []
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
            remaining.append(client_id or state.exchange_order_id or "unknown")
            continue

        normalized_local = _normalize_status(record.result.status)
        if normalized_local != normalized_remote:
            remaining.append(client_id)
            continue
        if record.result.filled_qty != filled_remote:
            remaining.append(client_id)
            continue
        if (
            filled_remote > ZERO
            and avg_remote != ZERO
            and record.result.avg_price != avg_remote
        ):
            remaining.append(client_id)
            continue

    if snapshot_authoritative:
        local_open_ids = await _collect_open_local_orders(account)
        for order_id in local_open_ids:
            if order_id not in remote_id_set:
                remaining.append(order_id)

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


def register(
    app: FastAPI,
    manager: "OMSManager",
    interval: float = 60.0,
    *,
    alert_base_url: Optional[str] = None,
    log_store: Optional[ReconcileLogStore] = None,
) -> OMSReconciler:
    global _RECONCILER
    reconciler = OMSReconciler(
        manager,
        interval=interval,
        alert_base_url=alert_base_url,
        log_store=log_store,
    )
    _RECONCILER = reconciler
    app.add_event_handler("startup", reconciler.start)
    app.add_event_handler("shutdown", reconciler.stop)
    app.include_router(router)
    return reconciler


# Local import for type checking without creating circular dependencies at runtime.
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from services.oms.oms_service import AccountContext, OMSManager
