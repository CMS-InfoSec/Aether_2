from __future__ import annotations

import logging
import os
import asyncio
from contextlib import suppress
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import time
from typing import Any, Dict, List

from fastapi import Depends, FastAPI, HTTPException, status

from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.common.schemas import OrderPlacementRequest, OrderPlacementResponse
from services.common.security import require_admin_account
from services.oms.kraken_client import (
    KrakenCredentialExpired,
    KrakenWSClient,
    KrakenWebsocketError,
)
from services.oms.rate_limit_guard import rate_limit_guard
from services.oms.shadow_oms import shadow_oms
from shared.graceful_shutdown import flush_logging_handlers, setup_graceful_shutdown

from metrics import (
    increment_trade_rejection,
    record_oms_submit_ack,
    record_ws_latency,
    setup_metrics,
)

SHUTDOWN_TIMEOUT = float(
    os.getenv("OMS_SHUTDOWN_TIMEOUT", os.getenv("SERVICE_SHUTDOWN_TIMEOUT", "60.0"))
)

app = FastAPI(title="OMS Service")
setup_metrics(app)


logger = logging.getLogger(__name__)


shutdown_manager = setup_graceful_shutdown(
    app,
    service_name="oms-core",
    allowed_paths={"/", "/docs", "/openapi.json"},
    shutdown_timeout=SHUTDOWN_TIMEOUT,
    logger_instance=logger,
)


def _flush_adapters() -> None:
    """Flush buffered adapter state before terminating."""

    flush_logging_handlers("", __name__)

    kafka_counts = KafkaNATSAdapter.flush_events()
    if kafka_counts:
        logger.info("Flushed Kafka/NATS buffers", extra={"event_counts": kafka_counts})

    timescale_summary = TimescaleAdapter.flush_event_buffers()
    if timescale_summary:
        logger.info(
            "Flushed Timescale buffers", extra={"bucket_counts": timescale_summary}
        )


shutdown_manager.register_flush_callback(_flush_adapters)


async def _await_background_tasks(timeout: float) -> None:
    """Wait for OMS async tasks (rate limits, websocket/rest) to settle."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(timeout, 0.0)

    remaining = max(deadline - loop.time(), 0.0)
    if remaining > 0:
        guard_completed = await rate_limit_guard.wait_for_idle(timeout=remaining)
        if not guard_completed:
            logger.warning("Timed out waiting for rate limit guard to drain")

    remaining = max(deadline - loop.time(), 0.0)
    if remaining <= 0:
        return

    current_task = asyncio.current_task(loop=loop)
    pending: List[asyncio.Task[Any]] = []
    for task in asyncio.all_tasks(loop):
        if task is current_task or task.done():
            continue
        coro = task.get_coro()
        code = getattr(coro, "cr_code", None) or getattr(coro, "gi_code", None)
        if not code:
            continue
        filename = getattr(code, "co_filename", "")
        if "/services/oms/" not in filename.replace("\\", "/"):
            continue
        pending.append(task)

    if not pending:
        return

    done, still_pending = await asyncio.wait(
        pending,
        timeout=remaining,
        return_when=asyncio.ALL_COMPLETED,
    )

    for task in done:
        with suppress(Exception):
            task.result()

    if still_pending:
        for task in still_pending:
            logger.warning(
                "Background OMS task pending during shutdown", extra={"task": task.get_name()}
            )


@app.on_event("shutdown")
async def _on_shutdown_complete() -> None:
    await _await_background_tasks(shutdown_manager.shutdown_timeout)
    _flush_adapters()


class CircuitBreaker:
    _halts: Dict[str, Dict[str, float | str]] = {}

    @classmethod
    def halt(cls, instrument: str, reason: str, ttl_seconds: float | None = None) -> None:
        expires = float("inf") if ttl_seconds is None else time.time() + ttl_seconds
        cls._halts[instrument] = {"reason": reason, "expires": expires}

    @classmethod
    def resume(cls, instrument: str) -> None:
        cls._halts.pop(instrument, None)

    @classmethod
    def reset(cls) -> None:
        cls._halts.clear()

    @classmethod
    def is_halted(cls, instrument: str) -> bool:
        data = cls._halts.get(instrument)
        if not data:
            return False
        expires = data.get("expires", float("inf"))
        if expires != float("inf") and expires < time.time():
            cls._halts.pop(instrument, None)
            return False
        return True

    @classmethod
    def reason(cls, instrument: str) -> str | None:
        data = cls._halts.get(instrument)
        return None if not data else str(data.get("reason"))


MARKET_METADATA: Dict[str, Dict[str, float]] = {
    "BTC-USD": {"tick": 0.1, "lot": 0.0001},
    "ETH-USD": {"tick": 0.01, "lot": 0.001},
    "SOL-USD": {"tick": 0.001, "lot": 0.01},
}


def _snap(value: float, step: float) -> float:
    if step <= 0:
        return value
    quant = Decimal(str(step))
    snapped = (Decimal(str(value)) / quant).to_integral_value(rounding=ROUND_HALF_UP) * quant
    return float(snapped)


def _kraken_flags(request: OrderPlacementRequest) -> List[str]:
    flags: List[str] = []
    if request.post_only:
        flags.append("post")
    if request.reduce_only:
        flags.append("reduce_only")
    return flags


@app.post("/oms/place", response_model=OrderPlacementResponse)
def place_order(
    request: OrderPlacementRequest,
    account_id: str = Depends(require_admin_account),
) -> OrderPlacementResponse:
    if request.account_id != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload.",
        )

    if not request.instrument.endswith("USD"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only USD-quoted instruments are currently supported.",
        )

    if CircuitBreaker.is_halted(request.instrument):
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail=CircuitBreaker.reason(request.instrument) or "Trading halted",
        )

    metadata = MARKET_METADATA.get(request.instrument, {"tick": 0.01, "lot": 0.0001})
    snapped_price = _snap(request.price, metadata["tick"])
    snapped_quantity = _snap(request.quantity, metadata["lot"])

    if snapped_price <= 0 or snapped_quantity <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Snapped price/quantity must be positive.",
        )

    kafka = KafkaNATSAdapter(account_id=account_id)
    timescale = TimescaleAdapter(account_id=account_id)
    client = KrakenWSClient(account_id=account_id)

    order_payload = {
        "clientOrderId": request.order_id,
        "pair": request.instrument.replace("-", "/"),
        "type": "buy" if request.side == "BUY" else "sell",
        "ordertype": "limit",
        "price": snapped_price,
        "volume": snapped_quantity,
        "oflags": ",".join(_kraken_flags(request)),
    }
    if request.time_in_force:
        order_payload["timeInForce"] = request.time_in_force
    if request.take_profit:
        order_payload["takeProfit"] = request.take_profit
    if request.stop_loss:
        order_payload["stopLoss"] = request.stop_loss

    kafka.publish(
        topic="oms.orders",
        payload={
            "order_id": request.order_id,
            "instrument": request.instrument,
            "side": request.side,
            "quantity": snapped_quantity,
            "price": snapped_price,
        },
    )

    start_time = time.perf_counter()
    try:
        ack = client.add_order(order_payload, timeout=1.0)
    except KrakenCredentialExpired as exc:
        client.close()
        increment_trade_rejection(account_id, request.instrument)
        logger.warning(
            "Rejected order due to expired Kraken credentials",
            extra={"account_id": account_id, "instrument": request.instrument},
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(exc),
        ) from exc
    except KrakenWebsocketError as exc:
        client.close()
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc))

    ack_latency_ms = (time.perf_counter() - start_time) * 1000.0
    record_ws_latency(account_id, request.instrument, ack_latency_ms)
    record_oms_submit_ack(account_id, request.instrument, ack_latency_ms)

    open_snapshot = client.open_orders()
    trades_snapshot = client.own_trades(txid=ack.get("txid"))
    client.close()

    ack_payload = {
        "order_id": request.order_id,
        "txid": ack.get("txid"),
        "status": ack.get("status", "ok"),
        "transport": ack.get("transport", "websocket"),
        "price": snapped_price,
        "quantity": snapped_quantity,
        "flags": order_payload["oflags"],
        "open_orders": open_snapshot.get("open", []),
    }
    timescale.record_ack(ack_payload)
    timescale.record_usage(snapped_price * snapped_quantity)

    kafka.publish(topic="oms.acks", payload=ack_payload)

    status_value = str(ack_payload.get("status", "")).lower()
    if status_value and status_value not in {"ok", "accepted", "open"}:
        increment_trade_rejection(account_id, request.instrument)

    for trade in trades_snapshot.get("trades", []):
        fill_payload = {
            "order_id": request.order_id,
            "txid": ack.get("txid"),
            "price": trade.get("price", snapped_price),
            "quantity": trade.get("quantity", snapped_quantity),
            "liquidity": trade.get("liquidity", "maker" if request.post_only else "taker"),
        }
        timescale.record_fill(fill_payload)
        kafka.publish(topic="oms.executions", payload=fill_payload)

        trade_side = str(trade.get("side", request.side)).lower()
        trade_qty = Decimal(str(trade.get("quantity", snapped_quantity)))
        trade_price = Decimal(str(trade.get("price", snapped_price)))
        trade_ts: datetime | None = None
        raw_ts = trade.get("time")
        if raw_ts is not None:
            try:
                trade_ts = datetime.fromtimestamp(float(raw_ts), tz=timezone.utc)
            except (TypeError, ValueError):
                trade_ts = None
        shadow_oms.record_real_fill(
            account_id=account_id,
            symbol=request.instrument,
            side=trade_side,
            quantity=trade_qty,
            price=trade_price,
            timestamp=trade_ts,
            fee=float(trade.get("fee", 0.0) or 0.0),
            slippage_bps=float(trade.get("slippage_bps", 0.0) or 0.0),
        )

    shadow_fills = shadow_oms.generate_shadow_fills(
        account_id=account_id,
        symbol=request.instrument,
        side=request.side,
        quantity=Decimal(str(snapped_quantity)),
        price=Decimal(str(snapped_price)),
        timestamp=datetime.now(timezone.utc),
    )
    for shadow_fill in shadow_fills:
        timescale.record_shadow_fill(shadow_fill)

    venue = "kraken"
    return OrderPlacementResponse(accepted=True, routed_venue=venue, fee=request.fee)


@app.get("/oms/shadow_pnl")
def get_shadow_pnl(
    account_id: str,
    header_account: str = Depends(require_admin_account),
) -> Dict[str, Any]:
    if account_id != header_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload.",
        )
    return shadow_oms.snapshot(account_id)

