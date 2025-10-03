from __future__ import annotations

import asyncio
import importlib
import logging
import os
import sys
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timezone

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import time
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Tuple

from fastapi import Depends, FastAPI, HTTPException, status

from auth.service import (
    InMemorySessionStore,
    RedisSessionStore,
    SessionStoreProtocol,
)

from services.common import security

from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.common.schemas import OrderPlacementRequest, OrderPlacementResponse
from services.common.security import require_admin_account
from services.oms.kraken_client import (
    KrakenCredentialExpired,
    KrakenWebsocketError,
    KrakenWebsocketTimeout,
    SECRET_MAX_AGE,
)
from services.oms.kraken_rest import KrakenRESTClient, KrakenRESTError
from services.oms.kraken_ws import (
    KrakenWSError,
    KrakenWSTimeout,
    KrakenWSClient,
    OrderAck,
    _WebsocketTransport,
)
from services.oms.oms_kraken import KrakenCredentialWatcher
from services.oms.rate_limit_guard import rate_limit_guard
from services.oms.shadow_oms import shadow_oms
from shared.graceful_shutdown import flush_logging_handlers, setup_graceful_shutdown

try:  # pragma: no cover - optional dependency during tests
    import websockets
    from websockets import WebSocketClientProtocol
except Exception:  # pragma: no cover - fallback for environments without websockets
    websockets = None  # type: ignore[assignment]
    WebSocketClientProtocol = object  # type: ignore[misc, assignment]

from metrics import (
    increment_trade_rejection,
    record_oms_latency,
    setup_metrics,
)


def _build_session_store_from_env() -> SessionStoreProtocol:
    ttl_minutes = int(os.getenv("SESSION_TTL_MINUTES", "60"))
    redis_url = os.getenv("SESSION_REDIS_URL")
    if redis_url:
        try:  # pragma: no cover - optional dependency for redis-backed sessions
            import redis  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - surfaced when redis missing
            raise RuntimeError("redis package is required when SESSION_REDIS_URL is set") from exc
        client = redis.Redis.from_url(redis_url)
        return RedisSessionStore(client, ttl_minutes=ttl_minutes)
    return InMemorySessionStore(ttl_minutes=ttl_minutes)


def _attach_auth_service(store: SessionStoreProtocol) -> object | None:
    existing = getattr(app.state, "auth_service", None)
    if existing is not None:
        return existing

    spec = os.getenv("OMS_AUTH_SERVICE_CLIENT")
    if not spec:
        return None

    module_name, _, attr_name = spec.partition(":")
    attr_name = attr_name or "auth_service"
    try:
        module = importlib.import_module(module_name)
    except Exception:
        logger.debug("Unable to import auth service client module '%s'", module_name)
        return None

    try:
        client = getattr(module, attr_name)
    except AttributeError:
        logger.debug(
            "Auth service client module '%s' is missing attribute '%s'", module_name, attr_name
        )
        return None

    setattr(app.state, "auth_service", client)
    if hasattr(client, "_sessions"):
        try:
            setattr(client, "_sessions", store)
        except Exception:
            logger.debug("Auth service client does not allow overriding session store")
    return client

SHUTDOWN_TIMEOUT = float(
    os.getenv("OMS_SHUTDOWN_TIMEOUT", os.getenv("SERVICE_SHUTDOWN_TIMEOUT", "60.0"))
)

app = FastAPI(title="OMS Service")
setup_metrics(app)


logger = logging.getLogger(__name__)

SESSION_STORE = _build_session_store_from_env()
app.state.session_store = SESSION_STORE

_AUTH_SERVICE = _attach_auth_service(SESSION_STORE)
if _AUTH_SERVICE is None:
    security.set_default_session_store(SESSION_STORE)


async def _production_transport_factory(
    url: str, *, headers: Optional[Dict[str, str]] = None
) -> _WebsocketTransport:
    """Establish a production Kraken websocket transport."""

    if websockets is None:  # pragma: no cover - runtime guard
        raise RuntimeError("websockets module unavailable for Kraken transport")
    protocol: WebSocketClientProtocol = await websockets.connect(  # type: ignore[assignment]
        url,
        ping_interval=None,
        extra_headers=headers,
    )
    return _WebsocketTransport(protocol)


if not hasattr(app.state, "kraken_transport_factory"):
    app.state.kraken_transport_factory = _production_transport_factory
if not hasattr(app.state, "kraken_client_factory"):
    app.state.kraken_client_factory = None


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


@app.on_event("startup")
async def _on_startup_initialize_metadata() -> None:
    await market_metadata_cache.start()


@app.on_event("shutdown")
async def _on_shutdown_complete() -> None:
    await market_metadata_cache.stop()
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


_BASE_ALIASES: Dict[str, str] = {
    "XBT": "BTC",
    "XXBT": "BTC",
    "XXBTZ": "BTC",
    "XDG": "DOGE",
    "XXDG": "DOGE",
    "XETH": "ETH",
    "XETC": "ETC",
}

_QUOTE_ALIASES: Dict[str, str] = {
    "USD": "USD",
    "ZUSD": "USD",
}


def _normalize_asset(symbol: str, *, is_quote: bool) -> str:
    token = (symbol or "").strip().upper()
    if not token:
        return ""

    aliases = _QUOTE_ALIASES if is_quote else _BASE_ALIASES
    direct = aliases.get(token)
    if direct:
        return direct

    trimmed = token
    while len(trimmed) > 3 and trimmed.endswith(("X", "Z")):
        trimmed = trimmed[:-1]
    while len(trimmed) > 3 and trimmed.startswith(("X", "Z")):
        trimmed = trimmed[1:]

    return aliases.get(trimmed, trimmed)


def _normalize_instrument(symbol: str) -> str:
    return symbol.replace("/", "-").upper()


def _step_from_metadata(
    metadata: Dict[str, Any],
    step_keys: List[str],
    decimal_keys: List[str],
) -> Optional[Decimal]:
    for key in step_keys:
        value = metadata.get(key)
        if value is None:
            continue
        try:
            step = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            continue
        if step > 0:
            return step

    for key in decimal_keys:
        value = metadata.get(key)
        if value is None:
            continue
        try:
            decimals = int(value)
        except (TypeError, ValueError):
            continue
        if decimals < 0:
            continue
        return Decimal("1") / (Decimal("10") ** decimals)

    return None


def _instrument_from_pair(metadata: Dict[str, Any]) -> Optional[str]:
    base = _normalize_asset(str(metadata.get("base") or ""), is_quote=False)
    quote = _normalize_asset(str(metadata.get("quote") or ""), is_quote=True)

    if not base or not quote:
        wsname = metadata.get("wsname")
        if isinstance(wsname, str) and "/" in wsname:
            base_part, quote_part = wsname.split("/", 1)
            base = base or _normalize_asset(base_part, is_quote=False)
            quote = quote or _normalize_asset(quote_part, is_quote=True)

    if (not base or not quote) and isinstance(metadata.get("altname"), str):
        altname = metadata["altname"].replace("/", "").upper()
        if len(altname) >= 6:
            base = base or _normalize_asset(altname[:-3], is_quote=False)
            quote = quote or _normalize_asset(altname[-3:], is_quote=True)

    if not base or quote != "USD":
        return None

    return f"{base}-USD"


def _parse_asset_pairs(payload: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    parsed: Dict[str, Dict[str, float]] = {}
    for entry in payload.values():
        if not isinstance(entry, dict):
            continue
        instrument = _instrument_from_pair(entry)
        if not instrument:
            continue
        tick = _step_from_metadata(entry, ["tick_size", "price_increment"], ["pair_decimals"])
        lot = _step_from_metadata(entry, ["lot_step", "step_size"], ["lot_decimals"])
        if tick is None or lot is None:
            continue
        parsed[_normalize_instrument(instrument)] = {
            "tick": float(tick),
            "lot": float(lot),
        }
    return parsed


async def _fetch_asset_pairs() -> Dict[str, Any]:
    async def _anonymous_credentials() -> Dict[str, Any]:
        return {}

    rest_client = KrakenRESTClient(credential_getter=_anonymous_credentials)
    try:
        return await rest_client.asset_pairs()
    except KrakenRESTError as exc:
        logger.warning("Failed to load Kraken asset metadata: %s", exc)
        return {}
    finally:
        await rest_client.close()


class MarketMetadataCache:
    def __init__(self, refresh_interval: float) -> None:
        self._data: Dict[str, Dict[str, float]] = {}
        self._lock = asyncio.Lock()
        self._refresh_interval = max(refresh_interval, 0.0)
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        await self.refresh()
        if self._refresh_interval > 0:
            self._task = asyncio.create_task(
                self._run(), name="kraken-metadata-refresh"
            )

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def refresh(self) -> None:
        payload = await _fetch_asset_pairs()
        if not payload:
            return
        parsed = _parse_asset_pairs(payload)
        if not parsed:
            return
        async with self._lock:
            self._data = parsed

    async def get(self, instrument: str) -> Optional[Dict[str, float]]:
        key = _normalize_instrument(instrument)
        async with self._lock:
            entry = self._data.get(key)
            return dict(entry) if entry else None

    async def snapshot(self) -> Dict[str, Dict[str, float]]:
        async with self._lock:
            return {symbol: dict(values) for symbol, values in self._data.items()}

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._refresh_interval)
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("Error refreshing Kraken asset metadata: %s", exc)


def _metadata_refresh_interval() -> float:
    try:
        return float(os.getenv("KRAKEN_METADATA_REFRESH_INTERVAL", "300"))
    except ValueError:
        return 300.0


market_metadata_cache = MarketMetadataCache(_metadata_refresh_interval())
app.state.market_metadata_cache = market_metadata_cache


_SUCCESS_STATUSES = {"ok", "accepted", "open"}


@dataclass
class KrakenClientBundle:
    credential_getter: Callable[[], Awaitable[Dict[str, Any]]]
    ws_client: KrakenWSClient
    rest_client: KrakenRESTClient


def _make_credential_getter(account_id: str) -> Callable[[], Awaitable[Dict[str, Any]]]:
    watcher = KrakenCredentialWatcher.instance(account_id)

    async def _get_credentials() -> Dict[str, Any]:
        payload, _ = watcher.snapshot()
        credentials = dict(payload)
        credentials.setdefault("account_id", account_id)
        return credentials

    return _get_credentials


def _credentials_expired(credentials: Dict[str, Any]) -> bool:
    metadata = credentials.get("metadata") if isinstance(credentials, dict) else None
    rotated_at = None
    if isinstance(metadata, dict):
        rotated_at = metadata.get("rotated_at") or metadata.get("last_rotated_at")
        if rotated_at is None:
            annotations = metadata.get("annotations")
            if isinstance(annotations, dict):
                rotated_at = annotations.get("aether.kraken/lastRotatedAt")

    if rotated_at is None:
        return True

    if isinstance(rotated_at, datetime):
        timestamp = rotated_at
    elif isinstance(rotated_at, str):
        try:
            timestamp = datetime.fromisoformat(rotated_at.replace("Z", "+00:00"))
        except ValueError:
            return True
    else:
        return True

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    return now - timestamp > SECRET_MAX_AGE


def _ensure_credentials_valid(credentials: Dict[str, Any]) -> None:
    api_key = credentials.get("api_key")
    api_secret = credentials.get("api_secret")
    if not api_key or not api_secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kraken credentials unavailable.",
        )
    if _credentials_expired(credentials):
        raise KrakenCredentialExpired(
            "Kraken API credentials have expired; rotation required before trading."
        )


@asynccontextmanager
async def _default_client_factory(account_id: str) -> AsyncIterator[KrakenClientBundle]:
    credential_getter = _make_credential_getter(account_id)
    rest_client = KrakenRESTClient(credential_getter=credential_getter)
    transport_factory = getattr(
        app.state, "kraken_transport_factory", _production_transport_factory
    ) or _production_transport_factory
    ws_ctor = getattr(sys.modules[__name__], "KrakenWSClient")
    ws_client = ws_ctor(
        credential_getter=credential_getter,
        rest_client=rest_client,
        transport_factory=transport_factory,
        account_id=account_id,
    )
    ws_client = _normalize_ws_client(ws_client)
    try:
        yield KrakenClientBundle(
            credential_getter=credential_getter,
            ws_client=ws_client,
            rest_client=rest_client,
        )
    finally:
        await ws_client.close()
        await rest_client.close()


@asynccontextmanager
async def _acquire_kraken_clients(account_id: str) -> AsyncIterator[KrakenClientBundle]:
    factory = getattr(app.state, "kraken_client_factory", None)
    if factory is None:
        factory = _default_client_factory
    async with factory(account_id) as bundle:
        yield bundle


def _normalize_ws_client(client: Any) -> Any:
    add_order = getattr(client, "add_order", None)
    if asyncio.iscoroutinefunction(add_order):
        return client

    class _SyncAdapter:
        def __init__(self, inner: Any) -> None:
            self._inner = inner

        async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
            try:
                result = await asyncio.to_thread(
                    self._inner.add_order, payload, timeout=None
                )
            except KrakenWebsocketTimeout as exc:
                raise KrakenWSTimeout(str(exc)) from exc
            except KrakenWebsocketError as exc:
                raise KrakenWSError(str(exc)) from exc
            if isinstance(result, OrderAck):
                return result
            txid: Any = None
            status: Any = None
            errors: Any = None
            if isinstance(result, dict):
                txid = result.get("txid")
                status = result.get("status")
                errors = result.get("error") or result.get("errors")
            if isinstance(errors, str):
                errors = [errors]
            return OrderAck(
                exchange_order_id=str(txid) if txid else None,
                status=status or "ok",
                filled_qty=None,
                avg_price=None,
                errors=errors,
            )

        async def fetch_open_orders_snapshot(self) -> List[Dict[str, Any]]:
            payload = await asyncio.to_thread(self._inner.open_orders)
            return _extract_open_orders(payload) if isinstance(payload, dict) else []

        async def fetch_own_trades_snapshot(self) -> List[Dict[str, Any]]:
            payload = await asyncio.to_thread(self._inner.own_trades)
            return _extract_trades(payload) if isinstance(payload, dict) else []

        async def close(self) -> None:
            close = getattr(self._inner, "close", None)
            if close is None:
                return
            await asyncio.to_thread(close)

    return _SyncAdapter(client)


async def _submit_order(
    ws_client: KrakenWSClient,
    rest_client: KrakenRESTClient,
    payload: Dict[str, Any],
) -> Tuple[OrderAck, str]:
    try:
        ack = await ws_client.add_order(payload)
        return ack, "websocket"
    except (KrakenWSError, KrakenWSTimeout) as exc:
        ws_error = exc
    try:
        ack = await rest_client.add_order(payload)
        return ack, "rest"
    except KrakenRESTError as rest_exc:
        if isinstance(ws_error, KrakenWSTimeout):
            raise HTTPException(
                status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Kraken websocket request timed out",
            ) from rest_exc
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            detail=str(rest_exc),
        ) from rest_exc


def _ensure_ack_success(ack: OrderAck, transport: str) -> None:
    errors = ack.errors or []
    if errors:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            detail=", ".join(str(err) for err in errors),
        )
    status_value = (ack.status or "").lower()
    if status_value and status_value not in _SUCCESS_STATUSES:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            detail=f"Kraken {transport} rejected order: {ack.status}",
        )
    if not ack.exchange_order_id:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            detail="Kraken did not return an order identifier.",
        )


def _ack_payload(
    ack: OrderAck,
    *,
    request: OrderPlacementRequest,
    transport: str,
    snapped_price: float,
    snapped_quantity: float,
    flags: str,
    open_orders: List[Dict[str, Any]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "order_id": request.order_id,
        "txid": ack.exchange_order_id,
        "status": ack.status or "ok",
        "transport": transport,
        "price": snapped_price,
        "quantity": snapped_quantity,
        "flags": flags,
        "open_orders": open_orders,
    }
    if ack.filled_qty is not None:
        payload["filled_qty"] = float(ack.filled_qty)
    if ack.avg_price is not None:
        payload["avg_price"] = float(ack.avg_price)
    if ack.errors:
        payload["errors"] = list(ack.errors)
    return payload


async def _fetch_open_orders(
    ws_client: KrakenWSClient,
    rest_client: KrakenRESTClient,
) -> List[Dict[str, Any]]:
    try:
        return await ws_client.fetch_open_orders_snapshot()
    except (KrakenWSError, KrakenWSTimeout):
        try:
            payload = await rest_client.open_orders()
        except KrakenRESTError as exc:
            logger.debug("Failed to fetch open orders via REST: %s", exc)
            return []
        return _extract_open_orders(payload)


def _extract_open_orders(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates: List[Any] = []
    open_section = payload.get("open")
    if isinstance(open_section, list):
        candidates.extend(open_section)
    elif isinstance(open_section, dict):
        candidates.extend(open_section.values())
    result = payload.get("result")
    if isinstance(result, dict):
        nested = result.get("open")
        if isinstance(nested, list):
            candidates.extend(nested)
        elif isinstance(nested, dict):
            candidates.extend(nested.values())
    orders: List[Dict[str, Any]] = []
    for entry in candidates:
        if isinstance(entry, dict):
            orders.append(entry)
    return orders


async def _fetch_own_trades(
    ws_client: KrakenWSClient,
    rest_client: KrakenRESTClient,
    txid: Optional[str],
) -> List[Dict[str, Any]]:
    trades: List[Dict[str, Any]]
    try:
        trades = await ws_client.fetch_own_trades_snapshot()
    except (KrakenWSError, KrakenWSTimeout):
        try:
            payload = await rest_client.own_trades()
        except KrakenRESTError as exc:
            logger.debug("Failed to fetch own trades via REST: %s", exc)
            return []
        trades = _extract_trades(payload)
    if not txid:
        return trades
    matched: List[Dict[str, Any]] = []
    for trade in trades:
        order_ref = trade.get("order_id") or trade.get("ordertxid") or trade.get("txid")
        if order_ref and str(order_ref) == str(txid):
            matched.append(trade)
    return matched


def _extract_trades(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates: List[Any] = []
    trades_section = payload.get("trades")
    if isinstance(trades_section, list):
        candidates.extend(trades_section)
    elif isinstance(trades_section, dict):
        candidates.extend(trades_section.values())
    result = payload.get("result")
    if isinstance(result, dict):
        nested = result.get("trades")
        if isinstance(nested, list):
            candidates.extend(nested)
        elif isinstance(nested, dict):
            candidates.extend(nested.values())
    trades: List[Dict[str, Any]] = []
    for entry in candidates:
        if isinstance(entry, dict):
            trades.append(entry)
    return trades


def _snap(
    value: float,
    step: float,
    *,
    side: str,
    floor_quantity: bool = False,
) -> float:
    try:
        quant = Decimal(str(step))
        decimal_value = Decimal(str(value))
    except Exception:
        return value

    if quant <= 0:
        return value

    rounding = ROUND_FLOOR
    if not floor_quantity and side.upper() == "SELL":
        rounding = ROUND_CEILING

    try:
        snapped_ratio = (decimal_value / quant).to_integral_value(rounding=rounding)
    except Exception:
        return value

    snapped = snapped_ratio * quant

    if floor_quantity and snapped > decimal_value:
        snapped -= quant

    return float(snapped)


def _kraken_flags(request: OrderPlacementRequest) -> List[str]:
    flags: List[str] = []
    if request.post_only:
        flags.append("post")
    if request.reduce_only:
        flags.append("reduce_only")
    return flags


@app.post("/oms/place", response_model=OrderPlacementResponse)
async def place_order(
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
    snapped_price = _snap(request.price, metadata["tick"], side=request.side)
    snapped_quantity = _snap(
        request.quantity, metadata["lot"], side=request.side, floor_quantity=True
    )


    if snapped_price <= 0 or snapped_quantity <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Snapped price/quantity must be positive.",
        )

    kafka = KafkaNATSAdapter(account_id=account_id)
    timescale = TimescaleAdapter(account_id=account_id)

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
        order_payload["takeProfit"] = _snap(
            request.take_profit,
            metadata["tick"],
            side=request.side,
        )
    if request.stop_loss:
        order_payload["stopLoss"] = _snap(
            request.stop_loss,
            metadata["tick"],
            side=request.side,
        )

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

    async with _acquire_kraken_clients(account_id) as clients:
        credentials = await clients.credential_getter()
        try:
            _ensure_credentials_valid(credentials)
        except KrakenCredentialExpired as exc:
            increment_trade_rejection(account_id, request.instrument)
            logger.warning(
                "Rejected order due to expired Kraken credentials",
                extra={"account_id": account_id, "instrument": request.instrument},
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=str(exc),
            ) from exc

        start_time = time.perf_counter()
        try:
            ack, transport = await _submit_order(
                clients.ws_client,
                clients.rest_client,
                order_payload,
            )
        except HTTPException as exc:
            if exc.status_code in (
                status.HTTP_502_BAD_GATEWAY,
                status.HTTP_504_GATEWAY_TIMEOUT,
            ):
                increment_trade_rejection(account_id, request.instrument)
            raise

        ack_latency_ms = (time.perf_counter() - start_time) * 1000.0
        record_oms_latency(
            account_id,
            request.instrument,
            transport,
            ack_latency_ms,
        )


        try:
            _ensure_ack_success(ack, transport)
        except HTTPException as exc:
            increment_trade_rejection(account_id, request.instrument)
            raise


        open_orders = await _fetch_open_orders(
            clients.ws_client,
            clients.rest_client,
        )
        trades = await _fetch_own_trades(
            clients.ws_client,
            clients.rest_client,
            ack.exchange_order_id,
        )

    ack_payload = _ack_payload(
        ack,
        request=request,
        transport=transport,
        snapped_price=snapped_price,
        snapped_quantity=snapped_quantity,
        flags=order_payload["oflags"],
        open_orders=open_orders,
    )
    timescale.record_ack(ack_payload)
    timescale.record_usage(snapped_price * snapped_quantity)

    kafka.publish(topic="oms.acks", payload=ack_payload)

    status_value = str(ack_payload.get("status", "")).lower()

    accepted = not status_value or status_value in _SUCCESS_STATUSES


    if status_value and not accepted:
        increment_trade_rejection(account_id, request.instrument)

    trades_snapshot = {"trades": trades}

    for trade in trades_snapshot.get("trades", []):
        fill_payload = {
            "order_id": request.order_id,
            "txid": ack.exchange_order_id,
            "price": trade.get("price") or trade.get("avg_price", snapped_price),
            "quantity": trade.get("quantity")
            or trade.get("vol")
            or trade.get("volume")
            or snapped_quantity,
            "liquidity": trade.get(
                "liquidity",
                "maker" if request.post_only else "taker",
            ),
        }
        timescale.record_fill(fill_payload)
        kafka.publish(topic="oms.executions", payload=fill_payload)

        trade_side = str(trade.get("side", request.side)).lower()
        trade_qty = Decimal(
            str(
                trade.get("quantity")
                or trade.get("vol")
                or trade.get("volume")
                or snapped_quantity
            )
        )
        trade_price = Decimal(
            str(trade.get("price") or trade.get("avg_price") or snapped_price)
        )
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

    try:
        shadow_fills = shadow_oms.generate_shadow_fills(
            account_id=account_id,
            symbol=request.instrument,
            side=request.side,
            quantity=Decimal(str(snapped_quantity)),
            price=Decimal(str(snapped_price)),
            timestamp=datetime.now(timezone.utc),
        )
    except RuntimeError as exc:
        logger.debug("Shadow fill generation failed: %s", exc)
        shadow_fills = []
    for shadow_fill in shadow_fills:
        timescale.record_shadow_fill(shadow_fill)

    accepted = (not status_value) or status_value in _SUCCESS_STATUSES
    venue = "kraken"
    return OrderPlacementResponse(accepted=accepted, routed_venue=venue, fee=request.fee)


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

