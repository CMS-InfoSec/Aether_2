from __future__ import annotations


import asyncio
import contextlib
import json
import logging
import os
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any, Awaitable, Dict, Iterable, List, Optional, Tuple

from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator

from services.oms.kraken_rest import KrakenRESTClient, KrakenRESTError
from services.oms.kraken_ws import (
    KrakenWSError,
    KrakenWSTimeout,
    KrakenWSClient,
    OrderAck,
    OrderState,
)



logger = logging.getLogger(__name__)


app = FastAPI(title="Kraken OMS Async Service")


class OMSPlaceRequest(BaseModel):
    account_id: str = Field(..., description="Account identifier for credential lookup")
    client_id: str = Field(..., description="Client supplied idempotency key")
    symbol: str = Field(..., description="Trading symbol (e.g. BTC/USD)")
    side: str = Field(..., description="BUY or SELL")
    order_type: str = Field(..., alias="type", description="Order type (limit, market, stop-limit, etc)")
    qty: Decimal = Field(..., gt=0, description="Base quantity to trade")
    limit_px: Optional[Decimal] = Field(
        default=None,
        gt=Decimal("0"),
        description="Limit price when applicable",
    )
    tif: Optional[str] = Field(
        default=None,
        description="Explicit time-in-force (GTC, IOC, FOK)",
    )
    flags: List[str] = Field(default_factory=list, description="Additional Kraken oflags")
    post_only: bool = Field(False, description="Convenience flag for post-only")
    reduce_only: bool = Field(False, description="Convenience flag for reduce-only")
    take_profit: Optional[Decimal] = Field(
        default=None,
        gt=Decimal("0"),
        description="Exchange-native take-profit trigger",
    )
    stop_loss: Optional[Decimal] = Field(
        default=None,
        gt=Decimal("0"),
        description="Exchange-native stop-loss trigger",
    )
    trailing_offset: Optional[Decimal] = Field(
        default=None,
        gt=Decimal("0"),
        description="Trailing stop offset",
    )

    @field_validator("side")
    @classmethod
    def _validate_side(cls, value: str) -> str:
        normalized = value.lower()
        if normalized not in {"buy", "sell"}:
            raise ValueError("side must be BUY or SELL")
        return normalized

    @field_validator("tif")
    @classmethod
    def _validate_tif(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        normalized = value.lower()
        if normalized not in {"gtc", "ioc", "fok"}:
            raise ValueError("tif must be one of GTC, IOC, FOK")
        return normalized

    @field_validator("flags", mode="before")
    @classmethod
    def _normalize_flags(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, Iterable):
            return list(value)
        raise ValueError("flags must be a list or string")


class OMSCancelRequest(BaseModel):
    account_id: str = Field(..., description="Account identifier")
    client_id: str = Field(..., description="Idempotent client identifier")
    exchange_order_id: Optional[str] = Field(
        default=None,
        description="Exchange provided txid (preferred). When omitted the last placed order for the client_id is cancelled.",
    )


class OMSOrderStatusResponse(BaseModel):
    exchange_order_id: str
    status: str
    filled_qty: Decimal = Field(default=Decimal("0"))
    avg_price: Decimal = Field(default=Decimal("0"))
    errors: Optional[List[str]] = None


class OMSPlaceResponse(OMSOrderStatusResponse):
    transport: str = Field(default="websocket")
    reused: bool = Field(
        default=False, description="True when the idempotency cache satisfied the request"
    )


class _IdempotencyStore:
    """Cooperative idempotency cache used per-account."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._entries: Dict[str, asyncio.Future[OMSOrderStatusResponse]] = {}

    async def get_or_create(
        self, key: str, factory: Awaitable[OMSOrderStatusResponse]
    ) -> Tuple[OMSOrderStatusResponse, bool]:
        async with self._lock:
            future = self._entries.get(key)
            if future is None:
                loop = asyncio.get_event_loop()
                future = loop.create_future()
                self._entries[key] = future
                create_future = True
            else:
                create_future = False

        if create_future:
            try:
                result = await factory
            except Exception as exc:  # pragma: no cover - propagate to awaiting callers
                future.set_exception(exc)
                raise
            else:
                future.set_result(result)
                return result, False

        return await future, True


@dataclass
class OrderRecord:
    client_id: str
    result: OMSOrderStatusResponse
    transport: str


class _PrecisionValidator:
    """Validates order precision using metadata from Kraken asset pairs."""

    @staticmethod
    def _snap(value: Decimal, step: Decimal | None) -> Decimal:
        if step is None or step <= 0:
            return value
        quant = step if isinstance(step, Decimal) else Decimal(str(step))
        operand = value if isinstance(value, Decimal) else Decimal(str(value))
        # quantize using bankers rounding
        snapped = (operand / quant).to_integral_value(rounding=ROUND_HALF_EVEN) * quant
        return snapped

    @classmethod
    def validate(
        cls,
        symbol: str,
        qty: Decimal,
        price: Optional[Decimal],
        metadata: Dict[str, Any] | None,
    ) -> Tuple[Decimal, Optional[Decimal]]:
        if not metadata:
            return qty, price

        pair_meta = metadata.get(symbol) if isinstance(metadata, dict) else None
        if not isinstance(pair_meta, dict):
            return qty, price

        qty_step = cls._step(pair_meta, ["lot_step", "lot_decimals", "step_size"])
        px_step = cls._step(pair_meta, ["price_increment", "pair_decimals", "tick_size"])

        snapped_qty = cls._snap(qty, qty_step) if qty_step else qty
        snapped_px = cls._snap(price, px_step) if price is not None else None

        min_qty = cls._maybe_decimal(pair_meta.get("ordermin"), pair_meta.get("min_qty"))
        if min_qty is not None and snapped_qty < min_qty:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Quantity {snapped_qty} below exchange minimum {min_qty}",
            )

        min_price = cls._maybe_decimal(pair_meta.get("min_price"))
        if min_price is not None and snapped_px is not None and snapped_px < min_price:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Price {snapped_px} below exchange minimum {min_price}",
            )

        return snapped_qty, snapped_px

    @staticmethod
    def _maybe_decimal(*values: Any) -> Optional[Decimal]:
        for value in values:
            if value is None:
                continue
            if isinstance(value, Decimal):
                return value
            try:
                return Decimal(str(value))
            except Exception:  # pragma: no cover - defensive
                continue
        return None

    @classmethod
    def _step(cls, metadata: Dict[str, Any], keys: List[str]) -> Optional[Decimal]:
        for key in keys:
            if key not in metadata:
                continue
            value = metadata[key]
            if value is None:
                continue
            if key.endswith("decimals"):
                try:
                    decimals = int(value)
                except (TypeError, ValueError):
                    continue
                return Decimal("1") / (Decimal("10") ** decimals)
            try:
                return Decimal(str(value))
            except Exception:  # pragma: no cover - defensive
                continue
        return None


class CredentialWatcher:
    """Watches Kubernetes mounted secrets for key rotation."""

    _instances: Dict[str, "CredentialWatcher"] = {}
    _base_path = Path(os.environ.get("KRAKEN_SECRETS_BASE", "/var/run/secrets/kraken"))

    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self._secret_path = self._resolve_secret_path(account_id)
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition()
        self._credentials: Dict[str, Any] = {}
        self._metadata: Dict[str, Any] | None = None
        self._version = 0
        self._last_mtime: float | None = None
        self._task: Optional[asyncio.Task[None]] = None
        self._poll_interval = float(os.environ.get("KRAKEN_SECRET_POLL", "5"))

    @classmethod
    def instance(cls, account_id: str) -> "CredentialWatcher":
        if account_id not in cls._instances:
            cls._instances[account_id] = cls(account_id)
        return cls._instances[account_id]

    @classmethod
    def _resolve_secret_path(cls, account_id: str) -> Path:
        directory = cls._base_path / account_id
        file_path = directory / "credentials.json"
        if file_path.exists():
            return file_path
        # fallback to single file per account
        fallback = cls._base_path / f"{account_id}.json"
        return fallback

    async def start(self) -> None:
        if self._task is None:
            await self._load()
            self._task = asyncio.create_task(self._watch(), name=f"{self.account_id}-credential-watch")

    async def _watch(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._poll_interval)
                await self._maybe_reload()
            except asyncio.CancelledError:  # pragma: no cover - cancellation path
                raise
            except Exception as exc:  # pragma: no cover - log and continue
                logger.exception("Credential watcher error for %s: %s", self.account_id, exc)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    async def _maybe_reload(self) -> None:
        try:
            mtime = self._secret_path.stat().st_mtime
        except FileNotFoundError:
            logger.warning("Credentials not found for account %s at %s", self.account_id, self._secret_path)
            return

        if self._last_mtime is None or mtime > self._last_mtime:
            await self._load()
            async with self._condition:
                self._condition.notify_all()

    async def _load(self) -> None:
        try:
            raw = self._secret_path.read_text()
        except FileNotFoundError:
            logger.warning("Credential file missing for account %s", self.account_id)
            return

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error("Invalid credential payload for %s: %s", self.account_id, exc)
            return
        credentials = {
            "api_key": data.get("api_key") or data.get("key"),
            "api_secret": data.get("api_secret") or data.get("secret"),
        }
        metadata = data.get("asset_pairs") or data.get("metadata")

        async with self._lock:
            self._credentials = credentials
            self._metadata = metadata
            self._version += 1
            self._last_mtime = self._secret_path.stat().st_mtime
        logger.info("Loaded credentials for account %s (version=%s)", self.account_id, self._version)

    async def get_credentials(self) -> Dict[str, Any]:
        async with self._lock:
            return dict(self._credentials)

    async def get_metadata(self) -> Dict[str, Any] | None:
        async with self._lock:
            return self._metadata.copy() if isinstance(self._metadata, dict) else None


class AccountContext:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.credentials = CredentialWatcher.instance(account_id)
        self.ws_client: Optional[KrakenWSClient] = None
        self.rest_client: Optional[KrakenRESTClient] = None
        self.idempotency = _IdempotencyStore()
        self._orders: Dict[str, OrderRecord] = {}
        self._orders_lock = asyncio.Lock()
        self._startup_lock = asyncio.Lock()
        self._stream_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        async with self._startup_lock:
            await self.credentials.start()
            if self.ws_client is None:
                self.ws_client = KrakenWSClient(
                    credential_getter=self.credentials.get_credentials,
                    stream_update_cb=self._apply_stream_state,
                )
                self._stream_task = asyncio.create_task(self.ws_client.stream_handler())
            if self.rest_client is None:
                self.rest_client = KrakenRESTClient(credential_getter=self.credentials.get_credentials)

            await self.ws_client.ensure_connected()
            await self.ws_client.subscribe_private(["openOrders", "ownTrades"])

    async def close(self) -> None:
        if self.ws_client is not None:
            await self.ws_client.close()
        if self._stream_task is not None:
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stream_task
            self._stream_task = None
        if self.rest_client is not None:
            await self.rest_client.close()

    async def _apply_stream_state(self, state: OrderState) -> None:
        if not state.client_order_id:
            return
        key = state.client_order_id
        result = OMSOrderStatusResponse(
            exchange_order_id=state.exchange_order_id or state.client_order_id,
            status=state.status,
            filled_qty=Decimal(str(state.filled_qty or 0)),
            avg_price=Decimal(str(state.avg_price or 0)),
            errors=state.errors or None,
        )
        record = OrderRecord(client_id=key, result=result, transport=state.transport)
        async with self._orders_lock:
            self._orders[key] = record

    async def place_order(self, request: OMSPlaceRequest) -> OMSPlaceResponse:
        await self.start()

        async def _execute() -> OMSOrderStatusResponse:
            assert self.ws_client is not None
            assert self.rest_client is not None

            metadata = await self.credentials.get_metadata()
            qty, px = _PrecisionValidator.validate(
                request.symbol,
                request.qty,
                request.limit_px,
                metadata,
            )

            payload = self._build_payload(request, qty, px)
            try:
                ack = await self.ws_client.add_order(payload)
                transport = "websocket"
            except (KrakenWSTimeout, KrakenWSError) as exc:
                logger.warning(
                    "Websocket add_order failed for account %s: %s", self.account_id, exc
                )
                try:
                    ack = await self.rest_client.add_order(payload)
                except KrakenRESTError as rest_exc:
                    raise HTTPException(
                        status_code=status.HTTP_502_BAD_GATEWAY,
                        detail=str(rest_exc),
                    ) from rest_exc
                transport = "rest"

            result = self._order_result_from_ack(request, ack)
            async with self._orders_lock:
                self._orders[request.client_id] = OrderRecord(
                    client_id=request.client_id, result=result, transport=transport
                )
            return result

        cache_key = f"place:{request.client_id}"
        result, reused = await self.idempotency.get_or_create(cache_key, _execute())
        async with self._orders_lock:
            record = self._orders.get(request.client_id)
        transport = record.transport if record else "websocket"
        return OMSPlaceResponse(
            exchange_order_id=result.exchange_order_id,
            status=result.status,
            filled_qty=result.filled_qty,
            avg_price=result.avg_price,
            errors=result.errors,
            transport=transport,
            reused=reused,
        )

    async def cancel_order(self, request: OMSCancelRequest) -> OMSOrderStatusResponse:
        await self.start()

        async def _execute_cancel() -> OMSOrderStatusResponse:
            assert self.ws_client is not None
            assert self.rest_client is not None

            txid = request.exchange_order_id
            if txid is None:
                async with self._orders_lock:
                    record = self._orders.get(request.client_id)
                if record is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Unknown order for cancellation",
                    )
                txid = record.result.exchange_order_id

            try:
                ack = await self.ws_client.cancel_order({"txid": txid})
                transport = "websocket"
            except (KrakenWSTimeout, KrakenWSError) as exc:
                logger.warning(
                    "Websocket cancel failed for account %s: %s", self.account_id, exc
                )
                try:
                    ack = await self.rest_client.cancel_order({"txid": txid})
                except KrakenRESTError as rest_exc:
                    raise HTTPException(
                        status_code=status.HTTP_502_BAD_GATEWAY,
                        detail=str(rest_exc),
                    ) from rest_exc
                transport = "rest"

            status_value = ack.status or ("rejected" if ack.errors else "canceled")
            result = OMSOrderStatusResponse(
                exchange_order_id=ack.exchange_order_id or txid,
                status=status_value,
                filled_qty=Decimal(str(ack.filled_qty or 0)),
                avg_price=Decimal(str(ack.avg_price or 0)),
                errors=ack.errors or None,
            )
            async with self._orders_lock:
                self._orders[request.client_id] = OrderRecord(
                    client_id=request.client_id, result=result, transport=transport
                )
            return result

        cache_key = f"cancel:{request.client_id}"
        result, _ = await self.idempotency.get_or_create(cache_key, _execute_cancel())
        return result

    def _build_payload(
        self,
        request: OMSPlaceRequest,
        qty: Decimal,
        price: Optional[Decimal],
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "clientOrderId": request.client_id,
            "pair": request.symbol.replace("-", "/").replace("_", "/"),
            "type": request.side,
            "ordertype": request.order_type.lower(),
            "volume": str(qty),
        }
        if price is not None:
            payload["price"] = str(price)

        oflags = set(flag.lower() for flag in request.flags)
        if request.post_only:
            oflags.add("post")
        if request.reduce_only:
            oflags.add("reduce_only")
        if oflags:
            payload["oflags"] = ",".join(sorted(oflags))

        if request.tif:
            payload["timeInForce"] = request.tif.upper()
        if request.take_profit is not None:
            payload["takeProfit"] = str(request.take_profit)
        if request.stop_loss is not None:
            payload["stopLoss"] = str(request.stop_loss)
        if request.trailing_offset is not None:
            payload["trailingStopOffset"] = str(request.trailing_offset)

        return payload

    def _order_result_from_ack(
        self,
        request: OMSPlaceRequest,
        ack: OrderAck,
    ) -> OMSOrderStatusResponse:
        status_value = ack.status or ("rejected" if ack.errors else "accepted")
        filled = Decimal(str(ack.filled_qty or 0))
        avg_price = Decimal(str(ack.avg_price or 0))
        return OMSOrderStatusResponse(
            exchange_order_id=ack.exchange_order_id or request.client_id,
            status=status_value,
            filled_qty=filled,
            avg_price=avg_price,
            errors=ack.errors or None,
        )

    async def lookup(self, client_id: str) -> OrderRecord | None:
        async with self._orders_lock:
            return self._orders.get(client_id)


class OMSManager:
    def __init__(self) -> None:
        self._accounts: Dict[str, AccountContext] = {}
        self._lock = asyncio.Lock()

    async def get_account(self, account_id: str) -> AccountContext:
        async with self._lock:
            ctx = self._accounts.get(account_id)
            if ctx is None:
                ctx = AccountContext(account_id)
                self._accounts[account_id] = ctx
        await ctx.start()
        return ctx

    async def shutdown(self) -> None:
        async with self._lock:
            accounts = list(self._accounts.values())
        await asyncio.gather(*(account.close() for account in accounts), return_exceptions=True)


manager = OMSManager()


async def require_account_id(request: Request) -> str:
    header = request.headers.get("X-Account-ID")
    if not header:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing X-Account-ID header")
    return header


@app.on_event("shutdown")
async def _shutdown() -> None:
    await manager.shutdown()


@app.post("/oms/place", response_model=OMSPlaceResponse)
async def place_order(
    payload: OMSPlaceRequest,
    account_id: str = Depends(require_account_id),
) -> OMSPlaceResponse:
    if payload.account_id != account_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account mismatch")

    if payload.order_type.lower() == "limit" and payload.limit_px is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="limit_px required for limit orders")

    account = await manager.get_account(payload.account_id)
    result = await account.place_order(payload)
    return result



@app.post("/oms/cancel", response_model=OMSOrderStatusResponse)
async def cancel_order(
    payload: OMSCancelRequest,
    account_id: str = Depends(require_account_id),
) -> OMSOrderStatusResponse:
    if payload.account_id != account_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account mismatch")

    account = await manager.get_account(payload.account_id)
    result = await account.cancel_order(payload)
    return result



@app.get("/oms/status", response_model=OMSOrderStatusResponse)
async def get_status(
    account_id: str,
    client_id: str,
    header_account: str = Depends(require_account_id),
) -> OMSOrderStatusResponse:
    if account_id != header_account:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account mismatch")

    account = await manager.get_account(account_id)
    record = await account.lookup(client_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown order")
    return record.result


