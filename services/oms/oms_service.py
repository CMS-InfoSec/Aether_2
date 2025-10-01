from __future__ import annotations

import uuid
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from services.oms.kraken_client import KrakenWSClient, KrakenWebsocketError


app = FastAPI(title="Kraken OMS Service")


SUPPORTED_FLAGS = {
    "post-only": "post",
    "post_only": "post",
    "post": "post",
    "reduce-only": "reduce_only",
    "reduce_only": "reduce_only",
    "reduce": "reduce_only",
}

SUPPORTED_TIFS = {"ioc", "fok", "gtc"}


class OMSPlaceRequest(BaseModel):
    account_id: str = Field(..., description="Account identifier for Kraken credentials")
    symbol: str = Field(..., description="Trading symbol (e.g. BTC/USD)")
    side: str = Field(..., description="buy or sell")
    qty: float = Field(..., gt=0, description="Order quantity")
    type: str = Field(..., description="Kraken order type (e.g. limit, market)")
    limit_px: Optional[float] = Field(
        default=None, gt=0, description="Limit price when applicable"
    )
    tif: Optional[str] = Field(
        default=None,
        description="Explicit time in force constraint (GTC, IOC, FOK)",
    )
    flags: List[str] = Field(default_factory=list, description="Optional Kraken flags")
    tp: Optional[float] = Field(default=None, gt=0, description="Take profit trigger")
    sl: Optional[float] = Field(default=None, gt=0, description="Stop loss trigger")
    trailing: Optional[float] = Field(
        default=None, gt=0, description="Trailing stop offset"
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
        tif = value.lower()
        if tif not in SUPPORTED_TIFS:
            raise ValueError("tif must be one of GTC, IOC, FOK")
        return tif

    @field_validator("flags", mode="before")
    @classmethod
    def _normalize_flags(cls, value: object) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            return value
        raise ValueError("flags must be a list or string")


class OMSPlaceResponse(BaseModel):
    exchange_order_id: str = Field(..., description="Kraken order identifier")
    status: str = Field(..., description="Derived order status")


def _build_flags(request: OMSPlaceRequest) -> Dict[str, Optional[str]]:
    tif = request.tif
    oflags: List[str] = []

    for raw in request.flags:
        normalized = raw.lower()
        if normalized in SUPPORTED_TIFS and tif is None:
            tif = normalized
            continue
        mapped = SUPPORTED_FLAGS.get(normalized)
        if mapped and mapped not in oflags:
            oflags.append(mapped)

    result: Dict[str, Optional[str]] = {
        "oflags": ",".join(oflags) if oflags else None,
        "timeInForce": tif.upper() if tif else None,
    }
    return result


def _build_order_payload(request: OMSPlaceRequest) -> Dict[str, object]:
    symbol = request.symbol.replace("-", "/")
    payload: Dict[str, object] = {
        "clientOrderId": uuid.uuid4().hex,
        "pair": symbol,
        "type": request.side,
        "ordertype": request.type.lower(),
        "volume": request.qty,
    }
    if request.limit_px is not None:
        payload["price"] = request.limit_px

    flag_payload = _build_flags(request)
    if flag_payload["oflags"]:
        payload["oflags"] = flag_payload["oflags"]
    if flag_payload["timeInForce"]:
        payload["timeInForce"] = flag_payload["timeInForce"]

    if request.tp is not None:
        payload["takeProfit"] = request.tp
    if request.sl is not None:
        payload["stopLoss"] = request.sl
    if request.trailing is not None:
        payload["trailingStopOffset"] = request.trailing

    return payload


def _derive_status(open_snapshot: Dict[str, object], trades_snapshot: Dict[str, object]) -> str:
    trades = trades_snapshot.get("trades") if isinstance(trades_snapshot, dict) else None
    if isinstance(trades, list) and trades:
        return "filled"

    open_orders = open_snapshot.get("open") if isinstance(open_snapshot, dict) else None
    if isinstance(open_orders, list) and open_orders:
        return "open"

    return "accepted"


@app.post("/oms/place", response_model=OMSPlaceResponse)
def place_order(request: OMSPlaceRequest) -> OMSPlaceResponse:
    if request.type.lower() == "limit" and request.limit_px is None:
        raise HTTPException(status_code=400, detail="limit_px required for limit orders")

    payload = _build_order_payload(request)
    client = KrakenWSClient(account_id=request.account_id)
    ack: Dict[str, object]
    exchange_order_id: str
    open_snapshot: Dict[str, object]
    trades_snapshot: Dict[str, object]
    try:
        try:
            ack = client.add_order(payload, timeout=2.0)
        except KrakenWebsocketError:
            ack = client.rest_add_order(payload)

        exchange_order_id = (
            str(ack.get("txid")) if ack.get("txid") else str(payload["clientOrderId"])
        )

        open_snapshot = client.open_orders()
        trades_snapshot = client.own_trades(txid=exchange_order_id)
    finally:
        client.close()

    status = _derive_status(open_snapshot, trades_snapshot)
    if isinstance(ack.get("status"), str) and ack["status"]:
        status = str(ack["status"])
    return OMSPlaceResponse(exchange_order_id=exchange_order_id, status=status)

