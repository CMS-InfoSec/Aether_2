"""FastAPI service for tax lot accounting and cost basis calculations.

The service ingests fills and treats each fill as an individual tax lot.  It
then exposes endpoints for computing realized and unrealized profit and loss
(PnL) using FIFO, LIFO, or average cost accounting.  Results can be returned as
JSON or exported as CSV to support downstream accounting workflows.
"""

from __future__ import annotations

import csv
import io
import uuid
from collections import defaultdict
import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from enum import Enum
from threading import RLock
from typing import Any, Dict, Iterable, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field, field_validator

from services.common.spot import require_spot_http
from shared.spot import require_spot_symbol


LOGGER = logging.getLogger(__name__)


class CostBasisMethod(str, Enum):
    """Supported cost basis calculation methodologies."""

    FIFO = "FIFO"
    LIFO = "LIFO"
    AVERAGE = "AVERAGE"

    @classmethod
    def parse(cls, value: str) -> "CostBasisMethod":
        try:
            return cls(value.upper())
        except (AttributeError, KeyError, ValueError) as exc:  # pragma: no cover - defensive
            raise HTTPException(status_code=400, detail=f"Unsupported method: {value}") from exc


@dataclass(frozen=True)
class TaxLot:
    """Immutable representation of an executed fill."""

    account_id: str
    symbol: str
    qty: Decimal
    price: Decimal
    ts: datetime
    lot_id: str

    @property
    def direction(self) -> int:
        return 1 if self.qty > 0 else -1 if self.qty < 0 else 0


@dataclass
class OpenPosition:
    """Track remaining quantity for an open lot."""

    lot: TaxLot
    remaining: Decimal  # absolute quantity still open
    direction: int


class TaxLotCreate(BaseModel):
    """Pydantic model for ingesting a new fill."""

    account_id: str = Field(..., min_length=1)
    symbol: str = Field(..., min_length=1)
    qty: Decimal = Field(..., description="Signed fill quantity; positive for buy, negative for sell")
    price: Decimal = Field(..., gt=0)
    ts: datetime
    lot_id: Optional[str] = Field(default=None, description="Optional unique identifier for the fill")

    @field_validator("qty", "price", mode="before")
    @classmethod
    def parse_decimal(cls, value: Any) -> Decimal:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (float, int)):
            value = str(value)
        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError("Value must be a decimal-compatible type")
            try:
                return Decimal(value)
            except InvalidOperation as exc:  # pragma: no cover - defensive validation
                raise ValueError("Invalid decimal value") from exc
        raise ValueError("Value must be a decimal-compatible type")

    @field_validator("qty")
    @classmethod
    def validate_quantity(cls, value: Decimal) -> Decimal:
        if value == 0:
            raise ValueError("Quantity must be non-zero")
        return value

    @field_validator("lot_id", mode="before")
    @classmethod
    def default_lot_id(cls, value: Optional[str]) -> str:
        return value or str(uuid.uuid4())

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        """Ensure symbols correspond to allowed USD spot market instruments."""

        return require_spot_symbol(value)

    def to_domain(self) -> TaxLot:
        return TaxLot(
            account_id=self.account_id,
            symbol=self.symbol,
            qty=self.qty,
            price=self.price,
            ts=self.ts,
            lot_id=self.lot_id,
        )


class TaxLotResponse(BaseModel):
    """Response model when a fill is recorded."""

    account_id: str
    symbol: str
    qty: Decimal
    price: Decimal
    ts: datetime
    lot_id: str


class RealizedLotDetail(BaseModel):
    """Detailed realized PnL information for a matched tax lot pair."""

    account_id: str
    symbol: str
    quantity: Decimal
    position_direction: str
    entry_price: Decimal
    entry_ts: datetime
    open_lot_id: Optional[str]
    exit_price: Decimal
    exit_ts: datetime
    close_lot_id: str
    realized_pnl: Decimal
    method: CostBasisMethod


class RealizedResponse(BaseModel):
    """Aggregate realized PnL for an account."""

    account_id: str
    method: CostBasisMethod
    total_realized_pnl: Decimal
    realized_by_symbol: Dict[str, Decimal]
    lots: List[RealizedLotDetail]


class UnrealizedLotDetail(BaseModel):
    """Unrealized PnL for each open lot or aggregated position."""

    account_id: str
    symbol: str
    quantity: Decimal
    position_direction: str
    cost_basis: Decimal
    current_price: Decimal
    unrealized_pnl: Decimal
    lot_ids: List[str]
    entry_ts: datetime
    method: CostBasisMethod


class UnrealizedResponse(BaseModel):
    """Aggregate unrealized PnL response."""

    account_id: str
    method: CostBasisMethod
    total_unrealized_pnl: Decimal
    unrealized_by_symbol: Dict[str, Decimal]
    lots: List[UnrealizedLotDetail]


class TaxLotStore:
    """Thread-safe in-memory store for tax lots."""

    def __init__(self) -> None:
        self._lots: Dict[str, List[TaxLot]] = defaultdict(list)
        self._lock = RLock()

    def add(self, lot: TaxLot) -> None:
        with self._lock:
            lots = self._lots[lot.account_id]
            lots.append(lot)
            lots.sort(key=lambda x: x.ts)

    def get(self, account_id: str, symbol: Optional[str] = None) -> List[TaxLot]:
        with self._lock:
            lots = list(self._lots.get(account_id, []))
        if symbol:
            lots = [lot for lot in lots if lot.symbol == symbol]
        return lots

    def accounts(self) -> Iterable[str]:
        with self._lock:
            return list(self._lots.keys())


store = TaxLotStore()
app = FastAPI(title="Tax Lot Service", version="1.0.0")


USD_PRECISION = Decimal("0.00000001")
ZERO = Decimal("0")


def _quantize_usd(value: Decimal) -> Decimal:
    """Quantize ``value`` to the configured USD precision."""

    return value.quantize(USD_PRECISION, rounding=ROUND_HALF_EVEN)


def _group_by_symbol(lots: Iterable[TaxLot]) -> Dict[str, List[TaxLot]]:
    grouped: Dict[str, List[TaxLot]] = defaultdict(list)
    for lot in lots:
        grouped[lot.symbol].append(lot)
    return grouped


def _realized_fifo_lifo(symbol: str, lots: List[TaxLot], method: CostBasisMethod) -> RealizedResponse:
    long_open: List[OpenPosition] = []
    short_open: List[OpenPosition] = []
    realized_details: List[RealizedLotDetail] = []
    realized_by_symbol: Dict[str, Decimal] = defaultdict(Decimal)

    def pick_open(container: List[OpenPosition]) -> OpenPosition:
        if not container:
            raise HTTPException(status_code=400, detail=f"No open positions to close for {symbol}")
        return container[0] if method == CostBasisMethod.FIFO else container[-1]

    def maybe_remove(container: List[OpenPosition], position: OpenPosition) -> None:
        if position.remaining <= ZERO:
            if method == CostBasisMethod.FIFO:
                container.pop(0)
            else:
                container.pop()

    for lot in lots:
        direction = 1 if lot.qty > ZERO else -1
        qty_remaining = abs(lot.qty)
        opposing = short_open if direction > 0 else long_open
        supporting = long_open if direction > 0 else short_open

        while qty_remaining > ZERO and opposing:
            open_pos = pick_open(opposing)
            matched = min(qty_remaining, open_pos.remaining)
            price_diff = lot.price - open_pos.lot.price
            realized = matched * price_diff * Decimal(open_pos.direction)
            realized_details.append(
                RealizedLotDetail(
                    account_id=lot.account_id,
                    symbol=symbol,
                    quantity=matched,
                    position_direction="LONG" if open_pos.direction > 0 else "SHORT",
                    entry_price=open_pos.lot.price,
                    entry_ts=open_pos.lot.ts,
                    open_lot_id=open_pos.lot.lot_id,
                    exit_price=lot.price,
                    exit_ts=lot.ts,
                    close_lot_id=lot.lot_id,
                    realized_pnl=realized,
                    method=method,
                )
            )
            realized_by_symbol[symbol] += realized
            open_pos.remaining -= matched
            qty_remaining -= matched
            maybe_remove(opposing, open_pos)

        if qty_remaining > ZERO:
            supporting.append(
                OpenPosition(
                    lot=lot,
                    remaining=qty_remaining,
                    direction=direction,
                )
            )

    total_realized = sum(realized_by_symbol.values(), ZERO)
    quantized_details = [
        detail.model_copy(update={"realized_pnl": _quantize_usd(detail.realized_pnl)})
        for detail in realized_details
    ]
    quantized_by_symbol = {symbol: _quantize_usd(value) for symbol, value in realized_by_symbol.items()}
    return RealizedResponse(
        account_id=lots[0].account_id if lots else "",
        method=method,
        total_realized_pnl=_quantize_usd(total_realized),
        realized_by_symbol=quantized_by_symbol,
        lots=quantized_details,
    )


def _unrealized_fifo_lifo(symbol: str, lots: List[TaxLot], method: CostBasisMethod) -> UnrealizedResponse:
    long_open: List[OpenPosition] = []
    short_open: List[OpenPosition] = []
    last_price: Optional[Decimal] = None

    def pick_open(container: List[OpenPosition]) -> OpenPosition:
        return container[0] if method == CostBasisMethod.FIFO else container[-1]

    def maybe_remove(container: List[OpenPosition], position: OpenPosition) -> None:
        if position.remaining <= ZERO:
            if method == CostBasisMethod.FIFO:
                container.pop(0)
            else:
                container.pop()

    for lot in lots:
        last_price = lot.price
        direction = 1 if lot.qty > ZERO else -1
        qty_remaining = abs(lot.qty)
        opposing = short_open if direction > 0 else long_open
        supporting = long_open if direction > 0 else short_open

        while qty_remaining > ZERO and opposing:
            open_pos = pick_open(opposing)
            matched = min(qty_remaining, open_pos.remaining)
            open_pos.remaining -= matched
            qty_remaining -= matched
            maybe_remove(opposing, open_pos)

        if qty_remaining > ZERO:
            supporting.append(OpenPosition(lot=lot, remaining=qty_remaining, direction=direction))

    if last_price is None:
        raise HTTPException(status_code=404, detail=f"No fills recorded for symbol {symbol}")

    unrealized_details: List[UnrealizedLotDetail] = []
    unrealized_by_symbol: Dict[str, Decimal] = {}

    for open_pos in long_open + short_open:
        quantity = open_pos.remaining * Decimal(open_pos.direction)
        unrealized = open_pos.remaining * (last_price - open_pos.lot.price) * Decimal(open_pos.direction)
        detail = UnrealizedLotDetail(
            account_id=open_pos.lot.account_id,
            symbol=symbol,
            quantity=quantity,
            position_direction="LONG" if open_pos.direction > 0 else "SHORT",
            cost_basis=open_pos.lot.price,
            current_price=last_price,
            unrealized_pnl=unrealized,
            lot_ids=[open_pos.lot.lot_id],
            entry_ts=open_pos.lot.ts,
            method=method,
        )
        unrealized_details.append(detail)
        unrealized_by_symbol[symbol] = unrealized_by_symbol.get(symbol, ZERO) + unrealized

    total_unrealized = sum(unrealized_by_symbol.values(), ZERO)
    account_id = lots[0].account_id if lots else ""
    quantized_details = [
        detail.model_copy(update={"unrealized_pnl": _quantize_usd(detail.unrealized_pnl)})
        for detail in unrealized_details
    ]
    quantized_by_symbol = {symbol: _quantize_usd(value) for symbol, value in unrealized_by_symbol.items()}
    return UnrealizedResponse(
        account_id=account_id,
        method=method,
        total_unrealized_pnl=_quantize_usd(total_unrealized),
        unrealized_by_symbol=quantized_by_symbol,
        lots=quantized_details,
    )


def _realized_average(symbol: str, lots: List[TaxLot]) -> RealizedResponse:
    position = ZERO
    avg_cost = ZERO
    components: List[OpenPosition] = []
    realized_details: List[RealizedLotDetail] = []
    realized_by_symbol: Dict[str, Decimal] = defaultdict(Decimal)

    def sign(value: Decimal) -> int:
        return 1 if value > ZERO else -1 if value < ZERO else 0

    for lot in lots:
        remaining = lot.qty
        while remaining != ZERO:
            if position == ZERO:
                position = remaining
                avg_cost = lot.price
                components = [
                    OpenPosition(lot=lot, remaining=abs(remaining), direction=sign(remaining))
                ]
                remaining = ZERO
            elif sign(position) == sign(remaining):
                new_position = position + remaining
                if new_position == ZERO:
                    avg_cost = ZERO
                    components = []
                    position = ZERO
                    remaining = ZERO
                else:
                    abs_position = abs(position)
                    abs_remaining = abs(remaining)
                    avg_cost = (avg_cost * abs_position + lot.price * abs_remaining) / (
                        abs_position + abs_remaining
                    )
                    components.append(
                        OpenPosition(lot=lot, remaining=abs_remaining, direction=sign(remaining))
                    )
                    position = new_position
                    remaining = ZERO
            else:
                close_qty = min(abs(position), abs(remaining))
                direction = sign(position)
                realized = close_qty * (lot.price - avg_cost) * Decimal(direction)
                entry_ts = components[0].lot.ts if components else lot.ts
                realized_details.append(
                    RealizedLotDetail(
                        account_id=lot.account_id,
                        symbol=symbol,
                        quantity=close_qty,
                        position_direction="LONG" if direction > 0 else "SHORT",
                        entry_price=avg_cost,
                        entry_ts=entry_ts,
                        open_lot_id=None,
                        exit_price=lot.price,
                        exit_ts=lot.ts,
                        close_lot_id=lot.lot_id,
                        realized_pnl=realized,
                        method=CostBasisMethod.AVERAGE,
                    )
                )
                realized_by_symbol[symbol] += realized

                position -= close_qty * Decimal(direction)
                remaining += close_qty * Decimal(direction)

                qty_to_reduce = close_qty
                while qty_to_reduce > ZERO and components:
                    component = components[0]
                    reduction = min(qty_to_reduce, component.remaining)
                    component.remaining -= reduction
                    qty_to_reduce -= reduction
                    if component.remaining <= ZERO:
                        components.pop(0)
                if position == ZERO:
                    avg_cost = ZERO
                    components = []

    total_realized = sum(realized_by_symbol.values(), ZERO)
    quantized_details = [
        detail.model_copy(update={"realized_pnl": _quantize_usd(detail.realized_pnl)})
        for detail in realized_details
    ]
    quantized_by_symbol = {symbol: _quantize_usd(value) for symbol, value in realized_by_symbol.items()}
    return RealizedResponse(
        account_id=lots[0].account_id if lots else "",
        method=CostBasisMethod.AVERAGE,
        total_realized_pnl=_quantize_usd(total_realized),
        realized_by_symbol=quantized_by_symbol,
        lots=quantized_details,
    )


def _unrealized_average(symbol: str, lots: List[TaxLot]) -> UnrealizedResponse:
    position = ZERO
    avg_cost = ZERO
    components: List[OpenPosition] = []
    last_price: Optional[Decimal] = None

    def sign(value: Decimal) -> int:
        return 1 if value > ZERO else -1 if value < ZERO else 0

    for lot in lots:
        last_price = lot.price
        remaining = lot.qty
        while remaining != ZERO:
            if position == ZERO:
                position = remaining
                avg_cost = lot.price
                components = [OpenPosition(lot=lot, remaining=abs(remaining), direction=sign(remaining))]
                remaining = ZERO
            elif sign(position) == sign(remaining):
                new_position = position + remaining
                if new_position == ZERO:
                    position = ZERO
                    avg_cost = ZERO
                    components = []
                else:
                    abs_position = abs(position)
                    abs_remaining = abs(remaining)
                    avg_cost = (avg_cost * abs_position + lot.price * abs_remaining) / (
                        abs_position + abs_remaining
                    )
                    components.append(OpenPosition(lot=lot, remaining=abs_remaining, direction=sign(remaining)))
                    position = new_position
                remaining = ZERO
            else:
                close_qty = min(abs(position), abs(remaining))
                direction = sign(position)
                position -= close_qty * Decimal(direction)
                remaining += close_qty * Decimal(direction)
                qty_to_reduce = close_qty
                while qty_to_reduce > ZERO and components:
                    component = components[0]
                    reduction = min(qty_to_reduce, component.remaining)
                    component.remaining -= reduction
                    qty_to_reduce -= reduction
                    if component.remaining <= ZERO:
                        components.pop(0)
                if position == ZERO:
                    avg_cost = ZERO
                    components = []

    if last_price is None:
        raise HTTPException(status_code=404, detail=f"No fills recorded for symbol {symbol}")

    unrealized_details: List[UnrealizedLotDetail] = []
    unrealized_by_symbol: Dict[str, Decimal] = {}

    if position != ZERO and components:
        direction = 1 if position > ZERO else -1
        unrealized = abs(position) * (last_price - avg_cost) * Decimal(direction)
        lot_ids = [component.lot.lot_id for component in components]
        entry_ts = min(component.lot.ts for component in components)
        detail = UnrealizedLotDetail(
            account_id=lots[0].account_id,
            symbol=symbol,
            quantity=position,
            position_direction="LONG" if direction > 0 else "SHORT",
            cost_basis=avg_cost,
            current_price=last_price,
            unrealized_pnl=unrealized,
            lot_ids=lot_ids,
            entry_ts=entry_ts,
            method=CostBasisMethod.AVERAGE,
        )
        unrealized_details.append(detail)
        unrealized_by_symbol[symbol] = unrealized

    total_unrealized = sum(unrealized_by_symbol.values(), ZERO)
    quantized_details = [
        detail.model_copy(update={"unrealized_pnl": _quantize_usd(detail.unrealized_pnl)})
        for detail in unrealized_details
    ]
    quantized_by_symbol = {symbol: _quantize_usd(value) for symbol, value in unrealized_by_symbol.items()}
    return UnrealizedResponse(
        account_id=lots[0].account_id if lots else "",
        method=CostBasisMethod.AVERAGE,
        total_unrealized_pnl=_quantize_usd(total_unrealized),
        unrealized_by_symbol=quantized_by_symbol,
        lots=quantized_details,
    )


def _merge_realized(responses: Iterable[RealizedResponse], account_id: str, method: CostBasisMethod) -> RealizedResponse:
    merged_details: List[RealizedLotDetail] = []
    realized_by_symbol: Dict[str, Decimal] = {}
    total = ZERO
    for response in responses:
        merged_details.extend(response.lots)
        for symbol, value in response.realized_by_symbol.items():
            realized_by_symbol[symbol] = realized_by_symbol.get(symbol, ZERO) + value
        total += response.total_realized_pnl
    merged_details.sort(key=lambda detail: detail.exit_ts)
    quantized_details = [
        detail.model_copy(update={"realized_pnl": _quantize_usd(detail.realized_pnl)})
        for detail in merged_details
    ]
    quantized_by_symbol = {symbol: _quantize_usd(value) for symbol, value in realized_by_symbol.items()}
    return RealizedResponse(
        account_id=account_id,
        method=method,
        total_realized_pnl=_quantize_usd(total),
        realized_by_symbol=quantized_by_symbol,
        lots=quantized_details,
    )


def _merge_unrealized(responses: Iterable[UnrealizedResponse], account_id: str, method: CostBasisMethod) -> UnrealizedResponse:
    merged_details: List[UnrealizedLotDetail] = []
    unrealized_by_symbol: Dict[str, Decimal] = {}
    total = ZERO
    for response in responses:
        merged_details.extend(response.lots)
        for symbol, value in response.unrealized_by_symbol.items():
            unrealized_by_symbol[symbol] = unrealized_by_symbol.get(symbol, ZERO) + value
        total += response.total_unrealized_pnl
    merged_details.sort(key=lambda detail: detail.entry_ts)
    quantized_details = [
        detail.model_copy(update={"unrealized_pnl": _quantize_usd(detail.unrealized_pnl)})
        for detail in merged_details
    ]
    quantized_by_symbol = {symbol: _quantize_usd(value) for symbol, value in unrealized_by_symbol.items()}
    return UnrealizedResponse(
        account_id=account_id,
        method=method,
        total_unrealized_pnl=_quantize_usd(total),
        unrealized_by_symbol=quantized_by_symbol,
        lots=quantized_details,
    )


def _render_csv(headers: List[str], rows: Iterable[Dict[str, object]]) -> StreamingResponse:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=headers)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    buffer.seek(0)
    return StreamingResponse(iter([buffer.getvalue()]), media_type="text/csv")


@app.post("/taxlots", response_model=TaxLotResponse, status_code=201)
async def create_tax_lot(lot: TaxLotCreate) -> TaxLotResponse:
    """Record a new tax lot representing a fill."""

    domain_lot = lot.to_domain()
    store.add(domain_lot)
    return TaxLotResponse(**domain_lot.__dict__)


@app.get("/taxlots/realized", response_model=RealizedResponse)
async def get_realized_pnl(
    account_id: str = Query(..., description="Account identifier"),
    method: CostBasisMethod = Query(CostBasisMethod.FIFO, description="Cost basis method"),
    symbol: Optional[str] = Query(None, description="Optional symbol filter"),
    export: Optional[str] = Query(None, description="Set to 'csv' to export results"),
) -> Response:
    normalized_symbol: Optional[str] = None
    if symbol is not None:
        normalized_symbol = require_spot_http(symbol, logger=LOGGER)

    lots = store.get(account_id, normalized_symbol)
    if not lots:
        raise HTTPException(status_code=404, detail="No tax lots found for account")

    grouped = _group_by_symbol(lots)
    if method == CostBasisMethod.AVERAGE:
        responses = [_realized_average(sym, sym_lots) for sym, sym_lots in grouped.items()]
    else:
        responses = [_realized_fifo_lifo(sym, sym_lots, method) for sym, sym_lots in grouped.items()]

    merged = _merge_realized(responses, account_id, method)

    if export and export.lower() == "csv":
        headers = [
            "account_id",
            "symbol",
            "quantity",
            "position_direction",
            "entry_price",
            "entry_ts",
            "open_lot_id",
            "exit_price",
            "exit_ts",
            "close_lot_id",
            "realized_pnl",
            "method",
        ]
        rows = [detail.dict() for detail in merged.lots]
        response = _render_csv(headers, rows)
        response.headers["Content-Disposition"] = "attachment; filename=realized_pnl.csv"
        return response

    return merged


@app.get("/taxlots/unrealized", response_model=UnrealizedResponse)
async def get_unrealized_pnl(
    account_id: str = Query(..., description="Account identifier"),
    method: CostBasisMethod = Query(CostBasisMethod.FIFO, description="Cost basis method"),
    symbol: Optional[str] = Query(None, description="Optional symbol filter"),
    export: Optional[str] = Query(None, description="Set to 'csv' to export results"),
) -> Response:
    normalized_symbol: Optional[str] = None
    if symbol is not None:
        normalized_symbol = require_spot_http(symbol, logger=LOGGER)

    lots = store.get(account_id, normalized_symbol)
    if not lots:
        raise HTTPException(status_code=404, detail="No tax lots found for account")

    grouped = _group_by_symbol(lots)
    if method == CostBasisMethod.AVERAGE:
        responses = [_unrealized_average(sym, sym_lots) for sym, sym_lots in grouped.items()]
    else:
        responses = [_unrealized_fifo_lifo(sym, sym_lots, method) for sym, sym_lots in grouped.items()]

    merged = _merge_unrealized(responses, account_id, method)

    if export and export.lower() == "csv":
        headers = [
            "account_id",
            "symbol",
            "quantity",
            "position_direction",
            "cost_basis",
            "current_price",
            "unrealized_pnl",
            "lot_ids",
            "entry_ts",
            "method",
        ]
        rows = []
        for detail in merged.lots:
            row = detail.dict()
            row["lot_ids"] = ";".join(detail.lot_ids)
            rows.append(row)
        response = _render_csv(headers, rows)
        response.headers["Content-Disposition"] = "attachment; filename=unrealized_pnl.csv"
        return response

    return merged
