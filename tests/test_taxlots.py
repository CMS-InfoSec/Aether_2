"""Unit tests for the tax lot accounting helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from taxlots import (
    CostBasisMethod,
    TaxLot,
    TaxLotCreate,
    app,
    store,
    _quantize_usd,
    _realized_average,
    _realized_fifo_lifo,
    _unrealized_average,
    _unrealized_fifo_lifo,
)


@pytest.fixture
def high_precision_lots() -> list[TaxLot]:
    """Create a trio of high precision Kraken-style fills."""

    ts1 = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    ts2 = datetime(2024, 1, 1, 12, 1, tzinfo=timezone.utc)
    ts3 = datetime(2024, 1, 1, 12, 2, tzinfo=timezone.utc)

    buy_one = TaxLot(
        account_id="acct-kraken",
        symbol="BTC-USD",
        qty=Decimal("0.00000051"),
        price=Decimal("30000.12345678"),
        ts=ts1,
        lot_id="lot-1",
    )
    buy_two = TaxLot(
        account_id="acct-kraken",
        symbol="BTC-USD",
        qty=Decimal("0.00000049"),
        price=Decimal("30500.87654321"),
        ts=ts2,
        lot_id="lot-2",
    )
    sell = TaxLot(
        account_id="acct-kraken",
        symbol="BTC-USD",
        qty=Decimal("-0.00000060"),
        price=Decimal("31000.99999999"),
        ts=ts3,
        lot_id="lot-3",
    )
    return [buy_one, buy_two, sell]


def test_taxlot_create_parses_decimal_strings() -> None:
    """Ensure API payloads parse decimal string fields without precision loss."""

    lot = TaxLotCreate(
        account_id="acct-kraken",
        symbol="btc-usd",
        qty="0.00000051",
        price="31000.12345678",
        ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    domain = lot.to_domain()

    assert domain.symbol == "BTC-USD"
    assert domain.qty == Decimal("0.00000051")
    assert domain.price == Decimal("31000.12345678")


def test_taxlot_create_rejects_non_spot_symbol() -> None:
    """Derivatives or leveraged pairs should be rejected during validation."""

    with pytest.raises(ValidationError) as excinfo:
        TaxLotCreate(
            account_id="acct-kraken",
            symbol="btc-perp",
            qty="0.1",
            price="27000",
            ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )

    assert "Only spot market instruments are supported" in str(excinfo.value)


def test_realized_endpoint_rejects_non_spot_symbol_filter() -> None:
    """HTTP filter parameters must also comply with the spot-only policy."""

    store._lots.clear()
    lot = TaxLotCreate(
        account_id="acct-kraken",
        symbol="eth/usd",
        qty="0.5",
        price="1800",
        ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
    ).to_domain()
    store.add(lot)

    client = TestClient(app)
    response = client.get(
        "/taxlots/realized",
        params={
            "account_id": "acct-kraken",
            "symbol": "eth-perp",
        },
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "symbol 'ETH-PERP' is not a supported USD spot market instrument"

    store._lots.clear()


def test_fifo_precision_matches_decimal_math(high_precision_lots: list[TaxLot]) -> None:
    """FIFO realized/unrealized values should match high precision decimal math."""

    response = _realized_fifo_lifo("BTC-USD", high_precision_lots, CostBasisMethod.FIFO)

    sell_price = Decimal("31000.99999999")
    buy_one_price = Decimal("30000.12345678")
    buy_two_price = Decimal("30500.87654321")
    matched_first = Decimal("0.00000051")
    matched_second = Decimal("0.00000009")
    expected_first_realized = _quantize_usd(matched_first * (sell_price - buy_one_price))
    expected_second_realized = _quantize_usd(matched_second * (sell_price - buy_two_price))
    expected_total = _quantize_usd(expected_first_realized + expected_second_realized)

    assert response.total_realized_pnl == expected_total
    assert response.realized_by_symbol["BTC-USD"] == expected_total
    assert response.lots[0].realized_pnl == expected_first_realized
    assert response.lots[1].realized_pnl == expected_second_realized

    unrealized = _unrealized_fifo_lifo("BTC-USD", high_precision_lots, CostBasisMethod.FIFO)
    remaining_qty = Decimal("0.00000040")
    expected_unrealized = _quantize_usd(remaining_qty * (sell_price - buy_two_price))

    assert unrealized.total_unrealized_pnl == expected_unrealized
    assert unrealized.unrealized_by_symbol["BTC-USD"] == expected_unrealized
    assert unrealized.lots[0].unrealized_pnl == expected_unrealized


def test_lifo_precision_matches_decimal_math(high_precision_lots: list[TaxLot]) -> None:
    """LIFO calculations should remain in Decimal space and quantize consistently."""

    response = _realized_fifo_lifo("BTC-USD", high_precision_lots, CostBasisMethod.LIFO)

    sell_price = Decimal("31000.99999999")
    buy_one_price = Decimal("30000.12345678")
    buy_two_price = Decimal("30500.87654321")
    matched_first = Decimal("0.00000049")
    matched_second = Decimal("0.00000011")
    expected_first_realized = _quantize_usd(matched_first * (sell_price - buy_two_price))
    expected_second_realized = _quantize_usd(matched_second * (sell_price - buy_one_price))
    expected_total = _quantize_usd(expected_first_realized + expected_second_realized)

    assert response.total_realized_pnl == expected_total
    assert response.realized_by_symbol["BTC-USD"] == expected_total
    assert response.lots[0].realized_pnl == expected_first_realized
    assert response.lots[1].realized_pnl == expected_second_realized

    unrealized = _unrealized_fifo_lifo("BTC-USD", high_precision_lots, CostBasisMethod.LIFO)
    remaining_qty = Decimal("0.00000040")
    expected_unrealized = _quantize_usd(remaining_qty * (sell_price - buy_one_price))

    assert unrealized.total_unrealized_pnl == expected_unrealized
    assert unrealized.unrealized_by_symbol["BTC-USD"] == expected_unrealized
    assert unrealized.lots[0].unrealized_pnl == expected_unrealized


def test_average_precision_matches_decimal_math(high_precision_lots: list[TaxLot]) -> None:
    """Average cost method should align with Decimal-weighted computations."""

    response = _realized_average("BTC-USD", high_precision_lots)

    sell_price = Decimal("31000.99999999")
    total_qty = Decimal("0.00000100")
    buy_one_price = Decimal("30000.12345678")
    buy_two_price = Decimal("30500.87654321")
    avg_cost = (buy_one_price * Decimal("0.00000051") + buy_two_price * Decimal("0.00000049")) / total_qty
    realized_qty = Decimal("0.00000060")
    expected_realized = _quantize_usd(realized_qty * (sell_price - avg_cost))

    assert response.total_realized_pnl == expected_realized
    assert response.realized_by_symbol["BTC-USD"] == expected_realized
    assert response.lots[0].realized_pnl == expected_realized

    unrealized = _unrealized_average("BTC-USD", high_precision_lots)
    remaining_qty = total_qty - realized_qty
    expected_unrealized = _quantize_usd(remaining_qty * (sell_price - avg_cost))

    assert unrealized.total_unrealized_pnl == expected_unrealized
    assert unrealized.unrealized_by_symbol["BTC-USD"] == expected_unrealized
    assert unrealized.lots[0].unrealized_pnl == expected_unrealized
