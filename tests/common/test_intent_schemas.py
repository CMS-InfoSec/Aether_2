"""Tests for shared intent schemas enforcing spot-only instruments."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from common.schemas.intents import Fill, Order


def _timestamp() -> datetime:
    return datetime.now(timezone.utc)


def test_order_symbol_normalized_to_spot_pair() -> None:
    order = Order(
        client_id="client-spot",
        account_id="acct-1",
        symbol="eth/usdt",
        status="NEW",
        ts=_timestamp(),
    )

    assert order.symbol == "ETH-USDT"


def test_order_rejects_non_spot_symbol() -> None:
    with pytest.raises(ValidationError, match="spot market symbols"):
        Order(
            client_id="client-deriv",
            account_id="acct-2",
            symbol="BTC-PERP",
            status="NEW",
            ts=_timestamp(),
        )


def test_fill_symbol_normalized_to_spot_pair() -> None:
    fill = Fill(
        exchange_order_id="ex-1",
        account_id="acct-1",
        symbol="btc_usd",
        qty=1.0,
        price=25000.0,
        fee=5.0,
        liquidity="MAKER",
        ts=_timestamp(),
    )

    assert fill.symbol == "BTC-USD"


def test_fill_rejects_non_spot_symbol() -> None:
    with pytest.raises(ValidationError, match="spot market symbols"):
        Fill(
            exchange_order_id="ex-deriv",
            account_id="acct-1",
            symbol="ETH-OPTION",
            qty=0.5,
            price=1800.0,
            fee=1.0,
            liquidity="TAKER",
            ts=_timestamp(),
        )
