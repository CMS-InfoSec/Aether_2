"""Tests for shared intent schemas enforcing spot-only instruments."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from common.schemas.intents import Fill, Order
from services.common.schemas import PortfolioState


def _timestamp() -> datetime:
    return datetime.now(timezone.utc)


def test_order_symbol_normalized_to_spot_pair() -> None:
    order = Order(
        client_id="client-spot",
        account_id="acct-1",
        symbol="eth/usd",
        status="NEW",
        ts=_timestamp(),
    )

    assert order.symbol == "ETH-USD"


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


def test_portfolio_state_normalizes_spot_exposures() -> None:
    state = PortfolioState(
        nav=1_000_000.0,
        loss_to_date=0.0,
        fee_to_date=0.0,
        instrument_exposure={
            "eth/usd": 125_000.0,
            "ETH-USD": 5_000.0,
            "btc-usd": 250_000.0,
        },
    )

    assert state.instrument_exposure == {
        "ETH-USD": pytest.approx(130_000.0),
        "BTC-USD": pytest.approx(250_000.0),
    }


def test_portfolio_state_rejects_derivative_exposures() -> None:
    with pytest.raises(ValidationError, match="spot market symbols"):
        PortfolioState(
            nav=1_000_000.0,
            loss_to_date=0.0,
            fee_to_date=0.0,
            instrument_exposure={"BTC-PERP": 125_000.0},
        )
