from decimal import Decimal

from services.oms.shadow_oms import _ShadowPnLTracker


def test_shadow_pnl_tracker_records_real_and_shadow_fills() -> None:
    tracker = _ShadowPnLTracker()

    tracker.record_fill(
        symbol="BTC-USD",
        side="buy",
        quantity=Decimal("1"),
        price=Decimal("20000"),
        shadow=False,
    )
    tracker.record_fill(
        symbol="BTC-USD",
        side="sell",
        quantity=Decimal("0.4"),
        price=Decimal("21000"),
        shadow=False,
    )

    tracker.record_fill(
        symbol="BTC-USD",
        side="buy",
        quantity=Decimal("0.5"),
        price=Decimal("20500"),
        shadow=True,
    )
    tracker.record_fill(
        symbol="BTC-USD",
        side="sell",
        quantity=Decimal("0.25"),
        price=Decimal("21500"),
        shadow=True,
    )

    snapshot = tracker.snapshot()

    real = snapshot["real"]
    shadow = snapshot["shadow"]

    assert Decimal(real["realized_pnl"]) == Decimal("400")
    assert Decimal(real["positions"]["BTC-USD"]["quantity"]) == Decimal("0.6")
    assert Decimal(real["positions"]["BTC-USD"]["avg_price"]) == Decimal("20000")

    assert Decimal(shadow["realized_pnl"]) == Decimal("250")
    assert Decimal(shadow["positions"]["BTC-USD"]["quantity"]) == Decimal("0.25")
    assert Decimal(shadow["positions"]["BTC-USD"]["avg_price"]) == Decimal("20500")

    assert len(real["fills"]) == 2
    assert len(shadow["fills"]) == 2
    assert shadow["fills"][0]["side"] == "buy"

    assert Decimal(snapshot["delta"]["realized_pnl"]) == Decimal("-150")


def test_shadow_pnl_tracker_preserves_high_precision() -> None:
    tracker = _ShadowPnLTracker()

    qty = Decimal("0.00000051")
    price = Decimal("27123.12345678")
    fee = Decimal("0.00000000012")
    slippage = Decimal("0.0123456789")

    tracker.record_fill(
        symbol="BTC-USD",
        side="buy",
        quantity=qty,
        price=price,
        shadow=False,
        fee=fee,
        slippage_bps=slippage,
    )

    snapshot = tracker.snapshot()
    real = snapshot["real"]

    assert Decimal(real["realized_pnl"]) == Decimal("0")
    assert Decimal(real["fees"]) == fee
    assert Decimal(real["slippage"]) == slippage

    position = real["positions"]["BTC-USD"]
    assert Decimal(position["quantity"]) == qty
    assert Decimal(position["avg_price"]) == price

    recorded_fill = real["fills"][0]
    assert Decimal(recorded_fill["quantity"]) == qty
    assert Decimal(recorded_fill["price"]) == price
    assert Decimal(recorded_fill["fee"]) == fee
    assert Decimal(recorded_fill["slippage_bps"]) == slippage
