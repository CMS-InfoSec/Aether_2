from decimal import Decimal

import pytest

from services.oms.oms_service import _ShadowPnLTracker


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

    assert real["realized_pnl"] == pytest.approx(400.0)
    assert real["positions"]["BTC-USD"]["quantity"] == pytest.approx(0.6)
    assert real["positions"]["BTC-USD"]["avg_price"] == pytest.approx(20000.0)

    assert shadow["realized_pnl"] == pytest.approx(250.0)
    assert shadow["positions"]["BTC-USD"]["quantity"] == pytest.approx(0.25)
    assert shadow["positions"]["BTC-USD"]["avg_price"] == pytest.approx(20500.0)

    assert len(real["fills"]) == 2
    assert len(shadow["fills"]) == 2
    assert shadow["fills"][0]["side"] == "buy"

    assert snapshot["delta"]["realized_pnl"] == pytest.approx(-150.0)
