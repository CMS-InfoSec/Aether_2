from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from services.oms.impact_store import ImpactAnalyticsStore


@pytest.mark.asyncio
async def test_record_fill_preserves_decimal_precision() -> None:
    store = ImpactAnalyticsStore()

    now = datetime.now(timezone.utc)
    qty = Decimal("0.00000051")
    avg_px = Decimal("27123.123456789")
    mid_px = Decimal("27123.123456788")
    impact = Decimal("12.3456789")

    await store.record_fill(
        account_id="acct-123",
        client_order_id="order-abc",
        symbol="BTC/USD",
        side="buy",
        filled_qty=qty,
        avg_price=avg_px,
        pre_trade_mid=mid_px,
        impact_bps=impact,
        recorded_at=now,
    )

    fills = store._memory[("acct-123", "BTC/USD")]
    assert fills, "expected fill to be stored in-memory"

    fill = fills[-1]
    assert fill.filled_qty == qty
    assert fill.avg_price == avg_px
    assert fill.pre_trade_mid == mid_px
    assert fill.impact_bps == impact
    assert fill.simulated is False


def test_build_curve_uses_decimal_averages() -> None:
    store = ImpactAnalyticsStore()

    rows = [
        (Decimal("0.00000051"), Decimal("12.3456789")),
        (Decimal("0.00000049"), Decimal("12.3456788")),
    ]

    points = store._build_curve(rows, bucket_count=1)

    assert len(points) == 1
    point = points[0]
    assert point["size"] == pytest.approx(float(Decimal("0.0000005")))
    assert point["impact_bps"] == pytest.approx(float(Decimal("12.34567885")))
