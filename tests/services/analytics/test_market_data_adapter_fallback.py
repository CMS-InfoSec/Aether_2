from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Sequence

from services.analytics.market_data_store import TimescaleMarketDataAdapter


class _FakeResult:
    def __init__(self, rows: Iterable[Sequence[object]]):
        self._rows: List[Sequence[object]] = list(rows)

    def fetchall(self) -> List[Sequence[object]]:
        return list(self._rows)

    def fetchone(self) -> Sequence[object] | None:
        return self._rows[0] if self._rows else None


class _FakeConnection:
    def __init__(self, results: List[_FakeResult]) -> None:
        self._results = results

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        return None

    def execute(self, *args: object, **kwargs: object) -> _FakeResult:
        if not self._results:
            raise AssertionError("No more fake results available")
        return self._results.pop(0)


class _FakeEngine:
    def __init__(self, results: List[_FakeResult]) -> None:
        self._results = results

    def connect(self) -> _FakeConnection:
        return _FakeConnection(self._results)


def test_timescale_adapter_handles_result_without_all_methods() -> None:
    now = datetime.now(timezone.utc)

    trades_result = _FakeResult(
        [
            ("buy", 1.25, 20050.0, now),
            ("sell", 0.75, 20010.0, now - timedelta(seconds=15)),
        ]
    )
    orderbook_result = _FakeResult([
        (
            [[20045.0, 1.0], [20040.0, 2.0]],
            [[20055.0, 1.5], [20060.0, 1.0]],
            now,
        )
    ])
    prices_result = _FakeResult(
        [
            (20050.0, now),
            (20010.0, now - timedelta(minutes=1)),
        ]
    )

    engine = _FakeEngine([trades_result, orderbook_result, prices_result])
    adapter = TimescaleMarketDataAdapter(engine=engine)

    trades = adapter.recent_trades("BTC-USD", window=60)
    assert len(trades) == 2
    assert trades[0].side == "buy"
    assert trades[1].price == 20010.0

    book = adapter.order_book_snapshot("BTC-USD", depth=10)
    assert book["bids"][0][0] == 20045.0
    assert book["asks"][1][1] == 1.0

    prices = adapter.price_history("BTC-USD", length=2)
    assert prices == [20010.0, 20050.0]
    assert adapter.latest_price_timestamp("BTC-USD") == now
