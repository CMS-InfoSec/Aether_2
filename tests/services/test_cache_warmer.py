from __future__ import annotations

from typing import Iterable, List

import pytest

from services.core.cache_warmer import CacheWarmer


class _StubRedis:
    def __init__(self, instruments: Iterable[str]):
        self._instruments: List[str] = list(instruments)

    def approved_instruments(self) -> List[str]:
        return list(self._instruments)


@pytest.mark.asyncio
async def test_select_instruments_filters_non_spot_pairs() -> None:
    warmer = CacheWarmer(
        most_used_instruments=("btc-usd", "ETH-PERP", "sol_usd", ""),
        max_instruments=10,
    )
    redis = _StubRedis(["btc-perp", "ADA-USD", "ETH-USD", "ADA-USD"])

    instruments = warmer._select_instruments(redis)

    assert instruments == ["BTC-USD", "SOL-USD", "ADA-USD", "ETH-USD"]
