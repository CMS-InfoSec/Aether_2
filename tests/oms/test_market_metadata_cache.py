import sys
import types

import pytest

if "aiohttp" not in sys.modules:
    class _StubSession:
        async def __aenter__(self) -> "_StubSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def close(self) -> None:
            return None

        async def post(self, *args, **kwargs):
            raise RuntimeError("aiohttp stub invoked")

        async def get(self, *args, **kwargs):
            raise RuntimeError("aiohttp stub invoked")

    class _ClientTimeout:
        def __init__(self, total: float | None = None) -> None:
            self.total = total

    aiohttp_stub = types.SimpleNamespace(
        ClientSession=lambda *args, **kwargs: _StubSession(),
        ClientTimeout=_ClientTimeout,
        ClientError=Exception,
    )
    sys.modules["aiohttp"] = aiohttp_stub

from services.oms import main


@pytest.mark.asyncio
async def test_market_metadata_cache_skips_invalid_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "ETHUSD": {
            "wsname": "ETH/USD",
            "base": "XETH",
            "quote": "ZUSD",
            "tick_size": "0.10",
            "lot_step": "0.01",
        },
        "FOOUSD": {
            "wsname": "FOO/USD",
            "base": "XFOO",
            "quote": "ZUSD",
            "tick_size": "not-a-number",
            "pair_decimals": "bad",
            "lot_step": "1",
        },
    }

    async def _fake_fetch_asset_pairs() -> dict:
        return payload

    cache = main.MarketMetadataCache(refresh_interval=0.0)
    monkeypatch.setattr(main, "_fetch_asset_pairs", _fake_fetch_asset_pairs)

    await cache.refresh()

    snapshot = await cache.snapshot()

    assert snapshot == {"ETH-USD": {"tick": 0.1, "lot": 0.01}}
    assert "FOO-USD" not in snapshot
