from __future__ import annotations

import asyncio
import importlib
from typing import Any, Dict, Mapping

import pytest

from services.common.precision import (
    PrecisionMetadataProvider,
    PrecisionMetadataUnavailable,
)


def _payload(tick: float, lot: float) -> dict[str, dict[str, str]]:
    return {
        "ADAUSD": {
            "wsname": "ADA/USD",

            "base": "ADA",
            "quote": "ZUSD",
            "tick_size": str(tick),
            "lot_step": str(lot),
        },
        "XXBTZEUR": {
            "wsname": "XBT/EUR",
            "base": "XXBT",
            "quote": "ZEUR",
            "tick_size": "0.1",
            "lot_step": "0.0001",
        },
        "USDTUSD": {
            "wsname": "USDT/USD",
            "base": "USDT",
            "quote": "ZUSD",
            "tick_size": "0.0001",
            "lot_step": "1",
        },
    }


@pytest.mark.asyncio
async def test_precision_provider_refreshes_metadata() -> None:
    payload_ref: dict[str, dict[str, dict[str, str]]] = {"data": _payload(0.0001, 0.1)}

    async def _fetch() -> dict[str, dict[str, str]]:
        return payload_ref["data"]


    provider = PrecisionMetadataProvider(fetcher=_fetch, refresh_interval=60.0)
    await provider.refresh(force=True)

    first = await provider.require("ADA-USD")

    assert first["tick"] == pytest.approx(0.0001)
    assert first["lot"] == pytest.approx(0.1)
    assert first["native_pair"] == "ADA/USD"

    payload_ref["data"] = _payload(0.001, 5.0)
    await provider.refresh(force=True)


    updated = await provider.require("ADA-USD")

    assert updated["tick"] == pytest.approx(0.001)
    assert updated["lot"] == pytest.approx(5.0)
    assert updated["native_pair"] == "ADA/USD"


def test_precision_provider_resolves_non_usd_pairs() -> None:
    provider = PrecisionMetadataProvider(fetcher=lambda: _payload(0.001, 1.0), refresh_interval=0.0)
    provider.refresh(force=True)

    eur_precision = provider.require("BTC-EUR")
    assert eur_precision["tick"] == pytest.approx(0.1)
    assert eur_precision["lot"] == pytest.approx(0.0001)
    assert eur_precision["native_pair"] == "XBT/EUR"

    usdt_precision = provider.require("USDT-USD")
    assert usdt_precision["tick"] == pytest.approx(0.0001)
    assert usdt_precision["lot"] == pytest.approx(1.0)
    assert usdt_precision["native_pair"] == "USDT/USD"



def test_precision_provider_handles_non_usd_pairs() -> None:
    payload = {
        "ETHUSDT": {
            "wsname": "ETH/USDT",
            "altname": "ETHUSDT",
            "base": "XETH",
            "quote": "USDT",
            "tick_size": "0.01",
            "lot_step": "0.001",
        },
        "ADAEUR": {
            "wsname": "ADA/EUR",
            "altname": "ADAEUR",
            "base": "ADA",
            "quote": "ZEUR",
            "tick_size": "0.0001",
            "lot_step": "0.1",
        },
    }

    provider = PrecisionMetadataProvider(fetcher=lambda: payload, refresh_interval=0.0)
    provider.refresh(force=True)

    native = provider.resolve_native("ETH-USDT")
    assert native == "ETH/USDT"

    metadata = provider.require_native(native)
    assert metadata["tick"] == pytest.approx(0.01)
    assert metadata["lot"] == pytest.approx(0.001)

    ada_native = provider.resolve_native("ADA-EUR")
    assert ada_native == "ADA/EUR"
    ada_metadata = provider.require("ADA-EUR")
    assert ada_metadata["tick"] == pytest.approx(0.0001)
    assert ada_metadata["lot"] == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_precision_provider_missing_symbol_raises() -> None:
    provider = PrecisionMetadataProvider(fetcher=lambda: {}, refresh_interval=0.0)
    await provider.refresh(force=True)


    with pytest.raises(PrecisionMetadataUnavailable):

        await provider.require("UNKNOWN")


@pytest.mark.asyncio
async def test_precision_provider_coalesces_concurrent_refreshes() -> None:
    payload = _payload(0.01, 0.1)
    gate = asyncio.Event()
    call_order: list[str] = []

    async def _fetch() -> Mapping[str, Any]:
        call_order.append("fetch")
        await gate.wait()
        return payload

    provider = PrecisionMetadataProvider(fetcher=_fetch, refresh_interval=60.0)

    async def _require() -> Dict[str, float]:
        return await provider.require("ADA-USD")

    tasks = [asyncio.create_task(_require()) for _ in range(5)]
    await asyncio.sleep(0)
    gate.set()
    results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=1.0)

    assert len(call_order) == 1
    assert all(result["tick"] == pytest.approx(0.01) for result in results)
    assert all(result["lot"] == pytest.approx(0.1) for result in results)


@pytest.mark.asyncio
async def test_precision_provider_get_normalizes_symbols() -> None:
    provider = PrecisionMetadataProvider(fetcher=lambda: {}, refresh_interval=0.0)

    provider._cache = {  # type: ignore[attr-defined]
        "ADA-USD": {"tick": 0.1, "lot": 1.0, "native_pair": "ADA/USD"}
    }
    provider._aliases = {"ADAUSD": "ADA-USD"}  # type: ignore[attr-defined]
    provider._refresh_interval = float("inf")  # type: ignore[attr-defined]
    provider._last_refresh = provider._time_source()  # type: ignore[attr-defined]

    result = await provider.get("adausd")

    assert result == {"tick": 0.1, "lot": 1.0, "native_pair": "ADA/USD"}


def test_precision_module_import_and_instantiation() -> None:
    module = importlib.import_module("services.common.precision")

    provider = module.PrecisionMetadataProvider()

    assert isinstance(provider, PrecisionMetadataProvider)

