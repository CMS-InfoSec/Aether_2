from __future__ import annotations

import asyncio
import importlib

import pytest

from services.common.precision import (
    PrecisionMetadataProvider,
    PrecisionMetadataUnavailable,
)


def _payload(tick: float, lot: float) -> dict[str, dict[str, str]]:
    return {
        "ADAUSD": {
            "wsname": "ADA/USD",
            "altname": "ADAUSD",
            "base": "ADA",
            "quote": "ZUSD",
            "tick_size": str(tick),
            "lot_step": str(lot),
        }
    }


@pytest.mark.asyncio
async def test_precision_provider_refreshes_metadata() -> None:
    payload_ref: dict[str, dict[str, dict[str, str]]] = {"data": _payload(0.0001, 0.1)}

    async def _fetch() -> dict[str, dict[str, str]]:
        return payload_ref["data"]

    provider = PrecisionMetadataProvider(fetcher=_fetch, refresh_interval=0.0)
    await provider.refresh(force=True)

    first = await asyncio.to_thread(provider.require, "ADA-USD")
    assert first["tick"] == pytest.approx(0.0001)
    assert first["lot"] == pytest.approx(0.1)

    payload_ref["data"] = _payload(0.001, 5.0)
    await provider.refresh(force=True)

    updated = await asyncio.to_thread(provider.require, "ADA-USD")
    assert updated["tick"] == pytest.approx(0.001)
    assert updated["lot"] == pytest.approx(5.0)



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


def test_precision_provider_missing_symbol_raises() -> None:
    provider = PrecisionMetadataProvider(fetcher=lambda: {}, refresh_interval=0.0)
    provider.refresh(force=True)


    with pytest.raises(PrecisionMetadataUnavailable):
        await asyncio.to_thread(provider.require, "UNKNOWN")


def test_precision_module_import_and_instantiation() -> None:
    module = importlib.import_module("services.common.precision")

    provider = module.PrecisionMetadataProvider()

    assert isinstance(provider, PrecisionMetadataProvider)

