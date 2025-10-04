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
            "tick_size": str(tick),
            "lot_step": str(lot),
        }
    }


@pytest.mark.asyncio
async def test_precision_provider_refreshes_metadata() -> None:
    payload_ref: dict[str, dict[str, dict[str, str]]] = {"data": _payload(0.0001, 0.1)}

    def _fetch() -> dict[str, dict[str, str]]:
        return payload_ref["data"]

    provider = PrecisionMetadataProvider(fetcher=_fetch, refresh_interval=60.0)
    await provider.refresh(force=True)

    first = await provider.require("ADA-USD")
    assert first["tick"] == pytest.approx(0.0001)
    assert first["lot"] == pytest.approx(0.1)

    payload_ref["data"] = _payload(0.001, 5.0)
    await provider.refresh(force=True)

    updated = await provider.require("ADA-USD")
    assert updated["tick"] == pytest.approx(0.001)
    assert updated["lot"] == pytest.approx(5.0)


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


def test_precision_module_import_and_instantiation() -> None:
    module = importlib.import_module("services.common.precision")

    provider = module.PrecisionMetadataProvider()

    assert isinstance(provider, PrecisionMetadataProvider)

