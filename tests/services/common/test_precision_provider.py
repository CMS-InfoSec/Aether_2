from __future__ import annotations

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


def test_precision_provider_refreshes_metadata() -> None:
    payload_ref: dict[str, dict[str, dict[str, str]]] = {"data": _payload(0.0001, 0.1)}

    def _fetch() -> dict[str, dict[str, str]]:
        return payload_ref["data"]

    provider = PrecisionMetadataProvider(fetcher=_fetch, refresh_interval=0.0)
    provider.refresh(force=True)

    first = provider.require("ADA-USD")
    assert first["tick"] == pytest.approx(0.0001)
    assert first["lot"] == pytest.approx(0.1)
    assert first["native_pair"] == "ADA/USD"

    payload_ref["data"] = _payload(0.001, 5.0)
    provider.refresh(force=True)

    updated = provider.require("ADA-USD")
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


def test_precision_provider_missing_symbol_raises() -> None:
    provider = PrecisionMetadataProvider(fetcher=lambda: {}, refresh_interval=0.0)
    provider.refresh(force=True)

    with pytest.raises(PrecisionMetadataUnavailable):
        provider.require("UNKNOWN")


def test_precision_module_import_and_instantiation() -> None:
    module = importlib.import_module("services.common.precision")

    provider = module.PrecisionMetadataProvider()

    assert isinstance(provider, PrecisionMetadataProvider)

