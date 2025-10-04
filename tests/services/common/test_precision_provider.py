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
            "tick_size": str(tick),
            "lot_step": str(lot),
        }
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

    payload_ref["data"] = _payload(0.001, 5.0)
    provider.refresh(force=True)

    updated = provider.require("ADA-USD")
    assert updated["tick"] == pytest.approx(0.001)
    assert updated["lot"] == pytest.approx(5.0)


def test_precision_provider_missing_symbol_raises() -> None:
    provider = PrecisionMetadataProvider(fetcher=lambda: {}, refresh_interval=0.0)
    provider.refresh(force=True)

    with pytest.raises(PrecisionMetadataUnavailable):
        provider.require("UNKNOWN")


def test_precision_module_import_and_instantiation() -> None:
    module = importlib.import_module("services.common.precision")

    provider = module.PrecisionMetadataProvider()

    assert isinstance(provider, PrecisionMetadataProvider)

