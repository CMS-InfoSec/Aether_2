from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from services.risk.position_sizer import PositionSizer


@dataclass
class _Limits:
    max_nav_pct_per_trade: float = 0.5
    notional_cap: float = 0.0


class _StaticTimescale:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def load_risk_config(self) -> dict[str, float]:
        return {}


class _StaticFeatures:
    def __init__(self, *args, **kwargs) -> None:
        self.requested: list[str] = []

    def fetch_online_features(self, symbol: str) -> dict[str, float]:
        self.requested.append(symbol)
        return {}

    def fee_override(self, symbol: str):  # pragma: no cover - simple stub
        return None


@pytest.mark.asyncio
async def test_position_sizer_quantizes_using_precision() -> None:
    sizer = PositionSizer(
        "acct-1",
        limits=_Limits(),
        timescale=_StaticTimescale(),
        feature_store=_StaticFeatures(),
    )


    result = await sizer.suggest_max_position(
        "ADA-USD",

        nav=2000.0,
        available_balance=2000.0,
        volatility=0.25,
        expected_edge_bps=50.0,
        fee_bps_estimate=1.0,
        price=1875.12,
        regime="trend",
    )

    assert result.reason == "sized"
    multiple = result.size_units / 0.001
    assert multiple == pytest.approx(round(multiple), abs=1e-9)
    assert result.size_units > 0


@pytest.mark.asyncio
async def test_position_sizer_halts_when_precision_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.common import precision as precision_module

    provider = precision_module.PrecisionMetadataProvider(fetcher=lambda: {}, refresh_interval=0.0)

    await provider.refresh(force=True)


    monkeypatch.setattr(precision_module, "precision_provider", provider)
    monkeypatch.setattr("services.risk.position_sizer.precision_provider", provider)

    sizer = PositionSizer(
        "acct-2",
        limits=_Limits(),
        timescale=_StaticTimescale(),
        feature_store=_StaticFeatures(),
    )

    result = await sizer.suggest_max_position(
        "MISSING-USD",
        nav=1000.0,
        available_balance=1000.0,
        volatility=0.2,
        expected_edge_bps=20.0,
        fee_bps_estimate=1.0,
        price=1.0,
    )

    assert result.reason == "precision_metadata_missing"
    assert result.max_size_usd == pytest.approx(0.0)
    assert result.size_units == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_position_sizer_normalizes_spot_instrument() -> None:
    features = _StaticFeatures()
    sizer = PositionSizer(
        "acct-spot",
        limits=_Limits(),
        timescale=_StaticTimescale(),
        feature_store=features,
    )

    result = await sizer.suggest_max_position(
        "eth/usd",
        nav=1500.0,
        available_balance=1500.0,
        volatility=0.3,
        expected_edge_bps=25.0,
        fee_bps_estimate=1.0,
        price=1500.0,
    )

    assert result.symbol == "ETH-USD"
    assert features.requested == ["ETH-USD"]


@pytest.mark.asyncio
async def test_position_sizer_rejects_non_spot_instrument() -> None:
    sizer = PositionSizer(
        "acct-derivative",
        limits=_Limits(),
        timescale=_StaticTimescale(),
        feature_store=_StaticFeatures(),
    )

    with pytest.raises(ValueError, match="spot market instruments"):
        await sizer.suggest_max_position(
            "ETH-PERP",
            nav=1000.0,
            available_balance=1000.0,
            volatility=0.2,
            expected_edge_bps=20.0,
            fee_bps_estimate=1.0,
        )

