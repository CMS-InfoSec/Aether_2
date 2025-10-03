from __future__ import annotations

import math

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from services.risk.diversification_allocator import (
    DiversificationAllocator,
    PolicyIntent,
    init_diversification_storage,
)
from services.universe.repository import MarketSnapshot, UniverseRepository


ACCOUNT_ID = "company"


def _setup_account_config() -> None:
    timescale = TimescaleAdapter(account_id=ACCOUNT_ID)
    config = timescale.load_risk_config()
    config.update(
        {
            "nav": 1_000_000.0,
            "diversification": {
                "max_concentration_pct_per_asset": 0.3,
                "max_sector_pct": 0.45,
                "correlation_threshold": 0.8,
                "correlation_penalty": 0.1,
                "rebalance_threshold_pct": 0.025,
                "buckets": [
                    {
                        "name": "btc_core",
                        "target_pct": 0.4,
                        "symbols": ["BTC-USD"],
                        "sector": "layer1",
                    },
                    {
                        "name": "top_cap",
                        "target_pct": 0.35,
                        "symbols": ["ETH-USD"],
                        "sector": "layer1",
                    },
                    {
                        "name": "growth",
                        "target_pct": 0.25,
                        "symbols": ["SOL-USD", "ADA-USD"],
                        "sector": "alts",
                    },
                ],
            },
            "correlation_matrix": {
                "BTC-USD": {"ETH-USD": 0.82, "SOL-USD": 0.7, "ADA-USD": 0.65},
                "ETH-USD": {"BTC-USD": 0.82, "SOL-USD": 0.78, "ADA-USD": 0.75},
                "SOL-USD": {"BTC-USD": 0.7, "ETH-USD": 0.78, "ADA-USD": 0.86},
                "ADA-USD": {"BTC-USD": 0.65, "ETH-USD": 0.75, "SOL-USD": 0.86},
            },
        }
    )
    timescale.save_risk_config(config)


def _seed_universe() -> None:
    snapshots = [
        MarketSnapshot(
            base_asset="BTC",
            quote_asset="USD",
            market_cap=1.5e12,
            global_volume_24h=5e10,
            kraken_volume_24h=2e10,
            volatility_30d=0.5,
        ),
        MarketSnapshot(
            base_asset="ETH",
            quote_asset="USD",
            market_cap=8e11,
            global_volume_24h=3e10,
            kraken_volume_24h=1.5e10,
            volatility_30d=0.45,
        ),
        MarketSnapshot(
            base_asset="SOL",
            quote_asset="USD",
            market_cap=1.2e11,
            global_volume_24h=7e9,
            kraken_volume_24h=4e9,
            volatility_30d=0.42,
        ),
        MarketSnapshot(
            base_asset="ADA",
            quote_asset="USD",
            market_cap=9e10,
            global_volume_24h=6e9,
            kraken_volume_24h=3e9,
            volatility_30d=0.41,
        ),
    ]
    UniverseRepository.seed_market_snapshots(snapshots)
    UniverseRepository.seed_fee_overrides(
        {
            "BTC-USD": {"currency": "USD", "maker": 0.1, "taker": 0.15},
            "ETH-USD": {"currency": "USD", "maker": 0.35, "taker": 0.6},
            "SOL-USD": {"currency": "USD", "maker": 0.12, "taker": 0.2},
            "ADA-USD": {"currency": "USD", "maker": 0.12, "taker": 0.2},
            "default": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        }
    )


def _seed_feature_store() -> None:
    store = RedisFeastAdapter._online_feature_store.setdefault(ACCOUNT_ID, {})
    store.update(
        {
            "BTC-USD": {"expected_edge_bps": 18.0},
            "ETH-USD": {"expected_edge_bps": 0.4},
            "SOL-USD": {"expected_edge_bps": 8.0},
            "ADA-USD": {"expected_edge_bps": 6.0},
        }
    )


def _build_allocator():
    engine = create_engine("sqlite:///:memory:", future=True)
    init_diversification_storage(engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    timescale = TimescaleAdapter(account_id=ACCOUNT_ID)
    universe = RedisFeastAdapter(account_id=ACCOUNT_ID)
    return DiversificationAllocator(
        ACCOUNT_ID,
        timescale=timescale,
        universe=universe,
        session_factory=session_factory,
        enable_persistence=False,
    )


@pytest.fixture(autouse=True)
def _setup_environment():
    TimescaleAdapter.reset(account_id=ACCOUNT_ID)
    RedisFeastAdapter._online_feature_store.pop(ACCOUNT_ID, None)
    _setup_account_config()
    _seed_universe()
    _seed_feature_store()
    yield


def test_compute_targets_enforces_caps():
    allocator = _build_allocator()
    targets = allocator.compute_targets(persist=False)

    assert pytest.approx(sum(targets.weights.values()), abs=1e-6) == 1.0
    for symbol, weight in targets.weights.items():
        if symbol == "CASH":
            continue
        assert weight <= 0.3 + 1e-6

    sector_weights = {"layer1": 0.0, "alts": 0.0}
    for symbol, bucket in targets.buckets.items():
        if bucket == "cash":
            continue
        if bucket == "growth":
            sector_weights["alts"] += targets.weights[symbol]
        else:
            sector_weights["layer1"] += targets.weights[symbol]

    assert sector_weights["layer1"] <= 0.45 + 1e-6
    assert sector_weights["alts"] <= 0.45 + 1e-6


def test_adjust_intent_scales_down_when_concentration_exceeded():
    allocator = _build_allocator()
    # Existing BTC exposure representing 20% of NAV.
    TimescaleAdapter(account_id=ACCOUNT_ID).record_instrument_exposure(
        "BTC-USD", 200_000.0
    )
    intent = PolicyIntent(symbol="BTC-USD", side="BUY", notional=200_000.0, expected_edge_bps=18.0)

    adjustment = allocator.adjust_intent_for_diversification(intent)

    assert adjustment.reduced is True
    assert adjustment.approved_symbol == "BTC-USD"
    # Cap should restrict BTC to 30% of NAV => additional 100k notional.
    assert math.isclose(adjustment.notional, 100_000.0, rel_tol=1e-3)


def test_rebalance_plan_respects_fees_and_threshold():
    allocator = _build_allocator()
    # Current exposures in USD notionals.
    timescale = TimescaleAdapter(account_id=ACCOUNT_ID)
    timescale.record_instrument_exposure("BTC-USD", 350_000.0)
    timescale.record_instrument_exposure("ETH-USD", 320_000.0)
    timescale.record_instrument_exposure("SOL-USD", 280_000.0)

    plan = allocator.generate_rebalance_plan()

    symbols = {instruction.symbol: instruction for instruction in plan.instructions}

    # BTC should be reduced due to overweight.
    btc_instruction = symbols.get("BTC-USD")
    assert btc_instruction is not None
    assert btc_instruction.side == "SELL"
    assert btc_instruction.expected_edge_bps and btc_instruction.expected_edge_bps > btc_instruction.fee_bps

    # ETH should be skipped because fee exceeds expected edge.
    assert "ETH-USD" not in symbols

    # SOL diff is below threshold and should not trigger a trade.
    assert "SOL-USD" not in symbols

