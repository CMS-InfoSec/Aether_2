"""Standalone FastAPI application for generating an approved trading universe.

The service demonstrates how a more fully fledged implementation could ingest
market data from CoinGecko together with trading metadata from Kraken, apply a
set of configurable thresholds, and expose the resulting instruments via a
simple API.  Manual overrides are persisted to a stubbed in-memory store while
still logging audit events so downstream systems can react when operators force
include or exclude instruments.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger("universe_service")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Universe Selection Service")


class UniverseThresholds(BaseModel):
    """Thresholds that incoming assets must satisfy before being approved."""

    min_market_cap: float = Field(..., description="Minimum market capitalisation in USD")
    min_volume: float = Field(..., description="Minimum 24h traded volume in USD")
    min_volatility: float = Field(..., description="Minimum annualised volatility as a decimal")


DEFAULT_THRESHOLDS = UniverseThresholds(
    min_market_cap=1_000_000_000.0,
    min_volume=50_000_000.0,
    min_volatility=0.01,
)


class MarketStat(BaseModel):
    """Subset of CoinGecko market statistics required for universe selection."""

    symbol: str
    market_cap: float
    volume_24h: float
    volatility: float


class KrakenAssetPair(BaseModel):
    """Minimal view of a Kraken trading pair."""

    symbol: str
    base: str
    quote: str
    status: str = Field(default="online")

    @property
    def is_online(self) -> bool:
        return self.status.lower() == "online"


class UniverseResponse(BaseModel):
    symbols: List[str]
    generated_at: datetime
    thresholds_used: UniverseThresholds


class OverrideRequest(BaseModel):
    symbol: str
    enabled: bool
    reason: str


class OverrideRecord(OverrideRequest):
    updated_at: datetime


class InMemoryOverrideStore:
    """A stub persistence layer for manual overrides."""

    def __init__(self) -> None:
        self._storage: Dict[str, OverrideRecord] = {}

    def all(self) -> Dict[str, OverrideRecord]:
        return dict(self._storage)

    def upsert(self, override: OverrideRequest) -> OverrideRecord:
        symbol = override.symbol.upper()
        record = OverrideRecord(
            symbol=symbol,
            enabled=override.enabled,
            reason=override.reason,
            updated_at=datetime.now(timezone.utc),
        )
        self._storage[symbol] = record
        return record


override_store = InMemoryOverrideStore()


def load_coingecko_stats() -> Iterable[MarketStat]:
    """Stub loader returning a deterministic CoinGecko payload."""

    # In a production system this function would call CoinGecko's API.
    sample_payload = [
        {"symbol": "BTC", "market_cap": 600_000_000_000.0, "volume_24h": 40_000_000_000.0, "volatility": 0.04},
        {"symbol": "ETH", "market_cap": 250_000_000_000.0, "volume_24h": 20_000_000_000.0, "volatility": 0.03},
        {"symbol": "SOL", "market_cap": 45_000_000_000.0, "volume_24h": 3_000_000_000.0, "volatility": 0.05},
        {"symbol": "DOGE", "market_cap": 12_000_000_000.0, "volume_24h": 1_200_000_000.0, "volatility": 0.09},
        {"symbol": "ADA", "market_cap": 14_000_000_000.0, "volume_24h": 800_000_000.0, "volatility": 0.02},
    ]
    return [MarketStat(**item) for item in sample_payload]


def load_kraken_asset_pairs() -> Iterable[KrakenAssetPair]:
    """Stub loader returning Kraken trading metadata."""

    sample_pairs = [
        {"symbol": "BTCUSD", "base": "BTC", "quote": "USD", "status": "online"},
        {"symbol": "ETHUSD", "base": "ETH", "quote": "USD", "status": "online"},
        {"symbol": "SOLUSD", "base": "SOL", "quote": "USD", "status": "online"},
        {"symbol": "DOGEUSD", "base": "DOGE", "quote": "USD", "status": "maintenance"},
        {"symbol": "ADAUSD", "base": "ADA", "quote": "USD", "status": "online"},
    ]
    return [KrakenAssetPair(**item) for item in sample_pairs]


def apply_thresholds(
    market_stats: Iterable[MarketStat],
    asset_pairs: Iterable[KrakenAssetPair],
    thresholds: UniverseThresholds,
    overrides: Dict[str, OverrideRecord],
) -> List[str]:
    """Compute the list of approved symbols using configured thresholds and overrides."""

    stats_by_symbol = {stat.symbol.upper(): stat for stat in market_stats}
    approved: List[str] = []

    for pair in asset_pairs:
        symbol = pair.symbol.upper()
        stats = stats_by_symbol.get(pair.base.upper())
        override = overrides.get(symbol)

        if override:
            if override.enabled:
                approved.append(symbol)
            continue

        if not pair.is_online or stats is None:
            continue

        meets_requirements = (
            stats.market_cap >= thresholds.min_market_cap
            and stats.volume_24h >= thresholds.min_volume
            and stats.volatility >= thresholds.min_volatility
        )
        if meets_requirements:
            approved.append(symbol)

    # Manual enables for pairs without metadata should still be returned.
    for symbol, override in overrides.items():
        if override.enabled and symbol.upper() not in approved:
            approved.append(symbol.upper())

    approved.sort()
    return approved


@app.get("/universe/approved", response_model=UniverseResponse)
def get_approved_universe() -> UniverseResponse:
    thresholds = DEFAULT_THRESHOLDS
    overrides = override_store.all()

    market_stats = load_coingecko_stats()
    asset_pairs = load_kraken_asset_pairs()

    symbols = apply_thresholds(market_stats, asset_pairs, thresholds, overrides)

    response = UniverseResponse(
        symbols=symbols,
        generated_at=datetime.now(timezone.utc),
        thresholds_used=thresholds,
    )
    return response


@app.post("/universe/override", response_model=OverrideRecord, status_code=201)
def set_override(override: OverrideRequest) -> OverrideRecord:
    if not override.reason.strip():
        raise HTTPException(status_code=400, detail="Override reason must not be empty")

    record = override_store.upsert(override)
    logger.info(
        "Manual override applied", extra={"symbol": record.symbol, "enabled": record.enabled, "reason": record.reason}
    )
    return record


__all__ = [
    "app",
    "apply_thresholds",
    "get_approved_universe",
    "load_coingecko_stats",
    "load_kraken_asset_pairs",
    "set_override",
]
