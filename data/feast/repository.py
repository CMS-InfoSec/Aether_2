"""Feast feature repository definitions for Aether."""
from __future__ import annotations

from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field
from feast.infra.offline_stores.contrib.postgres.postgres_source import PostgreSQLSource
from feast.types import Float64

asset = Entity(name="asset", join_keys=["asset_id"], description="Asset identifier from the whitelist")
account = Entity(name="account", join_keys=["account_id"], description="Trading account identifier")
market = Entity(name="market", join_keys=["market"], description="Market symbol")

asset_market_source = PostgreSQLSource(
    name="asset_market_source",
    query="""
        SELECT
            entity_id AS asset_id,
            event_timestamp,
            created_at,
            value AS market_cap,
            (metadata->>'price')::DOUBLE PRECISION AS spot_price,
            (metadata->>'volume_24h')::DOUBLE PRECISION AS volume_24h
        FROM features
        WHERE feature_name = 'coingecko_market_cap'
    """,
    timestamp_field="event_timestamp",
    created_timestamp_column="created_at",
)

asset_market_features = FeatureView(
    name="asset_market_features",
    entities=[asset],
    ttl=timedelta(days=7),
    schema=[
        Field(name="market_cap", dtype=Float64),
        Field(name="spot_price", dtype=Float64),
        Field(name="volume_24h", dtype=Float64),
    ],
    online=True,
    source=asset_market_source,
    tags={"source": "coingecko"},
)

market_ohlcv_source = PostgreSQLSource(
    name="market_ohlcv_source",
    query="""
        SELECT
            market,
            bucket_start AS event_timestamp,
            bucket_start AS created_at,
            open,
            high,
            low,
            close,
            volume
        FROM ohlcv_bars
    """,
    timestamp_field="event_timestamp",
    created_timestamp_column="created_at",
)

market_ohlcv_features = FeatureView(
    name="market_ohlcv_features",
    entities=[market],
    ttl=timedelta(days=30),
    schema=[
        Field(name="open", dtype=Float64),
        Field(name="high", dtype=Float64),
        Field(name="low", dtype=Float64),
        Field(name="close", dtype=Float64),
        Field(name="volume", dtype=Float64),
    ],
    online=True,
    source=market_ohlcv_source,
    tags={"source": "kraken"},
)

account_position_source = PostgreSQLSource(
    name="account_position_source",
    query="""
        SELECT
            account_id,
            market,
            as_of AS event_timestamp,
            as_of AS created_at,
            quantity,
            entry_price
        FROM positions
    """,
    timestamp_field="event_timestamp",
    created_timestamp_column="created_at",
)

account_position_features = FeatureView(
    name="account_position_features",
    entities=[account, market],
    ttl=timedelta(days=14),
    schema=[
        Field(name="quantity", dtype=Float64),
        Field(name="entry_price", dtype=Float64),
    ],
    online=True,
    source=account_position_source,
    tags={"source": "orders"},
)

asset_market_service = FeatureService(
    name="asset_market_service",
    features=[asset_market_features, market_ohlcv_features],
    tags={"team": "data-platform"},
)

account_trading_service = FeatureService(
    name="account_trading_service",
    features=[account_position_features, market_ohlcv_features],
    tags={"team": "trading"},
)

__all__ = [
    "asset",
    "account",
    "market",
    "asset_market_features",
    "market_ohlcv_features",
    "account_position_features",
    "asset_market_service",
    "account_trading_service",
]
