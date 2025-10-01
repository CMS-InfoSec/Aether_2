"""Feast repository defining offline/online feature views for Aether."""
from __future__ import annotations

import datetime as dt

from feast import Entity, FeatureService, FeatureView, Field
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.types import Float64

symbol = Entity(name="symbol", join_keys=["symbol"], description="Trading symbol")
account = Entity(name="account", join_keys=["account_id"], description="Trading account identifier")

coingecko_metrics_source = PostgreSQLSource(
    name="coingecko_metrics_source",
    query="""
        SELECT
            symbol,
            event_ts,
            MAX(CASE WHEN feature_name = 'coingecko.market_cap' THEN value END) AS market_cap,
            MAX(CASE WHEN feature_name = 'coingecko.volume_24h' THEN value END) AS volume_24h,
            MAX(CASE WHEN feature_name = 'coingecko.volatility_30d' THEN value END) AS volatility_30d
        FROM features
        WHERE feature_name IN (
            'coingecko.market_cap',
            'coingecko.volume_24h',
            'coingecko.volatility_30d'
        )
        GROUP BY 1, 2
    """,
    timestamp_field="event_ts",
)

coingecko_feature_view = FeatureView(
    name="coingecko_metrics",
    entities=[symbol],
    ttl=dt.timedelta(days=1),
    schema=[
        Field(name="market_cap", dtype=Float64),
        Field(name="volume_24h", dtype=Float64),
        Field(name="volatility_30d", dtype=Float64),
    ],
    online=True,
    source=coingecko_metrics_source,
    tags={"source": "coingecko"},
)

positions_source = PostgreSQLSource(
    name="positions_source",
    table="positions",
    timestamp_field="event_ts",
)

positions_feature_view = FeatureView(
    name="positions_snapshot",
    entities=[account, symbol],
    ttl=dt.timedelta(days=1),
    schema=[
        Field(name="quantity", dtype=Float64),
        Field(name="notional", dtype=Float64),
        Field(name="cost_basis", dtype=Float64),
    ],
    online=True,
    source=positions_source,
    tags={"source": "backoffice"},
)

pnl_source = PostgreSQLSource(
    name="pnl_source",
    table="pnl",
    timestamp_field="event_ts",
)

pnl_feature_view = FeatureView(
    name="pnl_metrics",
    entities=[account, symbol],
    ttl=dt.timedelta(days=7),
    schema=[
        Field(name="realized", dtype=Float64),
        Field(name="unrealized", dtype=Float64),
        Field(name="fees", dtype=Float64),
    ],
    online=True,
    source=pnl_source,
    tags={"source": "backoffice"},
)

core_feature_service = FeatureService(
    name="core_trading_service",
    features=[coingecko_feature_view, positions_feature_view, pnl_feature_view],
)
