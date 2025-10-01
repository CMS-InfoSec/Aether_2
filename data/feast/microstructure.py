"""Feast feature definitions for market microstructure signals."""
from __future__ import annotations

import datetime as dt

from feast import Entity, FeatureService, FeatureView, Field
from feast.infra.offline_stores.contrib.postgres.postgres_source import PostgreSQLSource
from feast.types import Float64

market_symbol = Entity(
    name="market_symbol",
    join_keys=["symbol"],
    description="Exchange symbol for the traded market",
)

market_microstructure_source = PostgreSQLSource(
    name="market_microstructure_source",
    table="market_microstructure_features",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_at",
)

market_microstructure_feature_view = FeatureView(
    name="market_microstructure_features",
    ttl=dt.timedelta(hours=6),
    entities=[market_symbol],
    schema=[
        Field(name="rolling_vwap", dtype=Float64),
        Field(name="spread", dtype=Float64),
        Field(name="order_book_imbalance", dtype=Float64),
    ],
    online=True,
    source=market_microstructure_source,
    tags={"source": "kafka-md"},
)

market_microstructure_feature_service = FeatureService(
    name="market_microstructure_service",
    features=[market_microstructure_feature_view],
    tags={"team": "research"},
)

__all__ = [
    "market_symbol",
    "market_microstructure_source",
    "market_microstructure_feature_view",
    "market_microstructure_feature_service",
]

