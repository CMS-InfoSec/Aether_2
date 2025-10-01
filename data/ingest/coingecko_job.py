"""Batch ingestion job for CoinGecko metrics and whitelist publication."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
from dataclasses import dataclass
from typing import Iterable, List, Mapping

import requests
from sqlalchemy import Boolean, Column, DateTime, JSON, MetaData, Numeric, String, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine, create_engine

try:
    from nats.aio.client import Client as NATS
except ImportError:  # pragma: no cover - optional dependency
    NATS = None  # type: ignore

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

COINGECKO_API = os.getenv("COINGECKO_API", "https://api.coingecko.com/api/v3")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://localhost:5432/aether")
WHITELIST_TOPIC = os.getenv("WHITELIST_TOPIC", "universe.whitelist")
NATS_SERVERS = os.getenv("NATS_SERVERS", "nats://localhost:4222").split(",")

metadata = MetaData()
whitelist_table = Table(
    "universe_whitelist",
    metadata,
    Column("asset_id", String, primary_key=True),
    Column("as_of", DateTime(timezone=True), primary_key=True),
    Column("source", String, nullable=False),
    Column("approved", Boolean, nullable=False),
    Column("metadata", JSON, nullable=False),
)
metrics_table = Table(
    "features",
    metadata,
    Column("feature_name", String, primary_key=True),
    Column("entity_id", String, primary_key=True),
    Column("event_timestamp", DateTime(timezone=True), primary_key=True),
    Column("value", Numeric),
    Column("created_at", DateTime(timezone=True), nullable=False, default=dt.datetime.utcnow),
    Column("metadata", JSON, nullable=False),
)


@dataclass
class AssetMetric:
    asset_id: str
    market_cap: float
    volume_24h: float
    price: float
    fetched_at: dt.datetime


class CoinGeckoClient:
    """Client responsible for pulling metrics from CoinGecko."""

    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()

    def fetch_top_assets(self, vs_currency: str = "usd", limit: int = 100) -> List[AssetMetric]:
        params = {
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": limit,
            "page": 1,
            "sparkline": "false",
        }
        url = f"{COINGECKO_API}/coins/markets"
        LOGGER.info("Fetching CoinGecko market data", extra={"url": url, "params": params})
        response = self.session.get(url, params=params, timeout=60)
        response.raise_for_status()
        fetched_at = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        metrics = [
            AssetMetric(
                asset_id=item["symbol"].upper(),
                market_cap=float(item.get("market_cap", 0.0)),
                volume_24h=float(item.get("total_volume", 0.0)),
                price=float(item.get("current_price", 0.0)),
                fetched_at=fetched_at,
            )
            for item in response.json()
        ]
        LOGGER.info("Fetched %d CoinGecko assets", len(metrics))
        return metrics


def upsert_metrics(engine: Engine, metrics: Iterable[AssetMetric]) -> None:
    metrics_list = list(metrics)
    if not metrics_list:
        LOGGER.info('No CoinGecko metrics to persist')
        return
    with engine.begin() as connection:
        for metric in metrics_list:
            stmt = pg_insert(metrics_table).values(
                feature_name="coingecko_market_cap",
                entity_id=metric.asset_id,
                event_timestamp=metric.fetched_at,
                value=metric.market_cap,
                metadata={
                    "price": metric.price,
                    "volume_24h": metric.volume_24h,
                    "source": "coingecko",
                },
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    metrics_table.c.feature_name,
                    metrics_table.c.entity_id,
                    metrics_table.c.event_timestamp,
                ],
                set_={
                    "value": stmt.excluded.value,
                    "metadata": stmt.excluded.metadata,
                },
            )
            connection.execute(stmt)
        LOGGER.info("Persisted CoinGecko metrics", extra={"count": len(metrics_list)})


def publish_whitelist(engine: Engine, assets: Iterable[AssetMetric]) -> List[Mapping[str, object]]:
    asset_list = list(assets)
    whitelist_records: List[Mapping[str, object]] = []
    if not asset_list:
        LOGGER.info('No assets to whitelist from CoinGecko')
        return []
    as_of = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    with engine.begin() as connection:
        for asset in asset_list:
            record = {
                "asset_id": asset.asset_id,
                "as_of": as_of,
                "source": "coingecko",
                "approved": True,
                "metadata": {
                    "market_cap": asset.market_cap,
                    "price": asset.price,
                },
            }
            whitelist_records.append(record)
            stmt = pg_insert(whitelist_table).values(**record)
            stmt = stmt.on_conflict_do_update(
                index_elements=[whitelist_table.c.asset_id, whitelist_table.c.as_of],
                set_={"metadata": stmt.excluded.metadata, "approved": stmt.excluded.approved},
            )
            connection.execute(stmt)
    LOGGER.info("Updated whitelist", extra={'count': len(whitelist_records)})
    return whitelist_records


async def publish_to_nats(payloads: Iterable[Mapping[str, object]]) -> None:
    if NATS is None:
        LOGGER.warning("NATS client not available; skipping publication")
        return
    client = NATS()
    await client.connect(servers=NATS_SERVERS)
    try:
        for payload in payloads:
            await client.publish(WHITELIST_TOPIC, json.dumps(payload, default=str).encode("utf-8"))
            LOGGER.debug("Published whitelist asset", extra=payload)
    finally:
        await client.drain()


def main() -> None:
    LOGGER.info("Starting CoinGecko ingestion job")
    client = CoinGeckoClient()
    engine = create_engine(DATABASE_URL, future=True)
    metrics = client.fetch_top_assets()
    upsert_metrics(engine, metrics)
    whitelist_records = publish_whitelist(engine, metrics)
    try:
        import asyncio

        asyncio.run(publish_to_nats(whitelist_records))
    except RuntimeError:
        LOGGER.exception("Failed to publish whitelist to NATS")


if __name__ == "__main__":
    main()
