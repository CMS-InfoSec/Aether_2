
"""Batch ingestion job for CoinGecko metrics and whitelist publication."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sys
from dataclasses import dataclass
from typing import Any, Iterable, List, Mapping

class MissingDependencyError(RuntimeError):
    """Raised when an optional dependency is required for CoinGecko ingest."""


try:  # pragma: no cover - optional dependency
    import requests
except Exception as exc:  # pragma: no cover - executed in lightweight environments
    requests = None  # type: ignore[assignment]
    _REQUESTS_IMPORT_ERROR = exc
else:
    _REQUESTS_IMPORT_ERROR = None

_SQLALCHEMY_AVAILABLE = True
_SQLALCHEMY_IMPORT_ERROR: Exception | None = None

try:  # pragma: no cover - optional dependency
    from sqlalchemy import Boolean, Column, DateTime, JSON, MetaData, Numeric, String, Table
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.engine import Engine, create_engine
except Exception as exc:  # pragma: no cover - executed when SQLAlchemy absent
    _SQLALCHEMY_AVAILABLE = False
    _SQLALCHEMY_IMPORT_ERROR = exc
else:
    if not hasattr(Table, "c"):
        _SQLALCHEMY_AVAILABLE = False
        _SQLALCHEMY_IMPORT_ERROR = RuntimeError("SQLAlchemy table metadata is unavailable")

if not _SQLALCHEMY_AVAILABLE:
    Boolean = Column = DateTime = JSON = Numeric = String = Table = None  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]

    def create_engine(*_: object, **__: object) -> None:  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for CoinGecko ingest") from _SQLALCHEMY_IMPORT_ERROR

    def pg_insert(*_: object, **__: object) -> None:  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for CoinGecko ingest") from _SQLALCHEMY_IMPORT_ERROR

    metadata = None
    whitelist_table = None
    metrics_table = None
else:
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

from shared.postgres import normalize_sqlalchemy_dsn

try:
    from nats.aio.client import Client as NATS
except ImportError:  # pragma: no cover - optional dependency
    NATS = None  # type: ignore

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def _require_requests() -> None:
    if requests is None:  # pragma: no cover - executed when requests missing
        raise MissingDependencyError("requests is required for CoinGecko ingest") from _REQUESTS_IMPORT_ERROR


def _require_sqlalchemy() -> None:
    if not _SQLALCHEMY_AVAILABLE or metadata is None or metrics_table is None or whitelist_table is None:
        raise MissingDependencyError("SQLAlchemy is required for CoinGecko ingest") from _SQLALCHEMY_IMPORT_ERROR

COINGECKO_API = os.getenv("COINGECKO_API", "https://api.coingecko.com/api/v3")
_SQLITE_FALLBACK_FLAG = "COINGECKO_ALLOW_SQLITE_FOR_TESTS"


def _resolve_database_url() -> str:
    """Return the configured Timescale/PostgreSQL DSN for persistence."""

    raw_url = os.getenv("DATABASE_URL", "")
    if not raw_url.strip():
        raise RuntimeError(
            "CoinGecko ingest requires DATABASE_URL to be set to a PostgreSQL/Timescale DSN."
        )

    allow_sqlite = "pytest" in sys.modules or os.getenv(_SQLITE_FALLBACK_FLAG) == "1"
    database_url = normalize_sqlalchemy_dsn(
        raw_url.strip(),
        allow_sqlite=allow_sqlite,
        label="CoinGecko ingest database URL",
    )

    if database_url.startswith("sqlite"):
        LOGGER.warning(
            "Using SQLite database '%s' for CoinGecko ingest; allowed only for tests.",
            database_url,
        )

    return database_url


DATABASE_URL = _resolve_database_url()
WHITELIST_TOPIC = os.getenv("WHITELIST_TOPIC", "universe.whitelist")
NATS_SERVERS = os.getenv("NATS_SERVERS", "nats://localhost:4222").split(",")


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
        if session is not None:
            self.session = session
        else:
            _require_requests()
            self.session = requests.Session()


    def fetch_top_assets(self, vs_currency: str = "usd", limit: int = 100) -> List[AssetMetric]:
        _require_requests()
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
    _require_sqlalchemy()
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
    _require_sqlalchemy()
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
