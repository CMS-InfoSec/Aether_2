"""Batch ingestion workflow for CoinGecko market data.

The workflow performs the following steps when executed as a script:

1. Fetch the top N assets by market capitalisation from CoinGecko's public API.
2. Derive liquidity metrics such as 24h volume and 30d volatility.
3. Persist the metrics to the offline store (TimescaleDB) by writing into the
   ``features`` hypertable.
4. Produce a nightly whitelist file identifying the symbols that pass the
   configured liquidity/volatility filters. This whitelist is later consumed by
   downstream model pipelines.

The job is deliberately implemented with a functional core / imperative shell so
it can be unit tested without touching external systems.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from dataclasses import dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import requests
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert

logger = logging.getLogger(__name__)

getcontext().prec = 28

COINGECKO_API_ROOT = "https://api.coingecko.com/api/v3"
DEFAULT_UNIVERSE_SIZE = 200
DEFAULT_VS_CURRENCY = "usd"
DEFAULT_VOL_LOOKBACK_DAYS = 30
WHITELIST_THRESHOLD_MCAP = Decimal("100000000")  # 100M USD
WHITELIST_THRESHOLD_VOLUME = Decimal("2000000")  # 2M USD


@dataclass
class AssetMetrics:
    """Container for CoinGecko derived metrics."""

    symbol: str
    name: str
    market_cap: Decimal
    volume_24h: Decimal
    volatility_30d: Decimal
    as_of: dt.datetime

    def to_feature_rows(self) -> Sequence[Dict[str, object]]:
        base = {
            "symbol": self.symbol,
            "event_ts": self.as_of,
        }
        return [
            {**base, "feature_name": "coingecko.market_cap", "value": self.market_cap},
            {**base, "feature_name": "coingecko.volume_24h", "value": self.volume_24h},
            {**base, "feature_name": "coingecko.volatility_30d", "value": self.volatility_30d},
        ]

    def passes_whitelist(self) -> bool:
        return self.market_cap >= WHITELIST_THRESHOLD_MCAP and self.volume_24h >= WHITELIST_THRESHOLD_VOLUME


class CoinGeckoClient:
    """Thin wrapper around the CoinGecko REST API."""

    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()

    def fetch_markets(
        self,
        vs_currency: str = DEFAULT_VS_CURRENCY,
        per_page: int = DEFAULT_UNIVERSE_SIZE,
        page: int = 1,
    ) -> List[Dict[str, object]]:
        url = f"{COINGECKO_API_ROOT}/coins/markets"
        resp = self.session.get(
            url,
            params={
                "vs_currency": vs_currency,
                "order": "market_cap_desc",
                "per_page": per_page,
                "page": page,
                "price_change_percentage": "24h,7d,30d",
                "sparkline": "false",
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_volatility(self, coin_id: str, days: int = DEFAULT_VOL_LOOKBACK_DAYS) -> Decimal:
        url = f"{COINGECKO_API_ROOT}/coins/{coin_id}/market_chart"
        resp = self.session.get(url, params={"vs_currency": DEFAULT_VS_CURRENCY, "days": days}, timeout=30)
        resp.raise_for_status()
        prices = resp.json().get("prices", [])
        if len(prices) < 2:
            return Decimal("0")
        closes = [Decimal(str(price[1])) for price in prices]
        returns = []
        for prev, curr in zip(closes, closes[1:]):
            if prev == 0:
                continue
            returns.append((curr - prev) / prev)
        if not returns:
            return Decimal("0")
        mean = sum(returns, Decimal("0")) / Decimal(len(returns))
        variance = sum((r - mean) ** 2 for r in returns) / Decimal(len(returns))
        count = Decimal(len(returns))
        return variance.sqrt() * count.sqrt()  # approximate annualised volatility


def derive_metrics(client: CoinGeckoClient, universe_size: int) -> List[AssetMetrics]:
    logger.info("Fetching top %d assets from CoinGecko", universe_size)
    markets = client.fetch_markets(per_page=universe_size)
    as_of = dt.datetime.now(tz=dt.timezone.utc)
    metrics: List[AssetMetrics] = []
    for market in markets:
        coin_id = market["id"]
        volatility = client.fetch_volatility(coin_id)
        metrics.append(
            AssetMetrics(
                symbol=market.get("symbol", "").upper(),
                name=market.get("name", coin_id),
                market_cap=Decimal(str(market.get("market_cap", 0))),
                volume_24h=Decimal(str(market.get("total_volume", 0))),
                volatility_30d=volatility,
                as_of=as_of,
            )
        )
    return metrics


def persist_metrics(engine_url: str, metrics: Iterable[AssetMetrics]) -> None:
    engine = create_engine(engine_url)
    metadata = MetaData()
    features = Table(
        "features",
        metadata,
        autoload_with=engine,
    )
    rows = [row for metric in metrics for row in metric.to_feature_rows()]
    if not rows:
        logger.warning("No rows to persist to features table")
        return

    stmt = pg_insert(features).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[features.c.feature_name, features.c.symbol, features.c.event_ts],
        set_={"value": stmt.excluded.value, "metadata": stmt.excluded.metadata},
    )
    with engine.begin() as conn:
        conn.execute(stmt)
    logger.info("Persisted %d feature rows", len(rows))


def emit_whitelist(metrics: Iterable[AssetMetrics], output_path: Path) -> List[str]:
    whitelist = [metric.symbol for metric in metrics if metric.passes_whitelist()]
    payload = {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "thresholds": {
            "market_cap": str(WHITELIST_THRESHOLD_MCAP),
            "volume_24h": str(WHITELIST_THRESHOLD_VOLUME),
        },
        "symbols": whitelist,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2))
    logger.info("Wrote whitelist with %d symbols to %s", len(whitelist), output_path)
    return whitelist


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)


def main(args: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest CoinGecko market metrics into TimescaleDB")
    parser.add_argument("--database-url", required=True, help="SQLAlchemy compatible Postgres/Timescale URL")
    parser.add_argument("--universe-size", type=int, default=DEFAULT_UNIVERSE_SIZE)
    parser.add_argument("--whitelist-path", type=Path, default=Path("data/output/nightly_whitelist.json"))
    parser.add_argument("--verbose", action="store_true")
    parsed = parser.parse_args(args=args)

    configure_logging(parsed.verbose)

    client = CoinGeckoClient()
    metrics = derive_metrics(client, parsed.universe_size)
    persist_metrics(parsed.database_url, metrics)
    emit_whitelist(metrics, parsed.whitelist_path)


if __name__ == "__main__":
    main()
