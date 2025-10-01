"""CoinGecko OHLCV ingestion script.

This module provides an asynchronous batch job that fetches daily OHLCV data
from the CoinGecko API and upserts it into a TimescaleDB hypertable named
``bars_daily``. It is designed to be executed as a Kubernetes CronJob or other
batch workload.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Iterable, Sequence

import httpx
from sqlalchemy import Column, DateTime, MetaData, Numeric, String, Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

LOGGER = logging.getLogger(__name__)

API_BASE_URL = "https://api.coingecko.com/api/v3"
REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2.0


@dataclass(slots=True)
class OHLCVRow:
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


metadata = MetaData()
BARS_DAILY = Table(
    "bars_daily",
    metadata,
    Column("symbol", String, primary_key=True),
    Column("ts", DateTime(timezone=True), primary_key=True),
    Column("open", Numeric(asdecimal=False), nullable=False),
    Column("high", Numeric(asdecimal=False), nullable=False),
    Column("low", Numeric(asdecimal=False), nullable=False),
    Column("close", Numeric(asdecimal=False), nullable=False),
    Column("volume", Numeric(asdecimal=False), nullable=False),
)


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:  # pragma: no cover - defensive
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def to_utc_datetime(value: date) -> datetime:
    return datetime.combine(value, datetime.min.time(), tzinfo=UTC)


async def fetch_market_chart(
    client: httpx.AsyncClient,
    symbol: str,
    vs_currency: str,
    start: datetime,
    end: datetime,
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> dict[str, Any]:
    """Fetch price and volume history for ``symbol`` within the inclusive range."""

    params = {
        "vs_currency": vs_currency,
        "from": math.floor(start.timestamp()),
        "to": math.ceil(end.timestamp()),
    }
    url = f"/coins/{symbol}/market_chart/range"

    for attempt in range(1, max_retries + 1):
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
            if not payload:
                raise ValueError("Empty response from CoinGecko")
            return payload
        except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as exc:
            retry_after = _retry_delay(attempt)
            if isinstance(exc, httpx.HTTPStatusError):
                status = exc.response.status_code
                if status < 500 and status not in {408, 429}:
                    raise
                retry_after = _retry_delay(attempt, retry_after_header=exc.response.headers.get("Retry-After"))
            if attempt >= max_retries:
                LOGGER.error("Giving up on %s after %s attempts: %s", symbol, attempt, exc)
                raise
            LOGGER.warning(
                "Attempt %s/%s for %s failed (%s). Retrying in %.1fs",
                attempt,
                max_retries,
                symbol,
                exc,
                retry_after,
            )
            await asyncio.sleep(retry_after)
    raise RuntimeError("Unreachable")  # pragma: no cover - defensive


def _retry_delay(attempt: int, retry_after_header: str | None = None) -> float:
    if retry_after_header:
        try:
            return float(retry_after_header)
        except ValueError:
            pass
    return BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))


def aggregate_daily_rows(
    symbol: str,
    payload: dict[str, Any],
    start: datetime,
    end: datetime,
) -> list[OHLCVRow]:
    prices: Sequence[Sequence[float]] = payload.get("prices", [])
    volumes: Sequence[Sequence[float]] = payload.get("total_volumes", [])

    if not prices:
        LOGGER.warning("No price data returned for %s", symbol)
        return []

    daily: dict[datetime, dict[str, list[Any]]] = {}

    for ts_millis, price in prices:
        ts = datetime.fromtimestamp(ts_millis / 1000.0, tz=UTC)
        if ts < start or ts > end:
            continue
        bucket = daily.setdefault(ts.replace(hour=0, minute=0, second=0, microsecond=0), {"prices": [], "volumes": []})
        bucket["prices"].append((ts, float(price)))

    for ts_millis, volume in volumes:
        ts = datetime.fromtimestamp(ts_millis / 1000.0, tz=UTC)
        if ts < start or ts > end:
            continue
        bucket = daily.setdefault(ts.replace(hour=0, minute=0, second=0, microsecond=0), {"prices": [], "volumes": []})
        bucket["volumes"].append(float(volume))

    rows: list[OHLCVRow] = []
    for day in sorted(daily):
        bucket = daily[day]
        price_series = sorted(bucket["prices"], key=lambda item: item[0])
        if not price_series:
            continue
        open_price = price_series[0][1]
        close_price = price_series[-1][1]
        high_price = max(value for _, value in price_series)
        low_price = min(value for _, value in price_series)
        volume_values = bucket["volumes"]
        volume = max(volume_values) if volume_values else 0.0
        rows.append(
            OHLCVRow(
                symbol=symbol,
                ts=day,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume,
            )
        )
    return rows


async def upsert_ohlcv_rows(engine: AsyncEngine, rows: Iterable[OHLCVRow], *, batch_size: int = 500) -> None:
    batch: list[dict[str, Any]] = []
    async with engine.begin() as connection:
        for row in rows:
            batch.append(
                {
                    "symbol": row.symbol,
                    "ts": row.ts,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                }
            )
            if len(batch) >= batch_size:
                await _execute_upsert(connection, batch)
                batch.clear()
        if batch:
            await _execute_upsert(connection, batch)


async def _execute_upsert(connection, payload: list[dict[str, Any]]) -> None:
    stmt = insert(BARS_DAILY).values(payload)
    update_cols = {col: getattr(stmt.excluded, col) for col in ("open", "high", "low", "close", "volume")}
    stmt = stmt.on_conflict_do_update(index_elements=[BARS_DAILY.c.symbol, BARS_DAILY.c.ts], set_=update_cols)
    await connection.execute(stmt)


async def ingest_symbol(
    engine: AsyncEngine,
    client: httpx.AsyncClient,
    symbol: str,
    vs_currency: str,
    start: datetime,
    end: datetime,
    *,
    max_retries: int,
) -> None:
    try:
        payload = await fetch_market_chart(
            client,
            symbol,
            vs_currency,
            start,
            end,
            max_retries=max_retries,
        )
        rows = aggregate_daily_rows(symbol, payload, start, end)
        if not rows:
            LOGGER.info("No rows to upsert for %s", symbol)
            return
        await upsert_ohlcv_rows(engine, rows)
        LOGGER.info("Ingested %s rows for %s", len(rows), symbol)
    except (httpx.HTTPError, SQLAlchemyError, ValueError) as exc:
        LOGGER.exception("Failed to ingest %s: %s", symbol, exc)
        raise


async def run_ingest(args: argparse.Namespace) -> None:
    engine = create_async_engine(args.db_url, future=True)
    timeout = httpx.Timeout(REQUEST_TIMEOUT_SECONDS)
    try:
        async with httpx.AsyncClient(base_url=args.api_base_url, timeout=timeout) as client:
            for symbol in args.symbols:
                await ingest_symbol(
                    engine,
                    client,
                    symbol,
                    args.vs_currency,
                    args.start,
                    args.end,
                    max_retries=args.max_retries,
                )
    finally:
        await engine.dispose()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest daily OHLCV data from CoinGecko")
    parser.add_argument("--symbols", nargs="+", required=True, help="CoinGecko coin IDs to ingest")
    parser.add_argument("--vs-currency", default="usd", help="Quote currency (default: usd)")
    parser.add_argument(
        "--start-date",
        type=parse_date,
        help="Inclusive start date (YYYY-MM-DD). Default: today - 30 days",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        help="Inclusive end date (YYYY-MM-DD). Default: today",
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"),
        help="Async SQLAlchemy database URL",
    )
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Max API retries")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"), help="Logging level")
    parser.add_argument(
        "--api-base-url",
        default=API_BASE_URL,
        help="Override the CoinGecko API base URL",
    )
    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def resolve_dates(args: argparse.Namespace) -> None:
    today = datetime.now(tz=UTC).date()
    start_date = args.start_date or today - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    end_date = args.end_date or today
    if start_date > end_date:
        raise ValueError("start-date must be before or equal to end-date")
    args.start = to_utc_datetime(start_date)
    args.end = to_utc_datetime(end_date) + timedelta(days=1) - timedelta(microseconds=1)


async def async_main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    resolve_dates(args)
    LOGGER.info(
        "Starting CoinGecko ingest for symbols=%s range=%s..%s", args.symbols, args.start.isoformat(), args.end.isoformat()
    )
    await run_ingest(args)


def main() -> None:
    try:
        asyncio.run(async_main())
    except Exception:  # pragma: no cover - surface errors to batch infrastructure
        LOGGER.exception("CoinGecko ingestion failed")
        raise


if __name__ == "__main__":
    main()
