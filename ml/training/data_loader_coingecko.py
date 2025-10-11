"""CoinGecko OHLCV data ingestion utilities.

This module provides helpers to download OHLCV data from the CoinGecko
API, validate the data using Great Expectations, and persist the results
into TimescaleDB hypertables.  It also offers a small orchestration layer
that coordinates multi-symbol, multi-granularity backfills while logging
run metadata for observability.
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Optional, Tuple

from uuid import UUID, uuid4

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

_PANDAS_ERROR = "pandas is required for CoinGecko data loading functionality"
_REQUESTS_ERROR = "requests is required for CoinGecko data downloads"
_SQLALCHEMY_ERROR = "sqlalchemy is required for CoinGecko persistence"
_GX_ERROR = "great_expectations is required for CoinGecko data validation"


class MissingDependencyError(RuntimeError):
    """Raised when a required third-party dependency is unavailable."""


try:  # Optional dependency for dataframe processing.
    import pandas as _PANDAS_MODULE
except Exception:  # pragma: no cover - exercised only when pandas is absent.
    _PANDAS_MODULE = None  # type: ignore[assignment]


try:  # HTTP client used for CoinGecko downloads.
    import requests as _REQUESTS_MODULE
except Exception:  # pragma: no cover - exercised only when requests is absent.
    _REQUESTS_MODULE = None  # type: ignore[assignment]


try:  # SQLAlchemy is required to persist OHLCV data in TimescaleDB.
    from sqlalchemy import text as _SQLALCHEMY_TEXT
    from sqlalchemy.engine import create_engine as _SQLALCHEMY_CREATE_ENGINE
except Exception:  # pragma: no cover - executed when SQLAlchemy isn't installed.
    _SQLALCHEMY_TEXT = None  # type: ignore[assignment]
    _SQLALCHEMY_CREATE_ENGINE = None  # type: ignore[assignment]


try:  # Great Expectations is optional in lightweight environments.
    import great_expectations as _GX_MODULE
    from great_expectations.exceptions import CheckpointNotFoundError
except Exception:  # pragma: no cover - executed only when GE is missing.
    _GX_MODULE = None  # type: ignore[assignment]

    class CheckpointNotFoundError(Exception):  # type: ignore[no-redef]
        """Fallback exception placeholder when Great Expectations is unavailable."""


if TYPE_CHECKING:  # pragma: no cover - imported solely for type checking.
    from pandas import DataFrame as _PandasDataFrame
    from sqlalchemy.engine import Engine as _SQLAlchemyEngine
    from requests import Session as _RequestsSession
else:  # Lightweight fallbacks used at runtime when optional deps are absent.
    _PandasDataFrame = Any  # type: ignore[assignment]
    _SQLAlchemyEngine = Any  # type: ignore[assignment]
    _RequestsSession = Any  # type: ignore[assignment]

DataFrame = _PandasDataFrame
Engine = _SQLAlchemyEngine

_SESSION: Optional[_RequestsSession] = None

def _require_pandas():
    """Return the pandas module or raise a descriptive dependency error."""

    if _PANDAS_MODULE is None:
        raise MissingDependencyError(_PANDAS_ERROR)
    return _PANDAS_MODULE


def _require_requests():
    """Return the requests module or raise when it is unavailable."""

    if _REQUESTS_MODULE is None:
        raise MissingDependencyError(_REQUESTS_ERROR)
    return _REQUESTS_MODULE


def _get_session() -> _RequestsSession:
    """Initialise or return the cached HTTP session."""

    requests_module = _require_requests()
    global _SESSION
    if _SESSION is None:
        _SESSION = requests_module.Session()
    return _SESSION


def _require_sqlalchemy() -> None:
    """Ensure SQLAlchemy helpers are available before using them."""

    if _SQLALCHEMY_CREATE_ENGINE is None or _SQLALCHEMY_TEXT is None:
        raise MissingDependencyError(_SQLALCHEMY_ERROR)


def _create_engine(*args, **kwargs):
    _require_sqlalchemy()
    assert _SQLALCHEMY_CREATE_ENGINE is not None  # For type checkers.
    return _SQLALCHEMY_CREATE_ENGINE(*args, **kwargs)


def _sqlalchemy_text(statement: str):
    _require_sqlalchemy()
    assert _SQLALCHEMY_TEXT is not None  # For type checkers.
    return _SQLALCHEMY_TEXT(statement)


def _require_gx():
    """Return the Great Expectations module or raise a dependency error."""

    if _GX_MODULE is None:
        raise MissingDependencyError(_GX_ERROR)
    return _GX_MODULE


COINGECKO_API = "https://api.coingecko.com/api/v3"
GRANULARITY_TO_PANDAS = {
    "1m": "1T",
    "5m": "5T",
    "15m": "15T",
    "1h": "1H",
    "1d": "1D",
}
MAX_RANGE_SECONDS = 90 * 24 * 60 * 60  # CoinGecko allows up to 90 days per call.


def _ensure_timezone(dt: datetime) -> datetime:
    """Return ``dt`` as an aware timestamp in UTC."""

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _request_with_retry(url: str, params: Mapping[str, object]) -> Mapping[str, object]:
    """Issue an HTTP GET with retries and backoff for rate limits."""

    max_attempts = 8
    backoff_seconds = 1.0

    for attempt in range(1, max_attempts + 1):
        try:
            response = _get_session().get(url, params=params, timeout=30)
        except Exception as exc:
            requests_module = _require_requests()
            if not isinstance(exc, requests_module.RequestException):
                raise
            if attempt == max_attempts:
                raise
            sleep_for = backoff_seconds * (2 ** (attempt - 1))
            jitter = random.uniform(0, 0.5)
            time.sleep(sleep_for + jitter)
            LOGGER.debug("Retrying request after network error: %s", exc)
            continue

        if response.status_code == 429:
            if attempt == max_attempts:
                response.raise_for_status()
            retry_after = response.headers.get("Retry-After")
            wait_seconds = float(retry_after) if retry_after else backoff_seconds * (
                2 ** (attempt - 1)
            )
            jitter = random.uniform(0, 0.5)
            LOGGER.warning(
                "CoinGecko rate limited request (attempt %s/%s); sleeping %.2fs",
                attempt,
                max_attempts,
                wait_seconds + jitter,
            )
            time.sleep(wait_seconds + jitter)
            continue

        if response.status_code >= 500:
            if attempt == max_attempts:
                response.raise_for_status()
            sleep_for = backoff_seconds * (2 ** (attempt - 1))
            jitter = random.uniform(0, 0.5)
            LOGGER.warning(
                "Server error from CoinGecko (status %s); retrying in %.2fs",
                response.status_code,
                sleep_for + jitter,
            )
            time.sleep(sleep_for + jitter)
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError("Failed to fetch data from CoinGecko after retries")


def _transform_market_chart(
    payload: Mapping[str, object],
    start: datetime,
    end: datetime,
    granularity: str,
) -> DataFrame:
    """Transform CoinGecko's market chart payload into an OHLCV frame."""

    prices = payload.get("prices")
    volumes = payload.get("total_volumes")

    if not isinstance(prices, list) or not prices:
        raise ValueError("CoinGecko response missing price data")

    pd = _require_pandas()
    price_df = pd.DataFrame(prices, columns=["ts", "price"])
    volume_df = pd.DataFrame(volumes or [], columns=["ts", "volume"])
    frame = price_df.merge(volume_df, on="ts", how="left")
    frame["ts"] = pd.to_datetime(frame["ts"], unit="ms", utc=True)
    frame = frame.set_index("ts")

    start_utc = _ensure_timezone(start)
    end_utc = _ensure_timezone(end)
    frame = frame[(frame.index >= start_utc) & (frame.index <= end_utc)]

    resampled = frame.resample(GRANULARITY_TO_PANDAS[granularity]).agg(
        {"price": ["first", "max", "min", "last"], "volume": "sum"}
    )
    resampled.columns = ["open", "high", "low", "close", "volume"]
    resampled = resampled.dropna(subset=["open", "high", "low", "close"])
    resampled = resampled.reset_index()
    resampled["volume"] = resampled["volume"].fillna(0.0)
    return resampled


def _get_ge_context():
    """Instantiate the Great Expectations data context."""

    gx = _require_gx()
    root_dir = os.getenv("GE_DATA_CONTEXT_ROOT_DIR")
    if root_dir:
        return gx.get_context(context_root_dir=root_dir)
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    default_path = os.path.join(repo_root, "data", "great_expectations")
    if os.path.isdir(default_path):
        return gx.get_context(context_root_dir=default_path)
    return gx.get_context()


def fetch_ohlcv(
    symbol: str,
    vs_currency: str,
    start: datetime,
    end: datetime,
    granularity: str,
) -> DataFrame:
    """Fetch OHLCV data for ``symbol`` between ``start`` and ``end``."""

    if granularity not in GRANULARITY_TO_PANDAS:
        raise ValueError(f"Unsupported granularity: {granularity}")

    start_utc = _ensure_timezone(start)
    end_utc = _ensure_timezone(end)
    if start_utc >= end_utc:
        raise ValueError("start must be earlier than end")

    symbol_id = symbol.lower()
    vs_currency = vs_currency.lower()

    pd = _require_pandas()
    frames: List[DataFrame] = []
    cursor = start_utc

    while cursor < end_utc:
        chunk_end = min(cursor + timedelta(seconds=MAX_RANGE_SECONDS), end_utc)
        params = {
            "vs_currency": vs_currency,
            "from": int(cursor.timestamp()),
            "to": int(chunk_end.timestamp()),
        }
        url = f"{COINGECKO_API}/coins/{symbol_id}/market_chart/range"
        payload = _request_with_retry(url, params)
        frame = _transform_market_chart(payload, cursor, chunk_end, granularity)
        frames.append(frame)
        cursor = chunk_end

    if not frames:
        columns = ["ts", "open", "high", "low", "close", "volume"]
        return pd.DataFrame(columns=columns)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["ts"]).sort_values("ts")

    context = _get_ge_context()
    checkpoint_result = None
    try:
        checkpoint_result = context.run_checkpoint(
            checkpoint_name="ohlcv_checkpoint",
            batch_request={
                "runtime_parameters": {"batch_data": combined},
                "batch_identifiers": {
                    "symbol": symbol_id,
                    "granularity": granularity,
                },
            },
        )
    except CheckpointNotFoundError:
        LOGGER.warning(
            "Great Expectations checkpoint 'ohlcv_checkpoint' not found; skipping validation"
        )
    else:
        success = (
            getattr(checkpoint_result, "success", None)
            if not isinstance(checkpoint_result, dict)
            else checkpoint_result.get("success")
        )
        if not success:
            raise ValueError("Great Expectations validation failed for OHLCV data")

    return combined.reset_index(drop=True)


def _get_engine() -> Engine:
    dsn = os.getenv("TIMESCALE_DSN")
    if not dsn:
        raise EnvironmentError("TIMESCALE_DSN environment variable must be set")
    return _create_engine(dsn, pool_pre_ping=True, pool_recycle=3600)


def _ensure_ohlcv_table(engine: Engine, table_name: str) -> None:
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        symbol TEXT NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        open DOUBLE PRECISION NOT NULL,
        high DOUBLE PRECISION NOT NULL,
        low DOUBLE PRECISION NOT NULL,
        close DOUBLE PRECISION NOT NULL,
        volume DOUBLE PRECISION NOT NULL,
        PRIMARY KEY (symbol, ts)
    );
    """
    create_hypertable_sql = (
        "SELECT create_hypertable(:table_name, 'ts', if_not_exists => TRUE);"
    )
    create_index_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_ts ON {table_name} (symbol, ts);
    """
    sql_text = _sqlalchemy_text

    with engine.begin() as conn:
        conn.execute(sql_text(create_table_sql))
        conn.execute(sql_text(create_hypertable_sql), {"table_name": table_name})
        conn.execute(sql_text(create_index_sql))


def upsert_timescale(df: DataFrame, symbol: str, granularity: str) -> None:
    """Persist the OHLCV dataframe into TimescaleDB."""

    if df.empty:
        LOGGER.warning("No data to upsert for %s @ %s", symbol, granularity)
        return

    engine = _get_engine()
    table_name = f"ohlcv_{granularity}"
    _ensure_ohlcv_table(engine, table_name)

    payload = [
        {
            "symbol": symbol,
            "ts": row.ts.to_pydatetime() if hasattr(row.ts, "to_pydatetime") else row.ts,
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
            "volume": float(row.volume),
        }
        for row in df.itertuples(index=False)
    ]

    sql_text = _sqlalchemy_text

    insert_sql = sql_text(
        f"""
        INSERT INTO {table_name} (symbol, ts, open, high, low, close, volume)
        VALUES (:symbol, :ts, :open, :high, :low, :close, :volume)
        ON CONFLICT (symbol, ts) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
        """
    )

    with engine.begin() as conn:
        conn.execute(insert_sql, payload)


def _ensure_runs_table(engine: Engine) -> None:
    create_sql = """
    CREATE TABLE IF NOT EXISTS cg_history_runs (
        run_id UUID NOT NULL,
        symbol TEXT NOT NULL,
        granularity TEXT NOT NULL,
        rows INTEGER NOT NULL DEFAULT 0,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        status TEXT NOT NULL,
        error TEXT,
        PRIMARY KEY (run_id, symbol, granularity)
    );
    """
    sql_text = _sqlalchemy_text

    with engine.begin() as conn:
        conn.execute(sql_text(create_sql))


def _record_run_start(
    engine: Engine,
    run_id: str,
    symbol: str,
    granularity: str,
    started_at: datetime,
) -> None:
    sql_text = _sqlalchemy_text

    sql = sql_text(
        """
        INSERT INTO cg_history_runs (run_id, symbol, granularity, rows, started_at, status)
        VALUES (:run_id, :symbol, :granularity, 0, :started_at, 'running')
        ON CONFLICT (run_id, symbol, granularity) DO UPDATE SET
            started_at = EXCLUDED.started_at,
            status = 'running',
            error = NULL,
            finished_at = NULL,
            rows = 0;
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "run_id": run_id,
                "symbol": symbol,
                "granularity": granularity,
                "started_at": started_at,
            },
        )


def _record_run_finish(
    engine: Engine,
    run_id: str,
    symbol: str,
    granularity: str,
    rows: int,
    finished_at: datetime,
    status: str,
    error: Optional[str],
) -> None:
    sql_text = _sqlalchemy_text

    sql = sql_text(
        """
        UPDATE cg_history_runs
        SET rows = :rows,
            finished_at = :finished_at,
            status = :status,
            error = :error
        WHERE run_id = :run_id AND symbol = :symbol AND granularity = :granularity;
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "rows": rows,
                "finished_at": finished_at,
                "status": status,
                "error": error,
                "run_id": run_id,
                "symbol": symbol,
                "granularity": granularity,
            },
        )


def _load_completed(engine: Engine, run_id: str) -> Mapping[Tuple[str, str], str]:
    sql_text = _sqlalchemy_text

    sql = sql_text(
        """
        SELECT symbol, granularity, status
        FROM cg_history_runs
        WHERE run_id = :run_id;
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql, {"run_id": run_id}).fetchall()
    return {(row.symbol, row.granularity): row.status for row in rows}


def load_history(
    symbols: Iterable[str],
    vs_currency: str,
    start: datetime,
    end: datetime,
    granularities: Iterable[str],
    resume: Optional[str] = None,
) -> Tuple[str, Dict[str, Dict[str, int]]]:
    """Backfill OHLCV data for multiple symbols/granularities."""

    engine = _get_engine()
    _ensure_runs_table(engine)

    if vs_currency.strip().lower() != "usd":
        raise ValueError("Only USD quote currency is supported")

    if resume:
        try:
            run_uuid = UUID(resume)
        except ValueError as exc:
            raise ValueError("Resume run_id must be a valid UUID") from exc
        run_id = str(run_uuid)
    else:
        run_id = str(uuid4())

    completed = _load_completed(engine, run_id) if resume else {}

    stats: Dict[str, Dict[str, int]] = {}
    for symbol in symbols:
        symbol_stats: Dict[str, int] = {}
        for granularity in granularities:
            if completed.get((symbol, granularity)) == "success":
                LOGGER.info(
                    "Skipping %s @ %s (already completed in run %s)",
                    symbol,
                    granularity,
                    run_id,
                )
                continue

            started_at = datetime.now(timezone.utc)
            _record_run_start(engine, run_id, symbol, granularity, started_at)
            try:
                frame = fetch_ohlcv(symbol, vs_currency, start, end, granularity)
                upsert_timescale(frame, symbol, granularity)
                rows = int(len(frame))
                symbol_stats[granularity] = rows
                _record_run_finish(
                    engine,
                    run_id,
                    symbol,
                    granularity,
                    rows,
                    datetime.now(timezone.utc),
                    "success",
                    None,
                )
                LOGGER.info(
                    "Ingested %s rows for %s @ %s", rows, symbol, granularity
                )
            except Exception as exc:  # pragma: no cover - logged for observability.
                LOGGER.exception(
                    "Failed to ingest data for %s @ %s: %s", symbol, granularity, exc
                )
                _record_run_finish(
                    engine,
                    run_id,
                    symbol,
                    granularity,
                    symbol_stats.get(granularity, 0),
                    datetime.now(timezone.utc),
                    "failed",
                    str(exc),
                )
                raise
        if symbol_stats:
            stats[symbol] = symbol_stats

    return run_id, stats


def _parse_comma_separated(values: str) -> List[str]:
    return [value.strip() for value in values.split(",") if value.strip()]


def _parse_datetime(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="CoinGecko OHLCV loader")
    parser.add_argument("--symbols", required=True, help="Comma separated list of symbols")
    parser.add_argument("--vs", required=True, help="Quote currency (e.g. USD)")
    parser.add_argument("--from", dest="start", required=True, help="Start timestamp (ISO8601)")
    parser.add_argument("--to", dest="end", required=True, help="End timestamp (ISO8601)")
    parser.add_argument(
        "--gran",
        dest="granularities",
        required=True,
        help="Comma separated list of granularities (1m,5m,15m,1h,1d)",
    )
    parser.add_argument("--resume", dest="resume", help="Resume run identifier")

    args = parser.parse_args(argv)

    symbols = _parse_comma_separated(args.symbols)
    granularities = _parse_comma_separated(args.granularities)
    start = _parse_datetime(args.start)
    end = _parse_datetime(args.end)

    run_id, stats = load_history(symbols, args.vs, start, end, granularities, args.resume)
    LOGGER.info("Run %s completed with stats: %s", run_id, stats)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
