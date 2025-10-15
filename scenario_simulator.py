"""FastAPI service for projecting scenario-driven portfolio risk metrics.

This module exposes a single endpoint that accepts a price shock percentage and
volatility multiplier, applies them to the current portfolio state, and returns
risk statistics derived from historical TimescaleDB data.  Each invocation is
persisted to the ``scenario_runs`` hypertable so that downstream systems can
replay or audit historical scenario analyses.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Set

try:  # pragma: no cover - numpy is optional in some environments
    import numpy as np
except Exception:  # pragma: no cover - exercised when numpy is unavailable
    np = None  # type: ignore[assignment]

from shared.common_bootstrap import _ensure_fastapi_stub, ensure_common_helpers

_ensure_fastapi_stub()

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from exposure_forecast import ForecastResult, PNL_CURVE_QUERY, get_exposure_ml_pipeline

ensure_common_helpers()

from services.common.config import get_timescale_session
from services.common.security import require_admin_account
from shared.correlation import CorrelationIdMiddleware
from shared.spot import filter_spot_symbols, is_spot_symbol, normalize_spot_symbol

try:  # pragma: no cover - psycopg is an optional dependency in some environments
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - executed when psycopg is unavailable
    psycopg = None  # type: ignore[assignment]
    sql = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)

LOOKBACK_DAYS = int(os.getenv("SCENARIO_LOOKBACK_DAYS", "90"))
EXPOSURE_RETRAIN_MINUTES = int(os.getenv("SCENARIO_EXPOSURE_RETRAIN_MINUTES", "60"))

_ENSURED_SCHEMAS: Set[str] = set()
_ENSURE_LOCK = threading.Lock()
_DEFAULT_ACCOUNT_ID = os.getenv("AETHER_ACCOUNT_ID")

POSITIONS_QUERY = """
WITH latest_positions AS (
    SELECT DISTINCT ON (market)
        market,
        quantity,
        COALESCE(entry_price, 0) AS entry_price,
        as_of
    FROM positions
    WHERE account_id = %(account_id)s
    ORDER BY market, as_of DESC
)
SELECT market, quantity, entry_price
FROM latest_positions
WHERE quantity IS NOT NULL
"""

PRICE_HISTORY_QUERY = """
SELECT market, bucket_start, close
FROM ohlcv_bars
WHERE market = ANY(%(markets)s)
  AND bucket_start >= %(start)s
ORDER BY market, bucket_start
"""

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS scenario_runs (
    account_id TEXT NOT NULL,
    shock_pct DOUBLE PRECISION NOT NULL,
    vol_multiplier DOUBLE PRECISION NOT NULL,
    results_json JSONB NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

INSERT_RUN_SQL = """
INSERT INTO scenario_runs (
    account_id,
    shock_pct,
    vol_multiplier,
    results_json,
    ts
) VALUES (%(account_id)s, %(shock_pct)s, %(vol_multiplier)s, %(results_json)s::jsonb, %(ts)s)
"""


class ScenarioRunRequest(BaseModel):
    """Request payload describing the scenario shock configuration."""

    shock_pct: float = Field(
        ...,
        ge=-1.0,
        le=1.0,
        description="Relative price move applied to all instruments",
    )
    vol_multiplier: float = Field(
        ...,
        ge=0.0,
        le=10.0,
        description="Multiplier applied to historical volatility",
    )


class ScenarioRunResponse(BaseModel):
    """Structured response summarising the simulated portfolio metrics."""

    projected_pnl: float
    var95: float
    cvar95: float
    drawdown: float


app = FastAPI(title="Scenario Simulator", version="1.0.0")
app.add_middleware(CorrelationIdMiddleware)


def _ensure_driver() -> None:
    if psycopg is None:  # pragma: no cover - executed when psycopg missing
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TimescaleDB driver (psycopg) is not installed in this environment.",
        )


def _open_conn(session: "TimescaleSession") -> psycopg.Connection:
    _ensure_driver()
    if sql is None:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=500, detail="SQL helper unavailable")

    try:
        conn = psycopg.connect(session.dsn, row_factory=dict_row)
    except Exception as exc:  # pragma: no cover - connection issues are operational faults
        LOGGER.exception("Failed to connect to Timescale for account %s", session.account_schema)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to connect to Timescale",
        ) from exc

    try:
        conn.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(session.account_schema))
        )
    except Exception as exc:
        conn.close()
        LOGGER.exception("Failed to set search_path for schema %s", session.account_schema)
        raise HTTPException(status_code=500, detail="Failed to configure Timescale session") from exc

    return conn


def _ensure_tables_for_session(session: "TimescaleSession") -> None:
    schema = session.account_schema
    with _ENSURE_LOCK:
        if schema in _ENSURED_SCHEMAS:
            return
        try:
            with _open_conn(session) as conn:
                conn.execute(CREATE_TABLE_SQL)
                conn.commit()
        except HTTPException:
            raise
        except Exception as exc:  # pragma: no cover - defensive logging for startup issues
            LOGGER.exception("Failed to ensure scenario_runs table for schema %s", schema)
            raise HTTPException(
                status_code=500,
                detail="Failed to initialise scenario storage",
            ) from exc
        _ENSURED_SCHEMAS.add(schema)


@app.on_event("startup")
def startup_event() -> None:
    if not _DEFAULT_ACCOUNT_ID:
        return
    session = get_timescale_session(_DEFAULT_ACCOUNT_ID)
    _ensure_tables_for_session(session)


def _row_to_mapping(row: Any, columns: Sequence[str]) -> Dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    try:
        return {columns[idx]: row[idx] for idx in range(len(columns))}
    except Exception:  # pragma: no cover - defensive conversion for unexpected rows
        return {column: getattr(row, column, None) for column in columns}


def _coerce_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return parsed
    return None


def _fetch_positions(conn: psycopg.Connection, account_id: str) -> list[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(POSITIONS_QUERY, {"account_id": account_id})
        rows = cur.fetchall()

    records = [_row_to_mapping(row, ["market", "quantity", "entry_price"]) for row in rows]
    return _normalize_spot_positions(records)


def _fetch_price_history(conn: psycopg.Connection, markets: Iterable[str]) -> list[Dict[str, Any]]:
    symbols = _canonical_spot_markets(markets)
    if not symbols:
        return []

    start = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    with conn.cursor() as cur:
        cur.execute(
            PRICE_HISTORY_QUERY,
            {"markets": symbols, "start": start},
        )
        rows = cur.fetchall()
    if not rows:
        return []

    history: list[Dict[str, Any]] = []
    for raw in rows:
        record = _row_to_mapping(raw, ["market", "bucket_start", "close"])
        market = normalize_spot_symbol(record.get("market"))
        if not market or not is_spot_symbol(market):
            continue
        bucket = _coerce_datetime(record.get("bucket_start"))
        price = _coerce_float(record.get("close"))
        if bucket is None or price is None:
            continue
        history.append({"market": market, "bucket_start": bucket, "close": price})
    return history


def _fetch_nav_history(conn: psycopg.Connection, account_id: str) -> list[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=LOOKBACK_DAYS)
    with conn.cursor() as cur:
        cur.execute(
            PNL_CURVE_QUERY,
            {"account_id": account_id, "start": start, "end": now},
        )
        rows = cur.fetchall()
    return [_row_to_mapping(row, ["as_of", "nav"]) for row in rows]


def _canonical_spot_markets(markets: Iterable[object]) -> list[str]:
    return filter_spot_symbols(markets, logger=LOGGER)


def _normalize_spot_positions(rows: Iterable[Mapping[str, Any]]) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    for record in rows:
        raw_market = record.get("market")
        canonical = normalize_spot_symbol(raw_market)
        if not canonical or not is_spot_symbol(canonical):
            LOGGER.warning(
                "Dropping non-spot position '%s' during scenario simulation", raw_market
            )
            continue

        quantity = _coerce_float(record.get("quantity")) or 0.0
        entry_price = _coerce_float(record.get("entry_price")) or 0.0
        normalized.append({"market": canonical, "quantity": quantity, "entry_price": entry_price})

    if not normalized:
        return []

    aggregated: Dict[str, Dict[str, float]] = {}
    for row in normalized:
        market = row["market"]
        if market not in aggregated:
            aggregated[market] = {
                "market": market,
                "quantity": 0.0,
                "entry_price": row["entry_price"],
            }
        aggregated_row = aggregated[market]
        aggregated_row["quantity"] += row["quantity"]
        aggregated_row["entry_price"] = row["entry_price"]
    return [dict(value) for value in aggregated.values()]


def _latest_prices(price_rows: Sequence[Mapping[str, Any]]) -> Mapping[str, float]:
    latest: Dict[str, tuple[datetime, float]] = {}
    for record in price_rows:
        market = record.get("market")
        bucket = record.get("bucket_start")
        price = _coerce_float(record.get("close"))
        if not isinstance(market, str) or not isinstance(bucket, datetime) or price is None:
            continue
        existing = latest.get(market)
        if existing is None or bucket > existing[0]:
            latest[market] = (bucket, price)
    return {market: value for market, (_, value) in latest.items()}


def _portfolio_exposures(
    positions: Sequence[Mapping[str, Any]],
    prices: Mapping[str, float],
) -> Dict[str, float]:
    if not positions or not prices:
        return {}
    exposures: Dict[str, float] = {}
    for record in positions:
        market = record.get("market")
        if not isinstance(market, str) or market not in prices:
            continue
        quantity = _coerce_float(record.get("quantity"))
        if quantity is None:
            continue
        exposures[market] = quantity * float(prices[market])
    return exposures


def _apply_ml_forecast_to_exposures(
    exposures: Mapping[str, float], forecast: ForecastResult | None
) -> Dict[str, float]:
    if forecast is None:
        return dict(exposures)

    predicted_total = float(forecast.value)
    if predicted_total <= 0:
        return dict(exposures)

    total_abs = sum(abs(value) for value in exposures.values())
    if total_abs <= 0:
        return dict(exposures)

    scale = predicted_total / total_abs
    return {market: value * scale for market, value in exposures.items()}


def _compute_returns(price_history: Sequence[Mapping[str, Any]]) -> list[Dict[str, Any]]:
    if not price_history:
        return []

    grouped: Dict[str, List[tuple[datetime, float]]] = {}
    for record in price_history:
        market = record.get("market")
        bucket = record.get("bucket_start")
        price = _coerce_float(record.get("close"))
        if not isinstance(market, str) or not isinstance(bucket, datetime) or price is None:
            continue
        grouped.setdefault(market, []).append((bucket, price))

    returns: list[Dict[str, Any]] = []
    for market, entries in grouped.items():
        entries.sort(key=lambda item: item[0])
        previous_price: float | None = None
        for bucket, price in entries:
            if previous_price is None:
                previous_price = price
                continue
            if previous_price == 0:
                previous_price = price
                continue
            change = (price / previous_price) - 1.0
            previous_price = price
            if math.isnan(change) or math.isinf(change):
                continue
            returns.append({"bucket_start": bucket, "market": market, "return": change})

    returns.sort(key=lambda item: item["bucket_start"])
    return returns


def _scenario_pnl_series(
    returns_rows: Sequence[Mapping[str, Any]],
    exposures: Mapping[str, float],
    *,
    shock_pct: float,
    vol_multiplier: float,
) -> List[float]:
    if not exposures:
        return []

    total_exposure = sum(exposures.values())
    if not returns_rows:
        return [total_exposure * shock_pct] if total_exposure else []

    timeline: Dict[datetime, float] = {}
    for record in returns_rows:
        bucket = record.get("bucket_start")
        market = record.get("market")
        ret_value = _coerce_float(record.get("return"))
        if not isinstance(bucket, datetime) or not isinstance(market, str) or ret_value is None:
            continue
        if market not in exposures:
            continue
        if bucket not in timeline:
            timeline[bucket] = total_exposure * shock_pct
        timeline[bucket] += vol_multiplier * exposures[market] * ret_value

    if not timeline:
        return [total_exposure * shock_pct] if total_exposure else []

    ordered = [timeline[bucket] for bucket in sorted(timeline)]
    return ordered


def _quantile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    if np is not None:  # pragma: no cover - exercised when numpy available
        return float(np.quantile(values, q))

    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[int(position)])
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    weight = position - lower
    return float(lower_value + weight * (upper_value - lower_value))


def _aggregate_metrics(
    pnl_values: Sequence[float],
    exposures: Mapping[str, float],
    shock_pct: float,
) -> Dict[str, float]:
    total_exposure = sum(exposures.values()) if exposures else 0.0
    projected = total_exposure * shock_pct
    if not pnl_values:
        return {"projected_pnl": projected, "var95": 0.0, "cvar95": 0.0, "drawdown": 0.0}

    losses = list(pnl_values)
    if not losses:
        return {"projected_pnl": projected, "var95": 0.0, "cvar95": 0.0, "drawdown": 0.0}

    var_threshold = _quantile(losses, 0.05)
    var95 = max(0.0, -var_threshold)
    tail_losses = [value for value in losses if value <= var_threshold]
    if tail_losses:
        tail_average = sum(tail_losses) / len(tail_losses)
        cvar95 = max(0.0, -tail_average)
    else:
        cvar95 = 0.0

    cumulative = 0.0
    running_max = -math.inf
    max_drawdown = 0.0
    for value in losses:
        cumulative += value
        if cumulative > running_max:
            running_max = cumulative
        drawdown = cumulative - running_max
        if drawdown < 0 and -drawdown > max_drawdown:
            max_drawdown = -drawdown

    return {
        "projected_pnl": projected,
        "var95": var95,
        "cvar95": cvar95,
        "drawdown": max_drawdown,
    }


def _store_run(
    conn: psycopg.Connection,
    results: Mapping[str, float],
    payload: ScenarioRunRequest,
    actor_account: str,
) -> None:
    params = {
        "account_id": actor_account,
        "shock_pct": float(payload.shock_pct),
        "vol_multiplier": float(payload.vol_multiplier),
        "results_json": json.dumps(results),
        "ts": datetime.now(timezone.utc),
    }
    conn.execute(INSERT_RUN_SQL, params)
    conn.commit()


@app.post("/scenario/run", response_model=ScenarioRunResponse)
def run_scenario(
    payload: ScenarioRunRequest,
    actor_account: str = Depends(require_admin_account),
) -> ScenarioRunResponse:
    """Simulate the portfolio under a combined price shock and volatility shift."""

    session = get_timescale_session(actor_account)
    _ensure_tables_for_session(session)

    try:
        with _open_conn(session) as conn:
            positions = _fetch_positions(conn, actor_account)
            markets = [row["market"] for row in positions if isinstance(row.get("market"), str)]
            price_history = _fetch_price_history(conn, markets)
            prices = _latest_prices(price_history)
            exposures = _portfolio_exposures(positions, prices)
            nav_history = _fetch_nav_history(conn, actor_account)

            ml_forecast: ForecastResult | None = None
            try:
                pipeline = get_exposure_ml_pipeline(
                    actor_account,
                    retrain_cadence=timedelta(minutes=EXPOSURE_RETRAIN_MINUTES),
                    lookback_days=LOOKBACK_DAYS,
                )
                ml_forecast = pipeline.forecast(
                    nav_rows=nav_history,
                    positions_rows=positions,
                    now=datetime.now(timezone.utc),
                    horizon_days=7,
                )
            except Exception:  # pragma: no cover - optional pipeline
                LOGGER.debug(
                    "Scenario ML exposure forecast unavailable for %s",
                    actor_account,
                    exc_info=True,
                )
                ml_forecast = None

            exposures = _apply_ml_forecast_to_exposures(exposures, ml_forecast)
            returns = _compute_returns(price_history)
            pnl_values = _scenario_pnl_series(
                returns,
                exposures,
                shock_pct=payload.shock_pct,
                vol_multiplier=payload.vol_multiplier,
            )
            metrics = _aggregate_metrics(pnl_values, exposures, payload.shock_pct)
            _store_run(conn, metrics, payload, actor_account)
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - protects against database/runtime issues
        LOGGER.exception("Scenario simulation failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to execute scenario simulation",
        ) from exc

    return ScenarioRunResponse(**metrics)
