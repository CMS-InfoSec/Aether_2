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
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Mapping, Set

import numpy as np
import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from services.common.config import get_timescale_session
from services.common.security import require_admin_account
from shared.correlation import CorrelationIdMiddleware

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

    shock_pct: float = Field(..., ge=-1.0, le=1.0, description="Relative price move applied to all instruments")
    vol_multiplier: float = Field(..., ge=0.0, le=10.0, description="Multiplier applied to historical volatility")


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


def _fetch_positions(conn: psycopg.Connection, account_id: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(POSITIONS_QUERY, {"account_id": account_id})
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["market", "quantity", "entry_price"])
    return pd.DataFrame(rows)


def _fetch_price_history(conn: psycopg.Connection, markets: Iterable[str]) -> pd.DataFrame:
    symbols = list({market for market in markets if market})
    if not symbols:
        return pd.DataFrame(columns=["market", "bucket_start", "close"])

    start = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    with conn.cursor() as cur:
        cur.execute(
            PRICE_HISTORY_QUERY,
            {"markets": symbols, "start": start},
        )
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["market", "bucket_start", "close"])
    frame = pd.DataFrame(rows)
    frame["bucket_start"] = pd.to_datetime(frame["bucket_start"], utc=True)
    return frame


def _latest_prices(price_frame: pd.DataFrame) -> Mapping[str, float]:
    if price_frame.empty:
        return {}
    latest = price_frame.sort_values("bucket_start").groupby("market").tail(1)
    return {row["market"]: float(row["close"]) for row in latest.to_dict("records")}


def _portfolio_exposures(positions: pd.DataFrame, prices: Mapping[str, float]) -> pd.Series:
    if positions.empty or not prices:
        return pd.Series(dtype=float)
    exposures: Dict[str, float] = {}
    for record in positions.to_dict("records"):
        market = record.get("market")
        quantity = record.get("quantity")
        if market not in prices:
            continue
        try:
            qty = float(quantity)
            exposures[market] = qty * float(prices[market])
        except (TypeError, ValueError):
            continue
    return pd.Series(exposures, dtype=float)


def _scenario_pnl_series(
    returns_frame: pd.DataFrame,
    exposures: pd.Series,
    *,
    shock_pct: float,
    vol_multiplier: float,
) -> pd.Series:
    if exposures.empty:
        return pd.Series([0.0])
    if returns_frame.empty:
        immediate = float((exposures * shock_pct).sum())
        return pd.Series([immediate])

    pivot = returns_frame.pivot(index="bucket_start", columns="market", values="return")
    pivot = pivot.fillna(0.0)
    scenario_returns = shock_pct + (vol_multiplier * pivot)
    pnl_series = scenario_returns.mul(exposures, axis=1).sum(axis=1)
    if pnl_series.empty:
        immediate = float((exposures * shock_pct).sum())
        return pd.Series([immediate])
    return pnl_series.sort_index()


def _compute_returns(price_history: pd.DataFrame) -> pd.DataFrame:
    if price_history.empty:
        return price_history
    pivot = price_history.pivot(index="bucket_start", columns="market", values="close").sort_index()
    returns = pivot.pct_change().dropna(how="all")
    if returns.empty:
        return pd.DataFrame(columns=["bucket_start", "market", "return"])
    tidy = returns.reset_index().melt(id_vars="bucket_start", var_name="market", value_name="return")
    tidy.dropna(subset=["return"], inplace=True)
    return tidy


def _aggregate_metrics(pnl_series: pd.Series, exposures: pd.Series, shock_pct: float) -> Dict[str, float]:
    projected = float((exposures * shock_pct).sum()) if not exposures.empty else 0.0
    if pnl_series.empty:
        return {"projected_pnl": projected, "var95": 0.0, "cvar95": 0.0, "drawdown": 0.0}

    losses = pnl_series.values
    if losses.size == 0:
        return {"projected_pnl": projected, "var95": 0.0, "cvar95": 0.0, "drawdown": 0.0}

    var_threshold = float(np.quantile(losses, 0.05))
    var95 = max(0.0, -var_threshold)
    tail_losses = losses[losses <= var_threshold]
    cvar95 = max(0.0, -float(tail_losses.mean())) if tail_losses.size else 0.0

    cumulative = pnl_series.cumsum()
    running_max = cumulative.cummax()
    drawdowns = cumulative - running_max
    max_drawdown = float(-drawdowns.min()) if not drawdowns.empty else 0.0

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
            price_history = _fetch_price_history(conn, positions["market"].tolist())
            prices = _latest_prices(price_history)
            exposures = _portfolio_exposures(positions, prices)
            returns = _compute_returns(price_history)
            pnl_series = _scenario_pnl_series(
                returns,
                exposures,
                shock_pct=payload.shock_pct,
                vol_multiplier=payload.vol_multiplier,
            )
            metrics = _aggregate_metrics(pnl_series, exposures, payload.shock_pct)
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
