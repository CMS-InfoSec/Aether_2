"""FastAPI service for simulating governance limit changes via backtests."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from config_sandbox import _current_config, _deep_merge, _run_backtest
from services.common.config import get_timescale_session
from shared.correlation import CorrelationIdMiddleware

try:  # pragma: no cover - psycopg optional in some environments
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - executed when psycopg missing
    psycopg = None  # type: ignore[assignment]
    sql = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)

ACCOUNT_ID = os.getenv("AETHER_ACCOUNT_ID", "default")
TIMESCALE = get_timescale_session(ACCOUNT_ID)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS governance_sims (
    run_id UUID PRIMARY KEY,
    changes_json JSONB NOT NULL,
    metrics_json JSONB NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

INSERT_RUN_SQL = """
INSERT INTO governance_sims (
    run_id,
    changes_json,
    metrics_json,
    ts
) VALUES (%(run_id)s, %(changes_json)s::jsonb, %(metrics_json)s::jsonb, %(ts)s)
"""


class GovernanceSimulationRequest(BaseModel):
    """Payload describing the governance configuration changes to evaluate."""

    config_changes: Dict[str, Any] = Field(
        default_factory=dict,
        description="Partial configuration overrides proposed for approval",
    )


class GovernanceSimulationResponse(BaseModel):
    """Response summarising baseline vs candidate risk and pnl metrics."""

    baseline_drawdown: float
    new_drawdown: float
    drawdown_delta: float
    baseline_pnl: float
    new_pnl: float
    pnl_delta: float


app = FastAPI(title="Governance Simulator", version="1.0.0")
app.add_middleware(CorrelationIdMiddleware)


def _ensure_driver() -> None:
    if psycopg is None:  # pragma: no cover - executed when psycopg missing
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TimescaleDB driver (psycopg) is not installed in this environment.",
        )


def _get_conn() -> psycopg.Connection:
    _ensure_driver()
    if sql is None:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=500, detail="SQL helper unavailable")

    conn = psycopg.connect(TIMESCALE.dsn, row_factory=dict_row)
    conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(TIMESCALE.account_schema)))
    return conn


def _ensure_tables() -> None:
    try:
        with _get_conn() as conn:
            conn.execute(CREATE_TABLE_SQL)
            conn.commit()
    except HTTPException:
        raise
    except Exception:  # pragma: no cover - defensive logging for startup issues
        LOGGER.exception("Failed to ensure governance_sims table exists")
        raise HTTPException(status_code=500, detail="Failed to initialise governance simulator storage")


@app.on_event("startup")
def startup_event() -> None:
    _ensure_tables()


def _record_simulation(run_id: str, changes: Dict[str, Any], metrics: Dict[str, Any]) -> None:
    payload = {
        "run_id": run_id,
        "changes_json": json.dumps(changes),
        "metrics_json": json.dumps(metrics),
        "ts": datetime.now(timezone.utc),
    }
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(INSERT_RUN_SQL, payload)
            conn.commit()
    except HTTPException:
        raise
    except Exception:  # pragma: no cover - defensive logging for persistence failures
        LOGGER.exception("Failed to persist governance simulation run")
        raise HTTPException(status_code=500, detail="Failed to persist simulation run")


@app.post("/governance/simulate", response_model=GovernanceSimulationResponse)
def simulate_governance_change(payload: GovernanceSimulationRequest) -> GovernanceSimulationResponse:
    baseline_config = _current_config()
    candidate_config = _deep_merge(baseline_config, payload.config_changes)

    baseline_metrics = _run_backtest(baseline_config)
    candidate_metrics = _run_backtest(candidate_config)

    response_payload = GovernanceSimulationResponse(
        baseline_drawdown=float(baseline_metrics["max_drawdown"]),
        new_drawdown=float(candidate_metrics["max_drawdown"]),
        drawdown_delta=float(candidate_metrics["max_drawdown"] - baseline_metrics["max_drawdown"]),
        baseline_pnl=float(baseline_metrics["pnl"]),
        new_pnl=float(candidate_metrics["pnl"]),
        pnl_delta=float(candidate_metrics["pnl"] - baseline_metrics["pnl"]),
    )

    run_id = str(uuid4())
    metrics_record = {
        "baseline": baseline_metrics,
        "candidate": candidate_metrics,
        "response": response_payload.dict(),
    }

    _record_simulation(run_id, payload.config_changes, metrics_record)

    return response_payload
