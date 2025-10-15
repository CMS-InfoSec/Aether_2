"""Prompt refinement service that monitors model health and stores prompt artifacts."""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Sequence

from typing import TYPE_CHECKING

try:  # pragma: no cover - optional dependency for database access.
    import psycopg2  # type: ignore[import-not-found]
    from psycopg2 import errors, sql  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - dependency might be unavailable in tests.
    psycopg2 = None  # type: ignore[assignment]

    class _ErrorsModule:  # pragma: no cover - executed only without psycopg2.
        class UndefinedTable(Exception):
            """Stand-in error matching psycopg2's UndefinedTable."""

        class UndefinedColumn(Exception):
            """Stand-in error matching psycopg2's UndefinedColumn."""

    errors = _ErrorsModule()  # type: ignore[assignment]
    sql = None  # type: ignore[assignment]

try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import FastAPI, Header, HTTPException, Request, status
    from fastapi.responses import JSONResponse
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[misc]
        FastAPI,
        Header,
        HTTPException,
        JSONResponse,
        Request,
        status,
    )
from pydantic import BaseModel, Field

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only.
    from psycopg2.extensions import connection as TimescaleConnection
else:  # pragma: no cover - runtime fallback when psycopg2 is absent.
    TimescaleConnection = Any  # type: ignore[assignment]

from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

from services.common.config import get_timescale_session


LOGGER = logging.getLogger(__name__)

ACCOUNT_ID = os.getenv("AETHER_ACCOUNT_ID", "default")
TIMESCALE = get_timescale_session(ACCOUNT_ID)

POLL_INTERVAL_SECONDS = int(os.getenv("PROMPT_REFINER_POLL_INTERVAL", "900"))
RELATIVE_DROP_THRESHOLD = float(os.getenv("PROMPT_REFINER_RELATIVE_DROP", "0.15"))


def _require_database() -> None:
    """Ensure the Timescale dependency is available before performing I/O."""

    if psycopg2 is None or sql is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Timescale database driver is not available",
        )


def _authenticate_request_headers(
    authorization: str | None,
    x_account_id: str | None,
) -> str | None:
    """Return the actor account when an authorization header is present."""

    if not authorization or not authorization.strip():
        return None

    if x_account_id and x_account_id.strip():
        return x_account_id.strip()
    return ACCOUNT_ID


def _authenticate_request(request: Request) -> str:
    """Resolve the caller account from the incoming HTTP request."""

    actor = _authenticate_request_headers(
        request.headers.get("authorization"),
        request.headers.get("x-account-id"),
    )
    if actor is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing Authorization header.",
        )
    return actor


def _json_ready(value: Any) -> Any:
    """Convert dataclasses and datetimes into JSON-friendly structures."""

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if hasattr(value, "model_dump"):
        return _json_ready(value.model_dump())  # type: ignore[arg-type]
    return value


async def require_admin_account(
    request: Request,
    authorization: str | None = Header(None, alias="Authorization"),
    x_account_id: str | None = Header(None, alias="X-Account-ID"),
) -> str:
    """Compatibility shim retained for tests that override this dependency."""

    actor = _authenticate_request_headers(authorization, x_account_id)
    if actor is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing Authorization header.",
        )
    return actor


def _connect() -> TimescaleConnection:
    """Open a PostgreSQL connection scoped to the account schema."""

    _require_database()

    conn = psycopg2.connect(TIMESCALE.dsn)
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(TIMESCALE.account_schema)
            )
        )
    return conn


def _ensure_tables() -> None:
    """Ensure the storage table for prompt artifacts exists."""

    if psycopg2 is None:
        LOGGER.warning(
            "Skipping prompt refiner table creation because psycopg2 is not installed"
        )
        return

    create_prompts_stmt = """
    CREATE TABLE IF NOT EXISTS ai_prompts (
        run_id TEXT NOT NULL,
        prompt_text TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    with _connect() as conn, conn.cursor() as cursor:
        cursor.execute(create_prompts_stmt)


def _latest_sharpe(conn: TimescaleConnection) -> tuple[float | None, datetime | None]:
    """Return the most recent Sharpe ratio and timestamp if available."""

    query = """
        SELECT evaluated_at, sharpe_ratio
        FROM model_performance
        ORDER BY evaluated_at DESC
        LIMIT 1
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
    except errors.UndefinedTable:
        return None, None

    if not row:
        return None, None
    evaluated_at, sharpe = row
    return float(sharpe), evaluated_at


def _latest_win_rate(conn: TimescaleConnection) -> tuple[float | None, datetime | None]:
    """Attempt to load the most recent win rate metric."""

    # First attempt to read from model_performance if the column exists.
    query_variants = (
        (
            """
            SELECT evaluated_at, win_rate
            FROM model_performance
            ORDER BY evaluated_at DESC
            LIMIT 1
            """,
            (errors.UndefinedColumn, errors.UndefinedTable),
        ),
        (
            """
            SELECT evaluated_at, win_rate
            FROM trade_performance
            ORDER BY evaluated_at DESC
            LIMIT 1
            """,
            (errors.UndefinedTable,),
        ),
    )

    for query, ignored_errors in query_variants:
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                row = cursor.fetchone()
        except ignored_errors:
            continue
        else:
            if row:
                evaluated_at, win_rate = row
                if win_rate is not None:
                    return float(win_rate), evaluated_at
    return None, None


def _latest_drift_rate(
    conn: TimescaleConnection,
) -> tuple[float | None, datetime | None]:
    """Return the proportion of drifting features in the most recent checks."""

    recent_window = datetime.now(timezone.utc) - timedelta(days=1)
    query = """
        SELECT
            MAX(checked_at) AS last_checked,
            AVG(CASE WHEN flagged THEN 1.0 ELSE 0.0 END) AS drift_ratio
        FROM drift_results
        WHERE checked_at >= %s
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (recent_window,))
            row = cursor.fetchone()
    except errors.UndefinedTable:
        return None, None

    if not row:
        return None, None
    last_checked, drift_ratio = row
    if drift_ratio is None:
        return None, last_checked
    return float(drift_ratio), last_checked


@dataclass
class PerformanceSnapshot:
    """Structured view of the core model health metrics."""

    collected_at: datetime
    sharpe: float | None
    win_rate: float | None
    drift: float | None


@dataclass(frozen=True)
class PerformanceThresholds:
    """Thresholds used to determine if performance degradation occurred."""

    sharpe: float = 1.0
    win_rate: float = 0.55
    drift: float = 0.25


class PerformanceMetrics(BaseModel):
    """Pydantic schema for ingesting or returning metric snapshots."""

    sharpe: float | None = Field(None, description="Latest Sharpe ratio")
    win_rate: float | None = Field(None, description="Recent win rate of the strategy")
    drift: float | None = Field(None, description="Proportion of features breaching drift thresholds")
    collected_at: datetime | None = Field(
        None, description="Timestamp associated with the metric snapshot"
    )


class PromptRecord(BaseModel):
    """Representation of an individual prompt artifact."""

    run_id: str
    prompt_text: str
    created_at: datetime


class PromptTestRequest(BaseModel):
    """Payload accepted by ``POST /ml/prompts/test``."""

    prompt_text: str = Field(..., description="Prompt variant that was evaluated")
    run_id: str | None = Field(
        None,
        description="Identifier for the experiment. Generated when omitted.",
    )
    metrics: PerformanceMetrics | None = Field(
        None,
        description="Observed metrics after testing the prompt variant",
    )
    tags: List[str] | None = Field(
        default=None,
        description="Optional human readable tags describing the experiment",
    )


class PromptTestResponse(BaseModel):
    """Response emitted by ``POST /ml/prompts/test``."""

    run_id: str
    stored_prompt: PromptRecord
    degradations: List[str]
    generated_prompts: List[PromptRecord]
    metrics: PerformanceMetrics


class PromptRefiner:
    """Encapsulates logic for monitoring metrics and creating prompt variants."""

    def __init__(
        self,
        thresholds: PerformanceThresholds | None = None,
        poll_interval: int = POLL_INTERVAL_SECONDS,
    ) -> None:
        self.thresholds = thresholds or PerformanceThresholds()
        self.poll_interval = poll_interval
        self._last_snapshot: PerformanceSnapshot | None = None
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Schedule the monitoring loop on the running event loop."""

        if psycopg2 is None:
            LOGGER.warning(
                "Prompt refinement background loop disabled because psycopg2 is missing"
            )
            return

        if self._task is not None and not self._task.done():
            return
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._monitor_loop())

    async def stop(self) -> None:
        """Cancel the background monitoring loop if it is running."""

        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _monitor_loop(self) -> None:
        """Periodic loop that evaluates metrics and writes prompt candidates."""

        while True:
            try:
                snapshot = self.fetch_latest_snapshot()
                if snapshot is not None:
                    degradations = self._assess(snapshot, update_baseline=True)
                    if degradations:
                        run_id = f"auto-{uuid.uuid4()}"
                        candidates = self._generate_prompts(snapshot, degradations)
                        if candidates:
                            self._persist_candidates(run_id, candidates)
            except Exception:  # pragma: no cover - defensive guard
                LOGGER.exception("Prompt refinement loop failed")
            await asyncio.sleep(max(self.poll_interval, 60))

    def fetch_latest_snapshot(self) -> PerformanceSnapshot | None:
        """Load the most recent metrics from the database."""

        with _connect() as conn:
            sharpe, sharpe_at = _latest_sharpe(conn)
            win_rate, win_rate_at = _latest_win_rate(conn)
            drift, drift_at = _latest_drift_rate(conn)

        timestamps = [ts for ts in (sharpe_at, win_rate_at, drift_at) if ts is not None]
        if timestamps:
            collected_at = max(timestamps)
        else:
            collected_at = datetime.now(timezone.utc)

        if sharpe is None and win_rate is None and drift is None:
            return None

        return PerformanceSnapshot(
            collected_at=collected_at,
            sharpe=sharpe,
            win_rate=win_rate,
            drift=drift,
        )

    def snapshot_from_metrics(
        self, metrics: PerformanceMetrics | None
    ) -> PerformanceSnapshot | None:
        """Create a snapshot from ad-hoc metrics provided by users."""

        if metrics is None:
            return self.fetch_latest_snapshot()

        collected_at = metrics.collected_at or datetime.now(timezone.utc)
        if (
            metrics.sharpe is None
            and metrics.win_rate is None
            and metrics.drift is None
        ):
            return None

        return PerformanceSnapshot(
            collected_at=collected_at,
            sharpe=metrics.sharpe,
            win_rate=metrics.win_rate,
            drift=metrics.drift,
        )

    def _assess(
        self, snapshot: PerformanceSnapshot, update_baseline: bool
    ) -> List[str]:
        """Evaluate a snapshot and optionally update the baseline cache."""

        degradations: List[str] = []
        last = self._last_snapshot

        sharpe = snapshot.sharpe
        if sharpe is not None:
            if sharpe < self.thresholds.sharpe:
                degradations.append("sharpe_below_threshold")
            elif last and last.sharpe is not None:
                if sharpe < last.sharpe * (1 - RELATIVE_DROP_THRESHOLD):
                    degradations.append("sharpe_relative_drop")

        win_rate = snapshot.win_rate
        if win_rate is not None:
            if win_rate < self.thresholds.win_rate:
                degradations.append("win_rate_below_threshold")
            elif last and last.win_rate is not None:
                if win_rate < last.win_rate * (1 - RELATIVE_DROP_THRESHOLD):
                    degradations.append("win_rate_relative_drop")

        drift = snapshot.drift
        if drift is not None:
            if drift > self.thresholds.drift:
                degradations.append("drift_above_threshold")
            elif last and last.drift is not None:
                if drift > last.drift * (1 + RELATIVE_DROP_THRESHOLD):
                    degradations.append("drift_trending_up")

        if update_baseline:
            self._last_snapshot = snapshot

        return degradations

    def _generate_prompts(
        self, snapshot: PerformanceSnapshot, degradations: Sequence[str]
    ) -> List[str]:
        """Generate prompt variants tailored to the degradation signals."""

        metrics_summary = (
            "Latest metrics -- "
            f"Sharpe: {snapshot.sharpe if snapshot.sharpe is not None else 'N/A'}, "
            f"Win rate: {snapshot.win_rate if snapshot.win_rate is not None else 'N/A'}, "
            f"Drift ratio: {snapshot.drift if snapshot.drift is not None else 'N/A'}."
        )

        prompts: List[str] = []

        if any(reason.startswith("sharpe") for reason in degradations):
            prompts.append(
                (
                    "Feature engineering refinement required: "
                    "Design new features or resampling strategies that improve risk-adjusted returns "
                    "while reducing noise. Provide step-by-step plans, target markets, and validation "
                    "experiments. "
                )
                + metrics_summary
            )

            prompts.append(
                (
                    "Training pipeline adjustment: "
                    "Recommend changes to model architectures, loss weighting, or regularisation that "
                    "stabilise Sharpe ratio without overfitting. Include curriculum schedules and "
                    "hyperparameter sweeps. "
                )
                + metrics_summary
            )

        if any(reason.startswith("win_rate") for reason in degradations):
            prompts.append(
                (
                    "Trade execution focus: "
                    "Suggest engineered features capturing microstructure signals or order-book context "
                    "to lift win rate. Outline data augmentation and evaluation tactics. "
                )
                + metrics_summary
            )

        if any(reason.startswith("drift") for reason in degradations):
            prompts.append(
                (
                    "Drift mitigation prompt: "
                    "Propose monitoring dashboards and adaptive feature stores that counteract detected "
                    "drift. Include retraining triggers and fallback heuristics. "
                )
                + metrics_summary
            )

        if not prompts:
            prompts.append(
                (
                    "General model health prompt: Summarise opportunities to enhance feature engineering "
                    "and training workflows using the provided metrics snapshot. "
                )
                + metrics_summary
            )

        return prompts

    def generate_candidates(
        self, snapshot: PerformanceSnapshot, degradations: Sequence[str]
    ) -> List[str]:
        """Public wrapper to create prompt candidates from a snapshot."""

        return self._generate_prompts(snapshot, degradations)

    def _persist_candidates(self, run_id: str, prompts: Iterable[str]) -> List[PromptRecord]:
        """Persist generated prompts and return the stored records."""

        stored: List[PromptRecord] = []
        with _connect() as conn, conn.cursor() as cursor:
            for prompt in prompts:
                cursor.execute(
                    """
                    INSERT INTO ai_prompts (run_id, prompt_text)
                    VALUES (%s, %s)
                    RETURNING run_id, prompt_text, created_at
                    """,
                    (run_id, prompt),
                )
                row = cursor.fetchone()
                if row is None:
                    continue
                stored.append(
                    PromptRecord(
                        run_id=row[0], prompt_text=row[1], created_at=row[2]
                    )
                )
        return stored

    def record_prompt(self, run_id: str, prompt_text: str) -> PromptRecord:
        """Persist an individual prompt artifact."""

        stored = self._persist_candidates(run_id, [prompt_text])
        if not stored:
            raise RuntimeError("Prompt insert returned no rows")
        return stored[0]

    def evaluate_metrics(
        self, metrics: PerformanceMetrics | None, update_baseline: bool
    ) -> tuple[PerformanceMetrics, List[str]]:
        """Assess metrics provided by the caller and optionally update the baseline."""

        snapshot = self.snapshot_from_metrics(metrics)
        if snapshot is None:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="No metrics available to evaluate",
            )

        degradations = self._assess(snapshot, update_baseline=update_baseline)
        return (
            PerformanceMetrics(
                sharpe=snapshot.sharpe,
                win_rate=snapshot.win_rate,
                drift=snapshot.drift,
                collected_at=snapshot.collected_at,
            ),
            degradations,
        )

    def latest_prompt(self) -> PromptRecord:
        """Fetch the most recently stored prompt artifact."""

        query = """
            SELECT run_id, prompt_text, created_at
            FROM ai_prompts
            ORDER BY created_at DESC
            LIMIT 1
        """

        with _connect() as conn, conn.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()

        if not row:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                detail="No prompt artifacts have been recorded",
            )

        return PromptRecord(run_id=row[0], prompt_text=row[1], created_at=row[2])


refiner = PromptRefiner()
app = FastAPI(title="Prompt Refinement Service", version="1.0.0")


@app.on_event("startup")
async def _on_startup() -> None:
    _ensure_tables()
    refiner.start()


@app.on_event("shutdown")
async def _on_shutdown() -> None:
    await refiner.stop()


def _assert_account_scope(actor_account: str, requested_account: str | None) -> None:
    """Ensure the authenticated caller matches the service scope and request."""

    normalized_actor = actor_account.strip().lower()
    normalized_scope = ACCOUNT_ID.strip().lower()
    if normalized_scope and normalized_actor != normalized_scope:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authenticated account does not match prompt refiner scope.",
        )

    if requested_account:
        normalized_requested = requested_account.strip().lower()
        if normalized_requested and normalized_requested != normalized_actor:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Requested account does not match authenticated session.",
            )


@app.get("/ml/prompts/latest", response_model=PromptRecord)
async def get_latest_prompt(
    request: Request,
    account_id: str | None = None,
) -> PromptRecord:
    """Return the latest generated prompt artifact."""

    actor = _authenticate_request(request)
    _assert_account_scope(actor, account_id)
    return refiner.latest_prompt()


@app.post("/ml/prompts/test", response_model=PromptTestResponse, status_code=status.HTTP_201_CREATED)
async def post_prompt_test(
    request: Request,
    payload: PromptTestRequest,
    account_id: str | None = None,
) -> PromptTestResponse:
    """Record a prompt experiment and surface follow-up candidates if needed."""

    actor = _authenticate_request(request)
    _assert_account_scope(actor, account_id)
    run_id = payload.run_id or str(uuid.uuid4())
    stored_prompt = refiner.record_prompt(run_id, payload.prompt_text)

    metrics, degradations = refiner.evaluate_metrics(
        payload.metrics, update_baseline=False
    )

    generated_prompts: List[PromptRecord] = []
    if degradations:
        generated = refiner.generate_candidates(
            PerformanceSnapshot(
                collected_at=metrics.collected_at or datetime.now(timezone.utc),
                sharpe=metrics.sharpe,
                win_rate=metrics.win_rate,
                drift=metrics.drift,
            ),
            degradations,
        )
        generated_prompts = refiner._persist_candidates(run_id, generated)

    response = PromptTestResponse(
        run_id=run_id,
        stored_prompt=stored_prompt,
        degradations=degradations,
        generated_prompts=generated_prompts,
        metrics=metrics,
    )
    return JSONResponse(
        content=_json_ready(response),
        status_code=status.HTTP_201_CREATED,
    )


__all__ = [
    "app",
    "PromptRefiner",
    "PerformanceMetrics",
    "PromptRecord",
    "PromptTestRequest",
    "PromptTestResponse",
]
