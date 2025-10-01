"""FastAPI microservice for monitoring feature drift against training baselines."""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import psycopg2
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from psycopg2 import errors, sql
from psycopg2.extras import RealDictCursor
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, generate_latest

from services.common.config import get_timescale_session

try:  # pragma: no cover - optional dependency for local environments.
    import requests
except Exception:  # pragma: no cover - dependency might be unavailable in tests.
    requests = None  # type: ignore


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------


class DriftStatus(BaseModel):
    """Payload returned by ``GET /drift/status``."""

    features_drift: Dict[str, bool] = Field(..., description="Drift flags per feature")
    psi_scores: Dict[str, float] = Field(..., description="Population stability index by feature")
    ks_scores: Dict[str, Dict[str, float]] = Field(
        ..., description="Kolmogorov-Smirnov statistic and p-value by feature"
    )
    last_checked: datetime | None = Field(
        None, description="Timestamp when the drift calculation last ran"
    )


class DriftAlert(BaseModel):
    """Structured representation of a drift alert and the mitigation taken."""

    triggered_at: datetime = Field(..., description="Timestamp when the alert fired")
    feature_name: str | None = Field(
        None, description="Name of the feature that triggered the alert, if applicable"
    )
    psi_score: float | None = Field(
        None, description="PSI score associated with the alert if relevant"
    )
    action: str = Field(..., description="Mitigation action taken in response to the alert")
    details: Dict[str, Any] | None = Field(
        default=None, description="Additional metadata recorded for the alert"
    )


class DriftAlertsResponse(BaseModel):
    """Response payload returned by ``GET /drift/alerts``."""

    alerts: List[DriftAlert] = Field(..., description="Collection of recent alerts")


@dataclass(frozen=True)
class DriftMetric:
    """Container for the per-feature drift metrics persisted to the database."""

    feature_name: str
    psi: float
    ks_statistic: float
    ks_pvalue: float
    flagged: bool


@dataclass(frozen=True)
class DriftAlertRecord:
    """Structured representation of an alert row from the database."""

    triggered_at: datetime
    feature_name: str | None
    psi_score: float | None
    action: str
    details: Dict[str, Any] | None


@dataclass(frozen=True)
class ModelPerformance:
    """Container for the latest model performance metrics by stage."""

    stage: str
    evaluated_at: datetime
    sharpe: float
    sortino: float


# ---------------------------------------------------------------------------
# Configuration and application setup
# ---------------------------------------------------------------------------


LOGGER = logging.getLogger(__name__)

ACCOUNT_ID = os.getenv("AETHER_ACCOUNT_ID", "default")
TIMESCALE = get_timescale_session(ACCOUNT_ID)

REGISTRY = CollectorRegistry()
DRIFT_FEATURES_TOTAL = Gauge(
    "drift_features_total",
    "Number of features evaluated during the latest drift run.",
    registry=REGISTRY,
)
DRIFT_FLAGS_TOTAL = Gauge(
    "drift_flags_total",
    "Number of features that breached drift thresholds in the latest run.",
    registry=REGISTRY,
)

PSI_THRESHOLD = 0.2
ROLLBACK_THRESHOLD = 0.2
MAX_ALERTS_RETURNED = 100

_daily_task: asyncio.Task[None] | None = None

app = FastAPI(title="Drift Monitoring Service", version="1.0.0")


# ---------------------------------------------------------------------------
# Database utilities
# ---------------------------------------------------------------------------


def _connect() -> psycopg2.extensions.connection:
    """Create a connection to the TimescaleDB instance with the account schema."""

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
    """Create the required tables when the service starts."""

    create_results_stmt = """
    CREATE TABLE IF NOT EXISTS drift_results (
        run_id BIGSERIAL PRIMARY KEY,
        checked_at TIMESTAMPTZ NOT NULL,
        feature_name TEXT NOT NULL,
        psi_score DOUBLE PRECISION NOT NULL,
        ks_statistic DOUBLE PRECISION NOT NULL,
        ks_pvalue DOUBLE PRECISION NOT NULL,
        flagged BOOLEAN NOT NULL
    )
    """
    create_alerts_stmt = """
    CREATE TABLE IF NOT EXISTS drift_alerts (
        alert_id BIGSERIAL PRIMARY KEY,
        triggered_at TIMESTAMPTZ NOT NULL,
        feature_name TEXT,
        psi_score DOUBLE PRECISION,
        action TEXT NOT NULL,
        details JSONB
    )
    """
    create_performance_stmt = """
    CREATE TABLE IF NOT EXISTS model_performance (
        performance_id BIGSERIAL PRIMARY KEY,
        evaluated_at TIMESTAMPTZ NOT NULL,
        model_stage TEXT NOT NULL,
        sharpe_ratio DOUBLE PRECISION NOT NULL,
        sortino_ratio DOUBLE PRECISION NOT NULL
    )
    """
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_results_stmt)
            cursor.execute(create_alerts_stmt)
            cursor.execute(create_performance_stmt)


async def _daily_drift_loop() -> None:
    """Run drift detection once per day in the background."""

    loop = asyncio.get_running_loop()
    while True:
        started_at = datetime.now(timezone.utc)
        try:
            await loop.run_in_executor(None, _perform_daily_checks)
        except Exception:  # pragma: no cover - defensive guard.
            LOGGER.exception("Unexpected failure during scheduled drift check")

        next_run = started_at + timedelta(days=1)
        sleep_seconds = max((next_run - datetime.now(timezone.utc)).total_seconds(), 60.0)
        try:
            await asyncio.sleep(sleep_seconds)
        except asyncio.CancelledError:  # pragma: no cover - cancellation during shutdown.
            raise


def _perform_daily_checks() -> None:
    """Execute drift detection and handle recoverable errors for the scheduler."""

    try:
        _run_drift_detection()
    except HTTPException as exc:  # pragma: no cover - background logging path.
        if exc.status_code in {
            status.HTTP_404_NOT_FOUND,
            status.HTTP_503_SERVICE_UNAVAILABLE,
        }:
            LOGGER.info("Daily drift calculation skipped: %s", exc.detail)
        else:
            LOGGER.exception("Drift detection failed with HTTP status %s", exc.status_code)
    except Exception:  # pragma: no cover - defensive guard.
        LOGGER.exception("Drift detection failed due to an unexpected exception")


@app.on_event("startup")
async def _on_startup() -> None:
    _ensure_tables()
    global _daily_task
    if _daily_task is None or _daily_task.done():
        loop = asyncio.get_running_loop()
        _daily_task = loop.create_task(_daily_drift_loop())


@app.on_event("shutdown")
async def _on_shutdown() -> None:
    global _daily_task
    if _daily_task is not None:
        _daily_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _daily_task
        _daily_task = None


# ---------------------------------------------------------------------------
# Statistical helpers
# ---------------------------------------------------------------------------


def _quantile_edges(values: Sequence[float], bins: int) -> List[float]:
    """Compute inclusive quantile-based bin edges for the PSI calculation."""

    sorted_values = sorted(values)
    if not sorted_values:
        return [0.0, 1.0]
    if len(sorted_values) == 1:
        single = sorted_values[0]
        return [single - 0.5, single + 0.5]

    step = 1.0 / bins
    edges = [sorted_values[0]]
    for i in range(1, bins):
        rank = step * i * (len(sorted_values) - 1)
        lower = math.floor(rank)
        upper = math.ceil(rank)
        if lower == upper:
            edges.append(sorted_values[int(rank)])
            continue
        lower_weight = upper - rank
        upper_weight = rank - lower
        interpolated = (
            sorted_values[lower] * lower_weight
            + sorted_values[upper] * upper_weight
        )
        edges.append(interpolated)
    edges.append(sorted_values[-1])

    # Ensure edges are strictly increasing
    for idx in range(1, len(edges)):
        if edges[idx] <= edges[idx - 1]:
            edges[idx] = edges[idx - 1] + 1e-6
    return edges


def _histogram(values: Sequence[float], edges: Sequence[float]) -> List[int]:
    """Compute histogram counts for the provided edges."""

    counts = [0 for _ in range(len(edges) - 1)]
    for value in values:
        for idx in range(len(edges) - 1):
            start = edges[idx]
            end = edges[idx + 1]
            inclusive = idx == len(edges) - 2
            if (start <= value < end) or (inclusive and start <= value <= end):
                counts[idx] += 1
                break
    return counts


def _population_stability_index(
    expected: Sequence[float], actual: Sequence[float], bins: int = 10
) -> float:
    """Calculate the population stability index between two distributions."""

    if not expected or not actual:
        return 0.0

    edges = _quantile_edges(expected, bins)
    expected_counts = _histogram(expected, edges)
    actual_counts = _histogram(actual, edges)

    expected_total = sum(expected_counts) or 1
    actual_total = sum(actual_counts) or 1

    psi = 0.0
    for expected_count, actual_count in zip(expected_counts, actual_counts):
        expected_ratio = expected_count / expected_total
        actual_ratio = actual_count / actual_total
        if expected_ratio <= 0:
            expected_ratio = 1e-6
        if actual_ratio <= 0:
            actual_ratio = 1e-6
        psi += (actual_ratio - expected_ratio) * math.log(actual_ratio / expected_ratio)
    return float(psi)


def _ks_statistic(values_a: Sequence[float], values_b: Sequence[float]) -> Tuple[float, float]:
    """Return the Kolmogorov-Smirnov statistic and p-value for two samples."""

    n_a = len(values_a)
    n_b = len(values_b)
    if n_a == 0 or n_b == 0:
        return 0.0, 1.0

    a_sorted = sorted(values_a)
    b_sorted = sorted(values_b)

    idx_a = idx_b = 0
    cdf_a = cdf_b = 0.0
    d_stat = 0.0

    while idx_a < n_a and idx_b < n_b:
        if a_sorted[idx_a] <= b_sorted[idx_b]:
            idx_a += 1
            cdf_a = idx_a / n_a
        else:
            idx_b += 1
            cdf_b = idx_b / n_b
        d_stat = max(d_stat, abs(cdf_a - cdf_b))

    # Consume remaining values
    while idx_a < n_a:
        idx_a += 1
        cdf_a = idx_a / n_a
        d_stat = max(d_stat, abs(cdf_a - cdf_b))
    while idx_b < n_b:
        idx_b += 1
        cdf_b = idx_b / n_b
        d_stat = max(d_stat, abs(cdf_a - cdf_b))

    effective_n = math.sqrt(n_a * n_b / (n_a + n_b))
    if effective_n <= 0:
        return d_stat, 1.0

    lam = (effective_n + 0.12 + 0.11 / effective_n) * d_stat
    p_value = 0.0
    for k in range(1, 101):
        term = (-1) ** (k - 1) * math.exp(-2 * (lam**2) * (k**2))
        p_value += term
        if abs(term) < 1e-8:
            break
    p_value = max(min(2 * p_value, 1.0), 0.0)
    return float(d_stat), float(p_value)


# ---------------------------------------------------------------------------
# Data loading and persistence
# ---------------------------------------------------------------------------


def _fetch_latest_features(conn: psycopg2.extensions.connection) -> Dict[str, List[float]]:
    """Load the most recent offline feature snapshot from TimescaleDB."""

    query = """
        WITH latest AS (
            SELECT MAX(event_ts) AS max_ts
            FROM features
        )
        SELECT feature_name, value::double precision
        FROM features
        WHERE event_ts = (SELECT max_ts FROM latest)
    """
    features: Dict[str, List[float]] = defaultdict(list)
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query)
        for row in cursor.fetchall():
            feature_name = row.get("feature_name")
            value = row.get("value")
            if feature_name is None or value is None:
                continue
            features[str(feature_name)].append(float(value))
    return features


def _fetch_reference_distribution(
    conn: psycopg2.extensions.connection,
) -> Dict[str, List[float]]:
    """Load the training reference distribution for each feature."""

    query = "SELECT feature_name, value::double precision FROM feature_reference"
    features: Dict[str, List[float]] = defaultdict(list)
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        try:
            cursor.execute(query)
        except errors.UndefinedTable as exc:  # pragma: no cover - defensive
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Training reference table 'feature_reference' is not available",
            ) from exc

        for row in cursor.fetchall():
            feature_name = row.get("feature_name")
            value = row.get("value")
            if feature_name is None or value is None:
                continue
            features[str(feature_name)].append(float(value))

    if not features:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Training reference distributions are empty",
        )
    return features


def _persist_results(
    conn: psycopg2.extensions.connection,
    checked_at: datetime,
    metrics: Iterable[DriftMetric],
) -> None:
    """Persist the calculated drift metrics to the ``drift_results`` table."""

    insert_stmt = """
        INSERT INTO drift_results (
            checked_at,
            feature_name,
            psi_score,
            ks_statistic,
            ks_pvalue,
            flagged
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cursor:
        for metric in metrics:
            cursor.execute(
                insert_stmt,
                (
                    checked_at,
                    metric.feature_name,
                    metric.psi,
                    metric.ks_statistic,
                    metric.ks_pvalue,
                    metric.flagged,
                ),
            )


def _load_latest_results(
    conn: psycopg2.extensions.connection,
) -> Tuple[datetime | None, Dict[str, DriftMetric]]:
    """Fetch the most recent drift metrics from the persistence layer."""

    query = """
        SELECT checked_at, feature_name, psi_score, ks_statistic, ks_pvalue, flagged
        FROM drift_results
        WHERE checked_at = (
            SELECT MAX(checked_at) FROM drift_results
        )
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    if not rows:
        return None, {}

    checked_at = rows[0]["checked_at"]
    metrics: Dict[str, DriftMetric] = {}
    for row in rows:
        metrics[row["feature_name"]] = DriftMetric(
            feature_name=row["feature_name"],
            psi=float(row["psi_score"]),
            ks_statistic=float(row["ks_statistic"]),
            ks_pvalue=float(row["ks_pvalue"]),
            flagged=bool(row["flagged"]),
        )
    return checked_at, metrics


def _record_alert(
    conn: psycopg2.extensions.connection,
    *,
    triggered_at: datetime,
    action: str,
    feature_name: str | None = None,
    psi_score: float | None = None,
    details: Dict[str, Any] | None = None,
) -> None:
    """Persist a drift alert entry for auditability and alert reporting."""

    insert_stmt = """
        INSERT INTO drift_alerts (
            triggered_at,
            feature_name,
            psi_score,
            action,
            details
        ) VALUES (%s, %s, %s, %s, %s)
    """

    payload: str | None = None
    if details is not None:
        try:
            payload = json.dumps(details)
        except (TypeError, ValueError):  # pragma: no cover - defensive.
            payload = json.dumps({"raw": str(details)})

    with conn.cursor() as cursor:
        cursor.execute(
            insert_stmt,
            (
                triggered_at,
                feature_name,
                psi_score,
                action,
                payload,
            ),
        )


def _fetch_recent_alerts(
    conn: psycopg2.extensions.connection, limit: int = MAX_ALERTS_RETURNED
) -> List[DriftAlertRecord]:
    """Retrieve the most recent drift alerts for the API endpoint."""

    query = """
        SELECT triggered_at, feature_name, psi_score, action, details
        FROM drift_alerts
        ORDER BY triggered_at DESC
        LIMIT %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()

    records: List[DriftAlertRecord] = []
    for row in rows:
        details_payload = row.get("details")
        if details_payload is None:
            parsed_details: Dict[str, Any] | None = None
        elif isinstance(details_payload, dict):
            parsed_details = details_payload
        else:
            try:
                parsed_details = json.loads(str(details_payload))
            except json.JSONDecodeError:
                parsed_details = {"raw": str(details_payload)}
        records.append(
            DriftAlertRecord(
                triggered_at=row["triggered_at"],
                feature_name=row.get("feature_name"),
                psi_score=(
                    float(row["psi_score"])
                    if row.get("psi_score") is not None
                    else None
                ),
                action=str(row["action"]),
                details=parsed_details,
            )
        )
    return records


def _fetch_model_performance(
    conn: psycopg2.extensions.connection,
) -> Dict[str, ModelPerformance]:
    """Load the latest Sharpe and Sortino ratios for each model stage."""

    query = """
        WITH latest AS (
            SELECT model_stage, MAX(evaluated_at) AS evaluated_at
            FROM model_performance
            GROUP BY model_stage
        )
        SELECT p.model_stage, p.evaluated_at, p.sharpe_ratio, p.sortino_ratio
        FROM model_performance AS p
        JOIN latest AS l
            ON l.model_stage = p.model_stage
            AND l.evaluated_at = p.evaluated_at
    """

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    except errors.UndefinedTable:
        return {}

    performance: Dict[str, ModelPerformance] = {}
    for row in rows:
        stage_raw = str(row.get("model_stage", "")).strip().lower()
        if not stage_raw:
            continue
        performance[stage_raw] = ModelPerformance(
            stage=stage_raw,
            evaluated_at=row["evaluated_at"],
            sharpe=float(row.get("sharpe_ratio", 0.0)),
            sortino=float(row.get("sortino_ratio", 0.0)),
        )
    return performance


def _relative_degradation(canary_value: float, prod_value: float) -> float:
    """Return the relative degradation between production and canary metrics."""

    if prod_value == 0:
        return 0.0 if canary_value >= prod_value else float("inf")
    if canary_value >= prod_value:
        return 0.0
    return (prod_value - canary_value) / abs(prod_value)


def _submit_retraining_workflow(
    features: Sequence[str],
    checked_at: datetime,
) -> Dict[str, Any]:
    """Submit an Argo workflow to retrain the model when drift is detected."""

    workflow_template = os.getenv("ARGO_RETRAIN_WORKFLOW", "ml-daily-retrain")
    namespace = os.getenv("ARGO_NAMESPACE", "default")
    argo_base_url = os.getenv("ARGO_BASE_URL")
    job_reference = f"retrain-{int(checked_at.timestamp())}"

    if not argo_base_url or requests is None:
        reason = "requests_not_available" if requests is None else "argo_not_configured"
        LOGGER.info("Skipping retraining submission: %s", reason)
        return {
            "status": "not_submitted",
            "reason": reason,
            "job_reference": job_reference,
            "features": list(features),
        }

    payload = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {"generateName": f"{workflow_template}-"},
        "spec": {
            "workflowTemplateRef": {"name": workflow_template},
            "arguments": {
                "parameters": [
                    {
                        "name": "drift_features",
                        "value": ",".join(sorted(features)) or "none",
                    }
                ]
            },
        },
    }

    submit_url = f"{argo_base_url.rstrip('/')}/api/v1/workflows/{namespace}"
    try:
        response = requests.post(  # type: ignore[union-attr]
            submit_url,
            json=payload,
            timeout=float(os.getenv("ARGO_SUBMIT_TIMEOUT", "10")),
        )
        response.raise_for_status()
        body = response.json()
        job_name = body.get("metadata", {}).get("name", job_reference)
        return {
            "status": "submitted",
            "job_reference": job_name,
            "features": list(features),
        }
    except Exception as exc:  # pragma: no cover - network failure path.
        LOGGER.exception("Failed to submit retraining workflow: %s", exc)
        return {
            "status": "submission_failed",
            "job_reference": job_reference,
            "error": str(exc),
            "features": list(features),
        }


def _rollback_canary_model() -> Dict[str, Any]:
    """Rollback the canary model when it underperforms production."""

    model_name = os.getenv("MODEL_REGISTRY_NAME")
    if not model_name:
        return {"status": "skipped", "reason": "model_registry_not_configured"}

    try:  # pragma: no cover - optional dependency path.
        import mlflow
        from mlflow.tracking import MlflowClient
    except Exception as exc:  # pragma: no cover - optional dependency path.
        LOGGER.warning("MLflow unavailable for rollback: %s", exc)
        return {"status": "skipped", "reason": "mlflow_unavailable"}

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    registry_uri = os.getenv("MLFLOW_REGISTRY_URI", tracking_uri)
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    if registry_uri:
        mlflow.set_registry_uri(registry_uri)

    client = MlflowClient()
    try:
        versions = client.search_model_versions(f"name='{model_name}'")
    except Exception as exc:  # pragma: no cover - network failure path.
        LOGGER.exception("Failed to fetch model versions for rollback: %s", exc)
        return {"status": "failed", "reason": str(exc)}

    production_versions = [
        version
        for version in versions
        if str(version.current_stage).lower() in {"prod", "production"}
    ]
    canary_versions = [
        version for version in versions if str(version.current_stage).lower() == "canary"
    ]

    if not production_versions:
        LOGGER.warning("No production model available to rollback to")
        return {"status": "skipped", "reason": "no_production_model"}

    latest_production = max(production_versions, key=lambda mv: int(mv.version))

    for canary in canary_versions:
        try:
            client.transition_model_version_stage(
                name=model_name,
                version=canary.version,
                stage="Archived",
                archive_existing_versions=False,
            )
        except Exception as exc:  # pragma: no cover - network failure path.
            LOGGER.warning(
                "Failed to archive canary model version %s: %s", canary.version, exc
            )

    try:
        client.transition_model_version_stage(
            name=model_name,
            version=latest_production.version,
            stage="Production",
            archive_existing_versions=False,
        )
    except Exception as exc:  # pragma: no cover - network failure path.
        LOGGER.exception("Failed to promote production model during rollback: %s", exc)
        return {"status": "failed", "reason": str(exc)}

    return {
        "status": "rolled_back",
        "model_name": model_name,
        "production_version": latest_production.version,
    }


def _evaluate_performance_and_mitigate(
    conn: psycopg2.extensions.connection, checked_at: datetime
) -> None:
    """Compare canary and production performance metrics and rollback if needed."""

    performance = _fetch_model_performance(conn)
    if not performance:
        return

    canary = performance.get("canary")
    production = performance.get("prod") or performance.get("production")
    if not canary or not production:
        return

    sharpe_drop = _relative_degradation(canary.sharpe, production.sharpe)
    sortino_drop = _relative_degradation(canary.sortino, production.sortino)

    if sharpe_drop <= ROLLBACK_THRESHOLD and sortino_drop <= ROLLBACK_THRESHOLD:
        return

    rollback_details = _rollback_canary_model()
    alert_details = {
        "stage": "canary",
        "reference_stage": "production",
        "sharpe_canary": canary.sharpe,
        "sharpe_production": production.sharpe,
        "sharpe_drop": sharpe_drop,
        "sortino_canary": canary.sortino,
        "sortino_production": production.sortino,
        "sortino_drop": sortino_drop,
        **rollback_details,
    }
    _record_alert(
        conn,
        triggered_at=checked_at,
        action="rollback_triggered",
        feature_name=None,
        psi_score=None,
        details=alert_details,
    )


# ---------------------------------------------------------------------------
# Drift evaluation
# ---------------------------------------------------------------------------


def _run_drift_detection() -> DriftStatus:
    """Execute the drift detection workflow and persist results."""

    with _connect() as conn:
        reference = _fetch_reference_distribution(conn)
        production = _fetch_latest_features(conn)

        if not production:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No offline features available for drift calculation",
            )

        checked_at = datetime.now(timezone.utc)
        metrics: List[DriftMetric] = []
        features_drift: Dict[str, bool] = {}
        psi_scores: Dict[str, float] = {}
        ks_scores: Dict[str, Dict[str, float]] = {}
        psi_breaches: Dict[str, float] = {}

        for feature_name, baseline_values in reference.items():
            production_values = production.get(feature_name)
            if not production_values:
                continue

            psi_value = _population_stability_index(baseline_values, production_values)
            ks_stat, ks_pvalue = _ks_statistic(baseline_values, production_values)

            flagged = psi_value > PSI_THRESHOLD or ks_pvalue < 0.05

            metrics.append(
                DriftMetric(
                    feature_name=feature_name,
                    psi=psi_value,
                    ks_statistic=ks_stat,
                    ks_pvalue=ks_pvalue,
                    flagged=flagged,
                )
            )
            features_drift[feature_name] = flagged
            psi_scores[feature_name] = psi_value
            ks_scores[feature_name] = {"statistic": ks_stat, "p_value": ks_pvalue}

            if psi_value > PSI_THRESHOLD:
                psi_breaches[feature_name] = psi_value

        if not metrics:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No overlapping features between reference and production",
            )

        _persist_results(conn, checked_at, metrics)

        if psi_breaches:
            submission_details = _submit_retraining_workflow(
                tuple(sorted(psi_breaches)), checked_at
            )
            for feature_name, psi_value in psi_breaches.items():
                _record_alert(
                    conn,
                    triggered_at=checked_at,
                    feature_name=feature_name,
                    psi_score=psi_value,
                    action="retraining_triggered",
                    details={"psi": psi_value, **submission_details},
                )

        _evaluate_performance_and_mitigate(conn, checked_at)

    DRIFT_FEATURES_TOTAL.set(len(metrics))
    DRIFT_FLAGS_TOTAL.set(sum(1 for metric in metrics if metric.flagged))

    return DriftStatus(
        features_drift=features_drift,
        psi_scores=psi_scores,
        ks_scores=ks_scores,
        last_checked=checked_at,
    )


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------


@app.get("/drift/status", response_model=DriftStatus)
def drift_status() -> DriftStatus:
    with _connect() as conn:
        checked_at, metrics = _load_latest_results(conn)

    if not metrics:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No drift results have been recorded yet",
        )

    features_drift = {name: metric.flagged for name, metric in metrics.items()}
    psi_scores = {name: metric.psi for name, metric in metrics.items()}
    ks_scores = {
        name: {"statistic": metric.ks_statistic, "p_value": metric.ks_pvalue}
        for name, metric in metrics.items()
    }

    return DriftStatus(
        features_drift=features_drift,
        psi_scores=psi_scores,
        ks_scores=ks_scores,
        last_checked=checked_at,
    )


@app.post("/drift/run", response_model=DriftStatus)
def drift_run() -> DriftStatus:
    return _run_drift_detection()


@app.get("/drift/alerts", response_model=DriftAlertsResponse)
def drift_alerts() -> DriftAlertsResponse:
    with _connect() as conn:
        records = _fetch_recent_alerts(conn)

    alerts = [
        DriftAlert(
            triggered_at=record.triggered_at,
            feature_name=record.feature_name,
            psi_score=record.psi_score,
            action=record.action,
            details=record.details,
        )
        for record in records
    ]
    return DriftAlertsResponse(alerts=alerts)


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    payload = generate_latest(REGISTRY)
    return PlainTextResponse(payload, media_type=CONTENT_TYPE_LATEST)


__all__ = ["app"]
