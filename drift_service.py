"""FastAPI microservice for monitoring feature drift against training baselines."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import math
import os
from typing import Dict, Iterable, List, Sequence, Tuple

import psycopg2
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from psycopg2 import errors, sql
from psycopg2.extras import RealDictCursor
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, generate_latest

from services.common.config import get_timescale_session


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


@dataclass(frozen=True)
class DriftMetric:
    """Container for the per-feature drift metrics persisted to the database."""

    feature_name: str
    psi: float
    ks_statistic: float
    ks_pvalue: float
    flagged: bool


# ---------------------------------------------------------------------------
# Configuration and application setup
# ---------------------------------------------------------------------------


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
    """Create the ``drift_results`` table when the service starts."""

    create_stmt = """
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
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_stmt)


@app.on_event("startup")
def _on_startup() -> None:
    _ensure_tables()


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

        for feature_name, baseline_values in reference.items():
            production_values = production.get(feature_name)
            if not production_values:
                continue

            psi_value = _population_stability_index(baseline_values, production_values)
            ks_stat, ks_pvalue = _ks_statistic(baseline_values, production_values)

            flagged = psi_value > 0.2 or ks_pvalue < 0.05

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

        if not metrics:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No overlapping features between reference and production",
            )

        _persist_results(conn, checked_at, metrics)

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


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    payload = generate_latest(REGISTRY)
    return PlainTextResponse(payload, media_type=CONTENT_TYPE_LATEST)


__all__ = ["app"]

