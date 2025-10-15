"""Account reporting service and FastAPI endpoints."""

from __future__ import annotations

import csv
import io
import os

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from statistics import mean
from typing import Any, Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional, Sequence

try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import APIRouter, Depends, HTTPException, Query, Request
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[assignment]
        APIRouter,
        Depends,
        HTTPException,
        Query,
        Request,
    )
try:  # pragma: no cover - prefer the real driver when available
    from psycopg2 import sql
    from psycopg2.extras import RealDictCursor
except Exception:  # pragma: no cover - dependency-light environments
    sql = None  # type: ignore[assignment]

    class RealDictCursor:  # type: ignore[empty-body]
        """Placeholder cursor used when psycopg2 is unavailable."""

        pass

from reports.storage import ArtifactStorage, StoredArtifact, TimescaleSession as StorageSession, build_storage_from_env
from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

from services.common.config import TimescaleSession, get_timescale_session
from services.common.security import ensure_admin_access


DAILY_ACCOUNT_PNL_QUERY = """
SELECT
    account_id::text AS account_id,
    COALESCE(SUM(realized), 0) AS realized_pnl,
    COALESCE(SUM(unrealized), 0) AS unrealized_pnl
FROM pnl
WHERE as_of >= %(start)s AND as_of < %(end)s
{account_filter}
GROUP BY account_id
"""

DAILY_ACCOUNT_FEES_QUERY = """
SELECT
    o.account_id::text AS account_id,
    COALESCE(SUM(f.fee), 0) AS fees
FROM fills AS f
JOIN orders AS o ON o.order_id = f.order_id
WHERE f.fill_time >= %(start)s AND f.fill_time < %(end)s
{account_filter}
GROUP BY o.account_id
"""

DAILY_ACCOUNT_EXPOSURE_QUERY = """
WITH latest_positions AS (
    SELECT DISTINCT ON (account_id, market)
        account_id::text AS account_id,
        quantity::numeric AS quantity,
        COALESCE(entry_price, 0)::numeric AS entry_price
    FROM positions
    WHERE as_of < %(end)s
    {account_filter}
    ORDER BY account_id, market, as_of DESC
)
SELECT
    account_id,
    COALESCE(SUM(ABS(quantity * entry_price)), 0) AS exposure
FROM latest_positions
GROUP BY account_id
"""

RECENT_SHAP_QUERY = """
SELECT
    account_id::text AS account_id,
    feature_name,
    shap_value,
    inference_time,
    model_version,
    metadata
FROM ml_shap_outputs
WHERE inference_time >= %(start)s AND inference_time < %(end)s
{account_filter}
"""


@dataclass(frozen=True)
class DailyAccountSnapshot:
    """Aggregated snapshot for a single account on a given report date."""

    account_id: str
    report_date: date
    realized_pnl: float
    unrealized_pnl: float
    fees: float
    exposures: float


@dataclass(frozen=True)
class FeatureAttribution:
    """Aggregated SHAP attribution for a single feature."""

    feature: str
    mean_shap: float
    mean_abs_shap: float
    sample_count: int


class ReportService:
    """Service façade for generating account reports and explainability payloads."""

    def __init__(
        self,
        *,
        account_id: str,
        timescale: TimescaleSession,
        storage: ArtifactStorage,
    ) -> None:
        self._account_id = account_id
        self._timescale = timescale
        self._storage = storage

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    @contextmanager
    def _session(self) -> Iterator[RealDictCursor]:
        if sql is None:
            raise RuntimeError("psycopg2 is required for report persistence")

        import psycopg2

        conn = psycopg2.connect(self._timescale.dsn)
        try:
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(self._timescale.account_schema)
                    )
                )
                yield cursor
        finally:
            conn.close()

    @staticmethod
    def _account_filter_clause(column: str, account_ids: Sequence[str] | None, params: Dict[str, Any]) -> str:
        if not account_ids:
            return ""
        params["account_ids"] = list(account_ids)
        return f" AND {column}::text = ANY(%(account_ids)s)"

    @staticmethod
    def _fetch(cursor: RealDictCursor, query: str, params: Mapping[str, Any]) -> List[Dict[str, Any]]:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Daily report generation
    # ------------------------------------------------------------------

    def generate_daily_account_reports(
        self,
        *,
        report_date: date,
        account_ids: Sequence[str] | None = None,
    ) -> List[StoredArtifact]:
        """Generate and persist per-account CSV reports for *report_date*."""

        start = datetime.combine(report_date, datetime.min.time(), tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        with self._session() as session:
            pnl_params: Dict[str, Any] = {"start": start, "end": end}
            pnl_clause = self._account_filter_clause("account_id", account_ids, pnl_params)
            pnl_rows = self._fetch(
                session,
                DAILY_ACCOUNT_PNL_QUERY.format(account_filter=pnl_clause),
                pnl_params,
            )

            fee_params: Dict[str, Any] = {"start": start, "end": end}
            fee_clause = self._account_filter_clause("o.account_id", account_ids, fee_params)
            fee_rows = self._fetch(
                session,
                DAILY_ACCOUNT_FEES_QUERY.format(account_filter=fee_clause),
                fee_params,
            )

            exposure_params: Dict[str, Any] = {"end": end}
            exposure_clause = ""
            if account_ids:
                exposure_clause = " AND account_id::text = ANY(%(account_ids)s)"
                exposure_params["account_ids"] = list(account_ids)
            exposure_rows = self._fetch(
                session,
                DAILY_ACCOUNT_EXPOSURE_QUERY.format(account_filter=exposure_clause),
                exposure_params,
            )

            snapshots = self._merge_account_snapshots(
                report_date,
                pnl_rows,
                fee_rows,
                exposure_rows,
                explicit_accounts=account_ids,
            )
            artifacts = self._persist_daily_snapshots(session, snapshots)
        return artifacts

    @staticmethod
    def _merge_account_snapshots(
        report_date: date,
        pnl_rows: Iterable[Mapping[str, Any]],
        fee_rows: Iterable[Mapping[str, Any]],
        exposure_rows: Iterable[Mapping[str, Any]],
        *,
        explicit_accounts: Sequence[str] | None = None,
    ) -> List[DailyAccountSnapshot]:
        aggregates: MutableMapping[str, Dict[str, float]] = defaultdict(
            lambda: {"realized": 0.0, "unrealized": 0.0, "fees": 0.0, "exposure": 0.0}
        )

        for row in pnl_rows:
            account_id = str(row.get("account_id"))
            aggregates[account_id]["realized"] = float(row.get("realized_pnl", 0.0))
            aggregates[account_id]["unrealized"] = float(row.get("unrealized_pnl", 0.0))

        for row in fee_rows:
            account_id = str(row.get("account_id"))
            aggregates[account_id]["fees"] = float(row.get("fees", 0.0))

        for row in exposure_rows:
            account_id = str(row.get("account_id"))
            aggregates[account_id]["exposure"] = float(row.get("exposure", 0.0))

        if explicit_accounts:
            for account_id in explicit_accounts:
                aggregates.setdefault(str(account_id), {"realized": 0.0, "unrealized": 0.0, "fees": 0.0, "exposure": 0.0})

        snapshots: List[DailyAccountSnapshot] = []
        for account_id, values in sorted(aggregates.items()):
            snapshots.append(
                DailyAccountSnapshot(
                    account_id=account_id,
                    report_date=report_date,
                    realized_pnl=float(values.get("realized", 0.0)),
                    unrealized_pnl=float(values.get("unrealized", 0.0)),
                    fees=float(values.get("fees", 0.0)),
                    exposures=float(values.get("exposure", 0.0)),
                )
            )
        return snapshots

    def _persist_daily_snapshots(
        self, session: StorageSession, snapshots: Sequence[DailyAccountSnapshot]
    ) -> List[StoredArtifact]:
        artifacts: List[StoredArtifact] = []
        generated_at = datetime.now(timezone.utc)
        for snapshot in snapshots:
            payload = self._serialize_daily_snapshot(snapshot)
            object_key = self._daily_object_key(snapshot.account_id, snapshot.report_date, generated_at)
            metadata = {
                "report_date": snapshot.report_date.isoformat(),
                "account_id": snapshot.account_id,
                "generated_at": generated_at.isoformat(),
                "type": "daily_account_summary",
            }
            artifact = self._storage.store_artifact(
                session,
                account_id=snapshot.account_id,
                object_key=object_key,
                data=payload,
                content_type="text/csv",
                metadata=metadata,
            )
            artifacts.append(artifact)
        return artifacts

    @staticmethod
    def _serialize_daily_snapshot(snapshot: DailyAccountSnapshot) -> bytes:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["date", "realized_pnl", "unrealized_pnl", "fees", "exposures"])
        writer.writerow(
            [
                snapshot.report_date.isoformat(),
                f"{snapshot.realized_pnl:.10f}",
                f"{snapshot.unrealized_pnl:.10f}",
                f"{snapshot.fees:.10f}",
                f"{snapshot.exposures:.10f}",
            ]
        )
        return buffer.getvalue().encode("utf-8")

    @staticmethod
    def _daily_object_key(account_id: str, report_date: date, generated_at: datetime) -> str:
        safe_account = account_id.replace("/", "-")
        return (
            "daily/account/"
            f"{safe_account}/{report_date.isoformat()}/"
            f"{generated_at:%Y%m%dT%H%M%SZ}.csv"
        )

    # ------------------------------------------------------------------
    # Explainability helpers
    # ------------------------------------------------------------------

    def recent_feature_attribution(
        self,
        *,
        account_id: Optional[str] = None,
        window_days: int = 7,
    ) -> Dict[str, Any]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=window_days)
        with self._session() as session:
            params: Dict[str, Any] = {"start": start, "end": end}
            account_clause = self._account_filter_clause("account_id", [account_id] if account_id else None, params)
            shap_rows = self._fetch(
                session,
                RECENT_SHAP_QUERY.format(account_filter=account_clause),
                params,
            )
        if account_id and not shap_rows:
            raise ValueError(f"No SHAP values found for account '{account_id}' in the last {window_days} days")
        summary = self._summarize_shap(shap_rows)
        return {
            "account_id": account_id or "all",
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "feature_attribution": [feature.__dict__ for feature in summary],
        }

    @staticmethod
    def _summarize_shap(rows: Iterable[Mapping[str, Any]]) -> List[FeatureAttribution]:
        grouped: MutableMapping[str, List[float]] = defaultdict(list)
        for row in rows:
            feature = str(row.get("feature_name"))
            value = float(row.get("shap_value", 0.0))
            grouped[feature].append(value)
        attributions: List[FeatureAttribution] = []
        for feature, values in sorted(grouped.items()):
            attributions.append(
                FeatureAttribution(
                    feature=feature,
                    mean_shap=mean(values) if values else 0.0,
                    mean_abs_shap=mean(abs(v) for v in values) if values else 0.0,
                    sample_count=len(values),
                )
            )
        return attributions


# ---------------------------------------------------------------------------
# FastAPI wiring
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/reports", tags=["reports"])


def _build_service() -> ReportService:
    account_id = os.getenv("AETHER_ACCOUNT_ID", "default")
    timescale = get_timescale_session(account_id)
    storage = build_storage_from_env(os.environ)
    return ReportService(account_id=account_id, timescale=timescale, storage=storage)


_SERVICE: ReportService | None = None


def get_report_service() -> ReportService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = _build_service()
    return _SERVICE


@router.get("/xai")
async def recent_xai(
    request: Request,
    account_id: Optional[str] = Query(default=None),
    service: ReportService = Depends(get_report_service),
) -> Dict[str, Any]:
    """Return aggregated SHAP explanations for recent trades."""

    await ensure_admin_access(request, forbid_on_missing_token=True)
    try:
        payload = service.recent_feature_attribution(account_id=account_id)
    except ValueError as exc:  # No data available for requested account
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return payload


__all__ = ["ReportService", "router", "get_report_service"]
