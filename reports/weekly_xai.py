"""Weekly SHAP summary report generation."""
from __future__ import annotations

import argparse
import csv
import io
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence

from reports.storage import ArtifactStorage, TimescaleSession, build_storage_from_env

LOGGER = logging.getLogger(__name__)

SHAP_QUERY = """
SELECT account_id, feature_name, shap_value, inference_time, model_version, metadata
FROM ml_shap_outputs
WHERE inference_time >= %(start)s AND inference_time < %(end)s
{account_filter}
"""

EXPECTED_SHAP_COLUMNS = {
    "account_id",
    "feature_name",
    "shap_value",
    "inference_time",
    "model_version",
    "metadata",
}

@dataclass
class WeeklyFeatureAttribution:
    account_id: str
    feature_name: str
    mean_shap: float
    mean_abs_shap: float
    sample_count: int


class ResultSetAdapter:
    def __init__(self, result: Any):
        self._result = result

    def as_dicts(self) -> List[Dict[str, Any]]:
        if hasattr(self._result, "mappings"):
            return list(self._result.mappings())
        return [dict(row) for row in self._result]


def _fetch(session: TimescaleSession, query: str, params: Mapping[str, Any]) -> List[Dict[str, Any]]:
    result = session.execute(query, params)
    return ResultSetAdapter(result).as_dicts()


def _validate_shap_rows(rows: Iterable[Mapping[str, Any]]) -> None:
    for row in rows:
        missing = EXPECTED_SHAP_COLUMNS.difference(row.keys())
        if missing:
            missing_columns = ", ".join(sorted(missing))
            raise ValueError(
                f"ml_shap_outputs row missing expected columns: {missing_columns}"
            )
        # Only need to validate one row since the query schema is static.
        break


def fetch_shap_values(
    session: TimescaleSession,
    *,
    start: datetime,
    end: datetime,
    account_ids: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
    account_filter = ""
    params: Dict[str, Any] = {"start": start, "end": end}
    if account_ids:
        account_filter = " AND account_id = ANY(%(account_ids)s)"
        params["account_ids"] = list(account_ids)
    query = SHAP_QUERY.format(account_filter=account_filter)
    rows = _fetch(session, query, params)
    if rows:
        _validate_shap_rows(rows)
    return rows


def summarize_weekly_shap(rows: Iterable[Mapping[str, Any]]) -> List[WeeklyFeatureAttribution]:
    aggregates: MutableMapping[tuple[str, str], List[float]] = defaultdict(list)
    for row in rows:
        key = (str(row["account_id"]), str(row["feature_name"]))
        aggregates[key].append(float(row["shap_value"]))
    summary: List[WeeklyFeatureAttribution] = []
    for (account_id, feature_name), values in sorted(aggregates.items()):
        summary.append(
            WeeklyFeatureAttribution(
                account_id=account_id,
                feature_name=feature_name,
                mean_shap=mean(values),
                mean_abs_shap=mean(abs(v) for v in values),
                sample_count=len(values),
            )
        )
    LOGGER.info("Computed %d feature attribution rows", len(summary))
    return summary


def _serialize_csv(rows: Sequence[WeeklyFeatureAttribution]) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "account_id",
        "feature_name",
        "mean_shap",
        "mean_abs_shap",
        "sample_count",
    ])
    for row in rows:
        writer.writerow(
            [
                row.account_id,
                row.feature_name,
                f"{row.mean_shap:.10f}",
                f"{row.mean_abs_shap:.10f}",
                row.sample_count,
            ]
        )
    return output.getvalue().encode()


def _serialize_parquet(rows: Sequence[WeeklyFeatureAttribution]) -> bytes:
    try:
        import pandas as pd  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("pandas is required for Parquet serialization") from exc

    frame = pd.DataFrame([row.__dict__ for row in rows])
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def generate_weekly_xai(
    session: TimescaleSession,
    storage: ArtifactStorage,
    *,
    week_ending: date,
    account_ids: Sequence[str] | None = None,
    output_formats: Sequence[str] = ("csv", "parquet"),
) -> List[str]:
    end = datetime.combine(week_ending + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    start = end - timedelta(days=7)
    shap_rows = fetch_shap_values(session, start=start, end=end, account_ids=account_ids)
    summary = summarize_weekly_shap(shap_rows)
    metadata = {
        "week_start": (end - timedelta(days=7)).date().isoformat(),
        "week_end": (end - timedelta(days=1)).date().isoformat(),
        "row_count": len(summary),
    }
    object_keys: List[str] = []
    for fmt in output_formats:
        fmt_lower = fmt.lower()
        if fmt_lower == "csv":
            data = _serialize_csv(summary)
            content_type = "text/csv"
            extension = "csv"
        elif fmt_lower == "parquet":
            data = _serialize_parquet(summary)
            content_type = "application/vnd.apache.parquet"
            extension = "parquet"
        else:
            raise ValueError(f"Unsupported format: {fmt}")
        object_key = f"weekly_xai/{week_ending.isoformat()}.{extension}"
        storage.store_artifact(
            session,
            account_id="global" if not account_ids else "multi-account",
            object_key=object_key,
            data=data,
            content_type=content_type,
            metadata={**metadata, "format": extension},
        )
        object_keys.append(object_key)
    return object_keys


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the weekly SHAP summary report")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--week-ending", default=date.today().isoformat())
    parser.add_argument("--output-formats", default="csv,parquet")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:  # pragma: no cover
    args = _parse_args(argv)
    try:
        import psycopg
    except ImportError:  # pragma: no cover
        raise SystemExit("psycopg is required to run the weekly XAI generator")

    conn = psycopg.connect(args.database_url)
    storage = build_storage_from_env(dict())
    with conn:
        with conn.cursor() as cursor:
            week_ending = date.fromisoformat(args.week_ending)
            formats = [fmt.strip() for fmt in args.output_formats.split(",") if fmt.strip()]
            generate_weekly_xai(cursor, storage, week_ending=week_ending, output_formats=formats)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
