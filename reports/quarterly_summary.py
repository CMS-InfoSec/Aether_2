"""Quarterly trading and governance summary report."""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence

from reports.storage import ArtifactStorage, TimescaleSession, build_storage_from_env

LOGGER = logging.getLogger(__name__)

DECIMAL_ZERO = Decimal("0")
DECIMAL_QUANTIZE_UNIT = Decimal("0.0000000001")

ORDER_QUERY = """
SELECT
    account_id,
    symbol AS instrument,
    COALESCE(SUM(size), 0) AS submitted_qty,
    COUNT(*) AS order_count
FROM orders
WHERE submitted_ts >= %(start)s AND submitted_ts < %(end)s
GROUP BY account_id, symbol
"""

FILL_QUERY = """
SELECT
    o.account_id,
    f.symbol AS instrument,
    COALESCE(SUM(f.size * f.price), 0) AS notional,
    COUNT(*) AS fill_count
FROM fills AS f
JOIN orders AS o ON o.order_id = f.order_id
WHERE f.fill_ts >= %(start)s AND f.fill_ts < %(end)s
GROUP BY o.account_id, f.symbol
"""

AUDIT_QUERY = """

SELECT actor AS account_id, created_at

FROM audit_logs
WHERE created_at >= %(start)s AND created_at < %(end)s
"""


@dataclass
class QuarterlyAccountSummary:
    account_id: str
    instrument: str
    order_count: int
    fill_count: int
    submitted_qty: Decimal
    notional: Decimal
    audit_events: int


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None:
        return DECIMAL_ZERO
    if isinstance(value, (int,)):
        return Decimal(value)
    return Decimal(str(value))


def _format_decimal(value: Decimal) -> str:
    return str(value.quantize(DECIMAL_QUANTIZE_UNIT, rounding=ROUND_HALF_UP))


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


def merge_quarterly_metrics(
    orders: Iterable[Mapping[str, Any]],
    fills: Iterable[Mapping[str, Any]],
    audits: Iterable[Mapping[str, Any]],
) -> List[QuarterlyAccountSummary]:
    aggregates: MutableMapping[tuple[str, str], Dict[str, Any]] = defaultdict(
        lambda: {
            "order_count": 0,
            "fill_count": 0,
            "submitted_qty": DECIMAL_ZERO,
            "notional": DECIMAL_ZERO,
            "audit_events": 0,
        }
    )

    for order in orders:
        key = (str(order["account_id"]), str(order["instrument"]))
        bucket = aggregates[key]
        bucket["order_count"] += int(order.get("order_count", 0))
        bucket["submitted_qty"] += _to_decimal(order.get("submitted_qty", 0))

    for fill in fills:
        key = (str(fill["account_id"]), str(fill["instrument"]))
        bucket = aggregates[key]
        bucket["fill_count"] += int(fill.get("fill_count", 0))
        bucket["notional"] += _to_decimal(fill.get("notional", 0))

    audit_counts: Dict[str, int] = defaultdict(int)
    for audit in audits:
        payload = audit.get("payload")
        if isinstance(payload, str) and payload:
            try:
                audit["payload"] = json.loads(payload)
            except json.JSONDecodeError:
                LOGGER.debug("Failed to decode audit payload", exc_info=True)
        elif payload is None:
            audit.setdefault("payload", {})
        account_id = audit.get("account_id") or audit.get("actor")
        if account_id is None:
            continue
        audit_counts[str(account_id)] += 1

    summaries: List[QuarterlyAccountSummary] = []
    for (account_id, instrument), bucket in sorted(aggregates.items()):
        summaries.append(
            QuarterlyAccountSummary(
                account_id=account_id,
                instrument=instrument,
                order_count=bucket["order_count"],
                fill_count=bucket["fill_count"],
                submitted_qty=bucket["submitted_qty"],
                notional=bucket["notional"],
                audit_events=audit_counts.get(account_id, 0),
            )
        )
    LOGGER.info("Computed %d quarterly summary rows", len(summaries))
    return summaries


def _serialize_csv(rows: Sequence[QuarterlyAccountSummary]) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "account_id",
            "instrument",
            "order_count",
            "fill_count",
            "submitted_qty",
            "notional",
            "audit_events",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.account_id,
                row.instrument,
                row.order_count,
                row.fill_count,
                _format_decimal(row.submitted_qty),
                _format_decimal(row.notional),
                row.audit_events,
            ]
        )
    return output.getvalue().encode()


def _serialize_parquet(rows: Sequence[QuarterlyAccountSummary]) -> bytes:
    try:
        import pandas as pd  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("pandas is required for Parquet serialization") from exc

    frame = pd.DataFrame(
        [
            {
                "account_id": row.account_id,
                "instrument": row.instrument,
                "order_count": row.order_count,
                "fill_count": row.fill_count,
                "submitted_qty": _format_decimal(row.submitted_qty),
                "notional": _format_decimal(row.notional),
                "audit_events": row.audit_events,
            }
            for row in rows
        ]
    )
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def generate_quarterly_summary(
    session: TimescaleSession,
    storage: ArtifactStorage,
    *,
    quarter_ending: date,
    output_formats: Sequence[str] = ("csv", "parquet"),
) -> List[str]:
    end = datetime.combine(quarter_ending + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    start = end - timedelta(days=90)
    orders = _fetch(session, ORDER_QUERY, {"start": start, "end": end})
    fills = _fetch(session, FILL_QUERY, {"start": start, "end": end})
    audits = _fetch(session, AUDIT_QUERY, {"start": start, "end": end})
    summary = merge_quarterly_metrics(orders, fills, audits)
    metadata = {
        "quarter_start": (end - timedelta(days=90)).date().isoformat(),
        "quarter_end": quarter_ending.isoformat(),
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
        object_key = f"quarterly_summary/{quarter_ending.isoformat()}.{extension}"
        storage.store_artifact(
            session,
            account_id="global",
            object_key=object_key,
            data=data,
            content_type=content_type,
            metadata={**metadata, "format": extension},
        )
        object_keys.append(object_key)
    return object_keys


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the quarterly trading summary report")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--quarter-ending", default=date.today().isoformat())
    parser.add_argument("--output-formats", default="csv,parquet")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:  # pragma: no cover
    args = _parse_args(argv)
    try:
        import psycopg
    except ImportError:  # pragma: no cover
        raise SystemExit("psycopg is required to run the quarterly summary generator")

    conn = psycopg.connect(args.database_url)
    storage = build_storage_from_env(dict())
    with conn:
        with conn.cursor() as cursor:
            quarter_ending = date.fromisoformat(args.quarter_ending)
            formats = [fmt.strip() for fmt in args.output_formats.split(",") if fmt.strip()]
            generate_quarterly_summary(cursor, storage, quarter_ending=quarter_ending, output_formats=formats)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
