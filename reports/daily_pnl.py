"""Daily profit and loss report generation."""
from __future__ import annotations

import argparse
import csv
import io
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence

from reports.storage import ArtifactStorage, TimescaleSession, build_storage_from_env

LOGGER = logging.getLogger(__name__)

FILL_QUERY = """
SELECT
    o.account_id,
    f.order_id,
    o.side,
    f.size,
    f.price,
    f.fee,
    f.symbol AS instrument
FROM fills AS f
JOIN orders AS o ON o.order_id = f.order_id
WHERE f.fill_time >= %(start)s AND f.fill_time < %(end)s
{account_filter}
"""

ORDER_QUERY = """
SELECT
    o.order_id,
    o.account_id,
    o.symbol AS instrument,
    o.size,
    o.submitted_at
FROM orders AS o
WHERE o.submitted_at >= %(start)s AND o.submitted_at < %(end)s
{account_filter}
"""


@dataclass
class DailyPnlRow:
    account_id: str
    instrument: str
    executed_quantity: float
    gross_pnl: float
    fees: float
    net_pnl: float


class ResultSetAdapter:
    """Normalize DB-API results to dictionaries."""

    def __init__(self, result: Any):
        self._result = result

    def as_dicts(self) -> List[Dict[str, Any]]:
        if hasattr(self._result, "mappings"):
            return list(self._result.mappings())
        return [dict(row) for row in self._result]


def _fetch_rows(session: TimescaleSession, query: str, params: Mapping[str, Any]) -> List[Dict[str, Any]]:
    result = session.execute(query, params)
    return ResultSetAdapter(result).as_dicts()


def fetch_daily_fills(
    session: TimescaleSession,
    *,
    start: datetime,
    end: datetime,
    account_ids: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
    account_filter = ""
    params: Dict[str, Any] = {"start": start, "end": end}
    if account_ids:
        account_filter = " AND o.account_id = ANY(%(account_ids)s)"
        params["account_ids"] = list(account_ids)
    query = FILL_QUERY.format(account_filter=account_filter)
    fills = _fetch_rows(session, query, params)
    LOGGER.debug("Fetched %d fills between %s and %s", len(fills), start, end)
    return fills


def fetch_daily_orders(
    session: TimescaleSession,
    *,
    start: datetime,
    end: datetime,
    account_ids: Sequence[str] | None = None,
) -> Dict[str, str]:
    account_filter = ""
    params: Dict[str, Any] = {"start": start, "end": end}
    if account_ids:
        account_filter = " AND o.account_id = ANY(%(account_ids)s)"
        params["account_ids"] = list(account_ids)
    query = ORDER_QUERY.format(account_filter=account_filter)
    orders = _fetch_rows(session, query, params)
    order_to_market: Dict[str, str] = {}
    for order in orders:
        order_id = str(order["order_id"])
        market = order.get("instrument") or order.get("symbol")
        order_to_market[order_id] = str(market) if market is not None else "UNKNOWN"
    LOGGER.debug("Fetched %d orders between %s and %s", len(orders), start, end)
    return order_to_market


def compute_daily_pnl(
    fills: Iterable[Mapping[str, Any]],
    order_markets: Mapping[str, str],
) -> List[DailyPnlRow]:
    aggregates: MutableMapping[tuple[str, str], Dict[str, float]] = defaultdict(
        lambda: {"executed_quantity": 0.0, "gross_pnl": 0.0, "fees": 0.0}
    )
    for fill in fills:
        account_id = str(fill["account_id"])
        order_id = str(fill["order_id"])
        instrument = str(
            fill.get("instrument")
            or fill.get("symbol")
            or order_markets.get(order_id, "UNKNOWN")
        )
        side = str(fill["side"]).upper()
        quantity = float(fill["size"])
        price = float(fill["price"])
        fee = float(fill.get("fee", 0.0))
        notional = quantity * price
        signed_notional = notional if side == "SELL" else -notional
        aggregate = aggregates[(account_id, instrument)]
        aggregate["executed_quantity"] += quantity
        aggregate["gross_pnl"] += signed_notional
        aggregate["fees"] += fee
        LOGGER.debug(
            "Processed fill order_id=%s account=%s instrument=%s notional=%f fee=%f",
            order_id,
            account_id,
            instrument,
            signed_notional,
            fee,
        )

    rows = [
        DailyPnlRow(
            account_id=account_id,
            instrument=instrument,
            executed_quantity=agg["executed_quantity"],
            gross_pnl=agg["gross_pnl"],
            fees=agg["fees"],
            net_pnl=agg["gross_pnl"] - agg["fees"],
        )
        for (account_id, instrument), agg in sorted(aggregates.items())
    ]
    LOGGER.info("Computed %d aggregated PnL rows", len(rows))
    return rows


def _serialize_csv(rows: Sequence[DailyPnlRow]) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["account_id", "instrument", "executed_quantity", "gross_pnl", "fees", "net_pnl"])
    for row in rows:
        writer.writerow([
            row.account_id,
            row.instrument,
            f"{row.executed_quantity:.10f}",
            f"{row.gross_pnl:.10f}",
            f"{row.fees:.10f}",
            f"{row.net_pnl:.10f}",
        ])
    return output.getvalue().encode()


def _serialize_parquet(rows: Sequence[DailyPnlRow]) -> bytes:
    try:
        import pandas as pd  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("pandas is required for Parquet serialization") from exc

    frame = pd.DataFrame([row.__dict__ for row in rows])
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def generate_daily_pnl(
    session: TimescaleSession,
    storage: ArtifactStorage,
    *,
    report_date: date,
    account_ids: Sequence[str] | None = None,
    output_formats: Sequence[str] = ("csv", "parquet"),
) -> List[str]:
    """Generate PnL artifacts for *report_date* and return object keys."""

    start = datetime.combine(report_date, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    fills = fetch_daily_fills(session, start=start, end=end, account_ids=account_ids)
    order_markets = fetch_daily_orders(
        session, start=start, end=end, account_ids=account_ids
    )
    rows = compute_daily_pnl(fills, order_markets)

    metadata = {
        "report_date": report_date.isoformat(),
        "row_count": len(rows),
        "accounts": sorted({row.account_id for row in rows}),
    }

    object_keys: List[str] = []
    for fmt in output_formats:
        fmt_lower = fmt.lower()
        if fmt_lower == "csv":
            data = _serialize_csv(rows)
            content_type = "text/csv"
            extension = "csv"
        elif fmt_lower == "parquet":
            data = _serialize_parquet(rows)
            content_type = "application/vnd.apache.parquet"
            extension = "parquet"
        else:
            raise ValueError(f"Unsupported output format: {fmt}")
        object_key = f"daily_pnl/{report_date.isoformat()}.{extension}"
        storage.store_artifact(
            session,
            account_id="global" if not account_ids else "multi-account",
            object_key=object_key,
            data=data,
            content_type=content_type,
            metadata={**metadata, "format": extension},
        )
        object_keys.append(object_key)
        LOGGER.debug("Persisted %s artifact for %s", extension, report_date)
    return object_keys


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the daily PnL report")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--report-date", default=date.today().isoformat())
    parser.add_argument("--output-formats", default="csv,parquet")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:  # pragma: no cover - CLI wiring
    args = _parse_args(argv)
    try:
        import psycopg
    except ImportError:  # pragma: no cover - optional dependency
        raise SystemExit("psycopg is required to run the daily PnL generator")

    conn = psycopg.connect(args.database_url)
    storage = build_storage_from_env(dict())
    with conn:
        with conn.cursor() as cursor:
            report_date = date.fromisoformat(args.report_date)
            formats = [fmt.strip() for fmt in args.output_formats.split(",") if fmt.strip()]
            generate_daily_pnl(cursor, storage, report_date=report_date, output_formats=formats)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
