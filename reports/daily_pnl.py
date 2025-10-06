"""Daily profit and loss report generation."""
from __future__ import annotations

import argparse
import csv
import io
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence

from reports.storage import ArtifactStorage, TimescaleSession, build_storage_from_env
from shared.spot import is_spot_symbol, normalize_spot_symbol

LOGGER = logging.getLogger(__name__)

REPORTING_SCALE = Decimal("0.01")
ZERO = Decimal("0")

FILL_QUERY = """
SELECT
    o.account_id,
    f.order_id,
    o.side,
    f.size,
    f.price,
    f.fee,
    f.market AS instrument,
    f.fill_time AS fill_ts
FROM fills AS f
JOIN orders AS o ON o.order_id = f.order_id
{account_join}
WHERE f.fill_time >= %(start)s AND f.fill_time < %(end)s
{account_filter}
"""

ORDER_QUERY = """
SELECT
    o.order_id,
    o.account_id,
    o.market AS instrument,
    o.size,
    o.submitted_at AS submitted_ts
FROM orders AS o
{account_join}
WHERE o.submitted_at >= %(start)s AND o.submitted_at < %(end)s
{account_filter}
"""


def _canonical_spot_instrument(
    candidate: Any,
    *,
    context: str,
    details: Mapping[str, object] | None = None,
) -> str | None:
    """Return a canonical spot instrument when *candidate* is valid."""

    normalized = normalize_spot_symbol(candidate)
    if not normalized:
        return None

    if not is_spot_symbol(normalized):
        extra = dict(details or {})
        extra.setdefault("instrument", normalized)
        LOGGER.warning(
            "Ignoring non-spot instrument '%s' while processing %s",
            candidate,
            context,
            extra=extra or None,
        )
        return None

    return normalized


@dataclass
class DailyPnlRow:
    account_id: str
    instrument: str
    executed_quantity: Decimal
    gross_pnl: Decimal
    fees: Decimal
    net_pnl: Decimal

    def quantized_gross(self) -> Decimal:
        return self.gross_pnl.quantize(REPORTING_SCALE)

    def quantized_net(self) -> Decimal:
        return self.net_pnl.quantize(REPORTING_SCALE)


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


def _prepare_account_filter(
    account_ids: Sequence[str | int],
) -> tuple[str, str, Dict[str, Any]]:
    numeric_ids: List[int] = []
    admin_slugs: List[str] = []
    for raw in account_ids:
        if isinstance(raw, int):
            numeric_ids.append(raw)
        elif isinstance(raw, str):
            candidate = raw.strip()
            if candidate.isdigit():
                numeric_ids.append(int(candidate))
            elif candidate:
                admin_slugs.append(candidate)
        else:
            raise TypeError(f"Unsupported account identifier type: {type(raw)!r}")

    params: Dict[str, Any] = {}
    clauses: List[str] = []
    account_join = ""

    if numeric_ids:
        params["account_ids"] = numeric_ids
        clauses.append("o.account_id = ANY(%(account_ids)s)")

    if admin_slugs:
        params["account_slugs"] = admin_slugs
        account_join = "\nJOIN accounts AS a ON a.account_id = o.account_id"
        clauses.append("a.admin_slug = ANY(%(account_slugs)s)")

    if not clauses:
        return "", "", params

    if len(clauses) == 1:
        predicate = clauses[0]
    else:
        predicate = "(" + " OR ".join(clauses) + ")"

    account_filter = f" AND {predicate}"
    return account_join, account_filter, params


def fetch_daily_fills(
    session: TimescaleSession,
    *,
    start: datetime,
    end: datetime,
    account_ids: Sequence[str | int] | None = None,
) -> List[Dict[str, Any]]:
    account_join = ""
    account_filter = ""
    params: Dict[str, Any] = {"start": start, "end": end}
    if account_ids:
        account_join, account_filter, extra_params = _prepare_account_filter(account_ids)
        params.update(extra_params)
    query = FILL_QUERY.format(account_join=account_join, account_filter=account_filter)
    fills = _fetch_rows(session, query, params)
    LOGGER.debug("Fetched %d fills between %s and %s", len(fills), start, end)
    return fills


def fetch_daily_orders(
    session: TimescaleSession,
    *,
    start: datetime,
    end: datetime,
    account_ids: Sequence[str | int] | None = None,
) -> Dict[str, str]:
    account_join = ""
    account_filter = ""
    params: Dict[str, Any] = {"start": start, "end": end}
    if account_ids:
        account_join, account_filter, extra_params = _prepare_account_filter(account_ids)
        params.update(extra_params)
    query = ORDER_QUERY.format(account_join=account_join, account_filter=account_filter)
    orders = _fetch_rows(session, query, params)
    order_to_instrument: Dict[str, str] = {}
    for order in orders:
        order_id = str(order["order_id"])
        account_id = str(order.get("account_id", ""))
        candidates = (
            order.get("instrument"),
            order.get("symbol"),
            order.get("market"),
        )

        normalized: str | None = None
        for candidate in candidates:
            normalized = _canonical_spot_instrument(
                candidate,
                context="daily PnL order lookup",
                details={"order_id": order_id, "account_id": account_id},
            )
            if normalized:
                break

        if not normalized:
            continue

        order_to_instrument[order_id] = normalized

    LOGGER.debug("Fetched %d orders between %s and %s", len(orders), start, end)
    return order_to_instrument


def _to_decimal(value: Any) -> Decimal:
    """Convert *value* to :class:`Decimal` with string-based coercion."""

    if isinstance(value, Decimal):
        return value
    if value is None:
        return ZERO
    return Decimal(str(value))


def compute_daily_pnl(
    fills: Iterable[Mapping[str, Any]],
    order_instruments: Mapping[str, str],
) -> List[DailyPnlRow]:
    aggregates: MutableMapping[tuple[str, str], Dict[str, Decimal]] = defaultdict(
        lambda: {"executed_quantity": ZERO, "gross_pnl": ZERO, "fees": ZERO}
    )
    for fill in fills:
        account_id = str(fill["account_id"])
        order_id = str(fill["order_id"])

        normalized = _canonical_spot_instrument(
            fill.get("instrument"),
            context="daily PnL fill aggregation",
            details={"order_id": order_id, "account_id": account_id},
        )

        if not normalized and order_id in order_instruments:
            normalized = _canonical_spot_instrument(
                order_instruments[order_id],
                context="daily PnL order reference",
                details={"order_id": order_id, "account_id": account_id},
            )

        if not normalized:
            LOGGER.debug(
                "Skipping fill without a canonical spot instrument",
                extra={"order_id": order_id, "account_id": account_id},
            )
            continue

        instrument = normalized
        side = str(fill.get("side", "")).upper()
        quantity = _to_decimal(fill.get("size"))
        price = _to_decimal(fill.get("price"))
        fee = _to_decimal(fill.get("fee", ZERO))
        notional = quantity * price
        signed_notional = notional if side == "SELL" else -notional
        aggregate = aggregates[(account_id, instrument)]
        aggregate["executed_quantity"] += quantity
        aggregate["gross_pnl"] += signed_notional
        aggregate["fees"] += fee
        LOGGER.debug(
            "Processed fill order_id=%s account=%s instrument=%s notional=%s fee=%s",
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


def _format_decimal(value: Decimal) -> str:
    return format(value, "f")


def _serialize_csv(rows: Sequence[DailyPnlRow]) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["account_id", "instrument", "executed_quantity", "gross_pnl", "fees", "net_pnl"])
    for row in rows:
        gross = row.quantized_gross()
        net = row.quantized_net()
        writer.writerow([
            row.account_id,
            row.instrument,
            _format_decimal(row.executed_quantity),
            _format_decimal(gross),
            _format_decimal(row.fees),
            _format_decimal(net),
        ])
    return output.getvalue().encode()


def _serialize_parquet(rows: Sequence[DailyPnlRow]) -> bytes:
    try:
        import pandas as pd  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("pandas is required for Parquet serialization") from exc

    serialized_rows = [
        {
            "account_id": row.account_id,
            "instrument": row.instrument,
            "executed_quantity": _format_decimal(row.executed_quantity),
            "gross_pnl": _format_decimal(row.quantized_gross()),
            "fees": _format_decimal(row.fees),
            "net_pnl": _format_decimal(row.quantized_net()),
        }
        for row in rows
    ]
    frame = pd.DataFrame(serialized_rows)
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def generate_daily_pnl(
    session: TimescaleSession,
    storage: ArtifactStorage,
    *,
    report_date: date,
    account_ids: Sequence[str | int] | None = None,
    output_formats: Sequence[str] = ("csv", "parquet"),
) -> List[str]:
    """Generate PnL artifacts for *report_date* and return object keys."""

    start = datetime.combine(report_date, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    fills = fetch_daily_fills(session, start=start, end=end, account_ids=account_ids)
    order_instruments = fetch_daily_orders(
        session, start=start, end=end, account_ids=account_ids
    )
    rows = compute_daily_pnl(fills, order_instruments)

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
