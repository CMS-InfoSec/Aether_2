#!/usr/bin/env python3
"""Refresh Timescale continuous aggregates for account-scoped materialised views."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import logging
from typing import Iterable, Sequence

from services.common.config import get_timescale_session

logger = logging.getLogger(__name__)


def _normalize_identifier(schema: str, name: str) -> str:
    if "." in name:
        return name
    return f"{schema}.{name}"


def _refresh_with_psycopg(dsn: str, qualified_names: Sequence[str], *, start: datetime, end: datetime) -> bool:
    try:
        import psycopg
    except Exception:  # pragma: no cover - optional dependency
        logger.debug("psycopg not available; skipping direct SQL refresh")
        return False

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                for name in qualified_names:
                    logger.info("Refreshing continuous aggregate %%s (%%s → %%s)", name, start, end)
                    cur.execute(
                        "CALL refresh_continuous_aggregate(%s, %s, %s);",
                        (name, start, end),
                    )
            conn.commit()
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to refresh aggregates using psycopg")
        return False
    return True


def refresh_continuous_aggregates(
    account_id: str,
    aggregates: Iterable[str],
    *,
    lookback_minutes: int = 60,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
) -> None:
    session = get_timescale_session(account_id)
    if window_end is None:
        window_end = datetime.now(timezone.utc)
    if window_start is None:
        window_start = window_end - timedelta(minutes=lookback_minutes)

    qualified = [_normalize_identifier(session.account_schema, name) for name in aggregates]
    logger.debug(
        "Preparing to refresh %%d aggregates for account %%s", len(qualified), account_id
    )

    if _refresh_with_psycopg(session.dsn, qualified, start=window_start, end=window_end):
        return

    # Fallback: emit advisory log entries so operational tooling can pick up manual refreshes.
    for name in qualified:
        logger.warning(
            "Unable to refresh continuous aggregate %s automatically; run the Timescale job manually",
            name,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("account_id", help="Account identifier to resolve the Timescale schema")
    parser.add_argument(
        "aggregates",
        nargs="+",
        help="Continuous aggregate names to refresh (schema prefixes optional)",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=60,
        help="Window size in minutes when start/end timestamps are not provided",
    )
    parser.add_argument(
        "--start",
        type=str,
        help="ISO8601 timestamp for the refresh window start",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="ISO8601 timestamp for the refresh window end",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    start = datetime.fromisoformat(args.start) if args.start else None
    end = datetime.fromisoformat(args.end) if args.end else None

    refresh_continuous_aggregates(
        args.account_id,
        args.aggregates,
        lookback_minutes=args.lookback_minutes,
        window_start=start,
        window_end=end,
    )


if __name__ == "__main__":
    main()
