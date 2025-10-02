"""Command-line entry point for Kraken statement reconciliation."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Sequence

from services.alert_manager import AlertManager, get_alert_metrics
from services.reports.kraken_reconciliation import (
    KrakenReconciliationService,
    KrakenStatementDownloader,
    TimescaleInternalLedger,
    get_reconciliation_metrics,
)

LOGGER = logging.getLogger(__name__)


def _parse_datetime(value: str | None, *, default: datetime) -> datetime:
    if not value:
        return default
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _default_window() -> tuple[datetime, datetime]:
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = today - timedelta(days=1)
    return start, today


def _parse_accounts(value: Sequence[str] | None) -> list[str]:
    if value:
        return [account.strip() for account in value if account.strip()]
    env_value = os.getenv("RECONCILE_ACCOUNTS", "")
    return [account.strip() for account in env_value.split(",") if account.strip()]


def run(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reconcile Kraken statements against Timescale")
    parser.add_argument("--accounts", nargs="*", help="Accounts to reconcile. Overrides RECONCILE_ACCOUNTS")
    parser.add_argument("--start", help="ISO timestamp for reconciliation window start (inclusive)")
    parser.add_argument("--end", help="ISO timestamp for reconciliation window end (exclusive)")
    parser.add_argument(
        "--base-url",
        default=os.getenv("KRAKEN_STATEMENT_BASE_URL", "https://api.kraken.com"),
        help="Base URL for Kraken statement exports",
    )
    parser.add_argument("--api-key", default=os.getenv("KRAKEN_STATEMENT_KEY"))
    parser.add_argument("--api-secret", default=os.getenv("KRAKEN_STATEMENT_SECRET"))
    parser.add_argument(
        "--tolerance",
        type=float,
        default=float(os.getenv("KRAKEN_RECONCILE_TOLERANCE", "1.0")),
        help="Maximum tolerated absolute difference before alerts fire",
    )
    args = parser.parse_args(argv)

    accounts = _parse_accounts(args.accounts)
    if not accounts:
        raise SystemExit("No accounts configured for reconciliation")

    default_start, default_end = _default_window()
    start = _parse_datetime(args.start, default=default_start)
    end = _parse_datetime(args.end, default=default_end)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    downloader = KrakenStatementDownloader(
        base_url=args.base_url,
        api_key=args.api_key,
        api_secret=args.api_secret,
    )
    ledger = TimescaleInternalLedger()
    metrics = get_reconciliation_metrics()
    alert_manager = AlertManager(metrics=get_alert_metrics(), alertmanager_url=os.getenv("ALERTMANAGER_URL"))
    service = KrakenReconciliationService(
        downloader=downloader,
        ledger=ledger,
        metrics=metrics,
        alert_manager=alert_manager,
        tolerance=args.tolerance,
    )

    try:
        results = service.reconcile_accounts(accounts, start, end)
    finally:
        downloader.close()

    mismatches = [result for result in results if not result.ok]
    for result in results:
        LOGGER.info(
            "Reconciliation completed",
            extra={"account_id": result.account_id, "status": result.as_dict()["status"]},
        )
        print(json.dumps(result.as_dict(), sort_keys=True))

    return 1 if mismatches else 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(run())
