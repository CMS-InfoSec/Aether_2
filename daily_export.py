"""Daily export utility for audit and regulatory logs."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from typing import Callable

from logging_export import ExportConfig, MissingDependencyError, run_export


DEFAULT_SLEEP_INTERVAL = 5.0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit and reg-log export utility")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Start the export routine")
    run_parser.add_argument(
        "--date",
        type=lambda value: dt.datetime.strptime(value, "%Y-%m-%d").date(),
        help="Explicit date to export (defaults to yesterday in UTC)",
    )
    run_parser.add_argument(
        "--daemon",
        action="store_true",
        help="Continuously wait until midnight UTC and export for the previous day",
    )

    return parser.parse_args()


def _resolve_export_date(explicit: dt.date | None = None) -> dt.date:
    if explicit:
        return explicit
    now = dt.datetime.now(dt.timezone.utc)
    return (now - dt.timedelta(days=1)).date()


def _load_config() -> ExportConfig:
    bucket = os.getenv("EXPORT_BUCKET")
    if not bucket:
        raise RuntimeError("EXPORT_BUCKET environment variable must be set")
    prefix = os.getenv("EXPORT_PREFIX", "log-exports")
    endpoint_url = os.getenv("EXPORT_S3_ENDPOINT_URL")
    return ExportConfig(bucket=bucket, prefix=prefix, endpoint_url=endpoint_url)


def _run_once(export_date: dt.date) -> None:
    config = _load_config()
    result = run_export(for_date=export_date, config=config)
    sys.stdout.write(
        "Exported logs for {date} to s3://{bucket}/{key} (sha256={digest})\n".format(
            date=result.export_date.isoformat(),
            bucket=result.s3_bucket,
            key=result.s3_key,
            digest=result.sha256,
        )
    )


def _seconds_until_midnight_utc(now: dt.datetime | None = None) -> float:
    now = now or dt.datetime.now(dt.timezone.utc)
    tomorrow = (now + dt.timedelta(days=1)).date()
    midnight = dt.datetime.combine(tomorrow, dt.time.min, tzinfo=dt.timezone.utc)
    delta = midnight - now
    return max(delta.total_seconds(), 0.0)


def _daemon_loop(run_once: Callable[[dt.date], None]) -> None:
    while True:
        sleep_for = _seconds_until_midnight_utc()
        time.sleep(sleep_for)
        export_date = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).date()
        try:
            run_once(export_date)
        except Exception as exc:  # pragma: no cover - best effort logging for daemon mode
            sys.stderr.write(f"log export failed for {export_date}: {exc}\n")
        finally:
            time.sleep(DEFAULT_SLEEP_INTERVAL)


def main() -> None:
    args = _parse_args()

    try:
        export_date = _resolve_export_date(args.date if args.command == "run" else None)
        if getattr(args, "daemon", False):
            _daemon_loop(_run_once)
        else:
            _run_once(export_date)
    except MissingDependencyError as exc:
        sys.stderr.write(f"{exc}\n")
        sys.exit(2)
    except Exception as exc:
        sys.stderr.write(f"{exc}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()

