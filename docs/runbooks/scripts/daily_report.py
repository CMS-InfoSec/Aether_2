#!/usr/bin/env python3
"""Generate a daily risk operations report from Prometheus, Loki, and TimescaleDB."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
from pathlib import Path
from typing import List

import requests

PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
LOKI_URL = os.environ.get("LOKI_URL", "http://loki:3100")
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "."))

RISK_METRICS = {
    "p99_latency": 'histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{service="risk-api"}[5m])) by (le))',
    "market_data_gap": 'time() - max(risk_marketdata_latest_timestamp_seconds)'
}


def fetch_prometheus_metric(metric: str, timestamp: dt.datetime) -> float:
    params = {"query": metric, "time": int(timestamp.timestamp())}
    response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params=params, timeout=15)
    response.raise_for_status()
    result = response.json()["data"]["result"]
    if not result:
        return float("nan")
    return float(result[0]["value"][1])


def fetch_loki_ingest_errors(start: dt.datetime, end: dt.datetime) -> int:
    params = {
        "query": '{app="marketdata-ingestor"} |= "ERROR"',
        "direction": "forward",
        "limit": 5000,
        "start": int(start.timestamp() * 1e9),
        "end": int(end.timestamp() * 1e9),
    }
    response = requests.get(f"{LOKI_URL}/loki/api/v1/query_range", params=params, timeout=30)
    response.raise_for_status()
    streams = response.json().get("data", {}).get("result", [])
    return sum(len(stream.get("values", [])) for stream in streams)


def write_csv(date: dt.date, rows: List[List[str]]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"risk-report-{date.isoformat()}.csv"
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value", "timestamp"])
        writer.writerows(rows)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(), default=dt.date.today())
    args = parser.parse_args()

    report_time = dt.datetime.combine(args.date, dt.time(hour=6, tzinfo=dt.timezone.utc))
    rows: List[List[str]] = []

    for name, query in RISK_METRICS.items():
        value = fetch_prometheus_metric(query, report_time)
        rows.append([name, f"{value:.4f}", report_time.isoformat()])

    errors = fetch_loki_ingest_errors(report_time - dt.timedelta(hours=24), report_time)
    rows.append(["ingest_errors", str(errors), report_time.isoformat()])

    csv_path = write_csv(args.date, rows)
    print(f"Report written to {csv_path}")


if __name__ == "__main__":
    main()
