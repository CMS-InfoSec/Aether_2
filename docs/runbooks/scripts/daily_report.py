#!/usr/bin/env python3
"""Generate a daily risk operations report from Prometheus, Loki, and TimescaleDB."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def _resolve_output_dir(raw_path: str) -> Path:
    """Resolve the output directory while guarding against unsafe inputs.

    The helper rejects traversal tokens (".."), control characters, and any
    symlinked ancestor to ensure report generation cannot escape the intended
    filesystem tree. Returned paths are absolute so downstream callers can rely
    on consistent behaviour regardless of the current working directory.
    """

    if any(ord(char) < 32 for char in raw_path):
        raise ValueError("OUTPUT_DIR contains control characters")

    candidate = Path(raw_path).expanduser()

    if any(part == ".." for part in candidate.parts):
        raise ValueError("OUTPUT_DIR must not contain parent directory traversal")

    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate

    # Convert to an absolute path without collapsing symlinks for validation.
    candidate = candidate.absolute()

    for ancestor in (candidate, *candidate.parents):
        if ancestor.exists() and ancestor.is_symlink():
            raise ValueError("OUTPUT_DIR must not include symlinked paths")

    return candidate


PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
LOKI_URL = os.environ.get("LOKI_URL", "http://loki:3100")
OUTPUT_DIR = _resolve_output_dir(os.environ.get("OUTPUT_DIR", "."))

RISK_METRICS = {
    "p99_latency": 'histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{service="risk-api"}[5m])) by (le))',
    "market_data_gap": 'time() - max(risk_marketdata_latest_timestamp_seconds)'
}


def _http_get_json(url: str, *, params: Mapping[str, Any], timeout: int) -> MutableMapping[str, Any]:
    """Perform a JSON HTTP GET request without relying on third-party clients."""

    query = urlencode([(key, value) for key, value in params.items()])
    request = Request(f"{url}?{query}")
    request.add_header("Accept", "application/json")

    with urlopen(request, timeout=timeout) as response:  # nosec: trusted internal endpoints
        content_type = response.headers.get("Content-Type", "").split(";")[0].strip()
        payload: MutableMapping[str, Any] = {}
        if content_type != "application/json":
            raise RuntimeError(f"Unexpected content type: {content_type or 'unknown'}")
        body = response.read().decode("utf-8")
        data: Any = json.loads(body)
        if isinstance(data, MutableMapping):
            payload = data
        elif isinstance(data, Mapping):
            payload = dict(data)
        else:
            raise RuntimeError("Response payload must be a JSON object")
    return payload


def fetch_prometheus_metric(metric: str, timestamp: dt.datetime) -> float:
    params = {"query": metric, "time": int(timestamp.timestamp())}
    payload = _http_get_json(
        f"{PROMETHEUS_URL}/api/v1/query",
        params=params,
        timeout=15,
    )
    data = payload.get("data", {}) if isinstance(payload, Mapping) else {}
    result = data.get("result", []) if isinstance(data, Mapping) else []
    if not result:
        return float("nan")
    first_entry = result[0]
    if not isinstance(first_entry, Mapping):
        raise RuntimeError("Prometheus response contained malformed series data")
    value = first_entry.get("value")
    if not isinstance(value, list) or len(value) != 2:
        raise RuntimeError("Prometheus value payload must be a [timestamp, value] pair")
    return float(value[1])


def fetch_loki_ingest_errors(start: dt.datetime, end: dt.datetime) -> int:
    params = {
        "query": '{app="marketdata-ingestor"} |= "ERROR"',
        "direction": "forward",
        "limit": 5000,
        "start": int(start.timestamp() * 1e9),
        "end": int(end.timestamp() * 1e9),
    }
    payload = _http_get_json(
        f"{LOKI_URL}/loki/api/v1/query_range",
        params=params,
        timeout=30,
    )
    data = payload.get("data", {}) if isinstance(payload, Mapping) else {}
    streams = data.get("result", []) if isinstance(data, Mapping) else []
    total = 0
    for stream in streams:
        if isinstance(stream, Mapping):
            values = stream.get("values", [])
            if isinstance(values, list):
                total += sum(1 for _ in values)
    return total


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
