"""Latency metrics collection and exposition helpers.

This module wires Prometheus histograms for the critical stages of the
trade lifecycle and maintains rolling windows that allow us to compute
p95 latency based alerts.  The metrics are exported through a lightweight
FastAPI application that serves the ``/metrics`` endpoint which can be
scraped by a Prometheus server.
"""

from __future__ import annotations

from bisect import bisect_left, insort
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, MutableMapping, Optional, Tuple

from fastapi import FastAPI, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Gauge,
    Histogram,
    generate_latest,
)

__all__ = ["LatencySnapshot", "LatencyMetrics", "create_metrics_app"]


@dataclass(frozen=True)
class LatencySnapshot:
    """Latency percentiles for a metric."""

    p50: float
    p95: float
    p99: float


class _PercentileWindow:
    """Maintain a rolling, order-aware collection of samples."""

    def __init__(self, maxlen: int = 1024) -> None:
        if maxlen <= 0:
            raise ValueError("maxlen must be a positive integer")
        self._maxlen = maxlen
        self._samples: Deque[float] = deque(maxlen=maxlen)
        self._sorted: list[float] = []

    def add(self, value: float) -> None:
        if len(self._samples) == self._maxlen:
            expired = self._samples.popleft()
            idx = bisect_left(self._sorted, expired)
            if 0 <= idx < len(self._sorted):
                del self._sorted[idx]
        self._samples.append(value)
        insort(self._sorted, value)

    def snapshot(self) -> LatencySnapshot:
        if not self._sorted:
            return LatencySnapshot(0.0, 0.0, 0.0)
        return LatencySnapshot(
            p50=self._quantile(0.50),
            p95=self._quantile(0.95),
            p99=self._quantile(0.99),
        )

    def _quantile(self, q: float) -> float:
        if not 0 <= q <= 1:
            raise ValueError("q must be in the [0, 1] range")
        if not self._sorted:
            return 0.0
        position = q * (len(self._sorted) - 1)
        lower_idx = int(position)
        upper_idx = min(lower_idx + 1, len(self._sorted) - 1)
        lower_value = self._sorted[lower_idx]
        upper_value = self._sorted[upper_idx]
        if lower_idx == upper_idx:
            return lower_value
        fraction = position - lower_idx
        return lower_value + (upper_value - lower_value) * fraction


class LatencyMetrics:
    """Collect latency metrics for trading services."""

    _HISTOGRAM_SPECS = {
        "policy_latency_ms": 200.0,
        "risk_latency_ms": 200.0,
        "oms_latency_ms": 500.0,
    }

    _BUCKETS = (5, 10, 25, 50, 100, 200, 300, 500, 750, 1000, float("inf"))

    def __init__(
        self,
        registry: Optional[CollectorRegistry] = None,
        window_size: int = 1024,
    ) -> None:
        self._registry = registry or CollectorRegistry()
        self._histograms: Dict[str, Histogram] = {
            name: Histogram(
                name,
                f"Latency distribution for {name.replace('_', ' ')} in milliseconds.",
                ["symbol", "account_id"],
                buckets=self._BUCKETS,
                registry=self._registry,
            )
            for name in self._HISTOGRAM_SPECS
        }
        self._alerts = Gauge(
            "latency_p95_alert",
            "1 when the p95 latency breaches the configured SLO threshold.",
            ["metric", "symbol", "account_id"],
            registry=self._registry,
        )
        self._windows: Dict[str, MutableMapping[Tuple[str, str], _PercentileWindow]] = {
            name: defaultdict(lambda: _PercentileWindow(window_size))
            for name in self._HISTOGRAM_SPECS
        }

    @property
    def registry(self) -> CollectorRegistry:
        return self._registry

    def observe(
        self,
        metric: str,
        symbol: str,
        account_id: str,
        latency_ms: float,
    ) -> LatencySnapshot:
        """Record a new latency observation and update alerting state."""

        if metric not in self._histograms:
            raise ValueError(
                f"Unknown metric '{metric}'. Expected one of {tuple(self._histograms)}"
            )
        if latency_ms < 0:
            raise ValueError("latency_ms must be non-negative")

        histogram = self._histograms[metric]
        histogram.labels(symbol=symbol, account_id=account_id).observe(latency_ms)

        window = self._windows[metric][(symbol, account_id)]
        window.add(latency_ms)
        snapshot = window.snapshot()

        threshold = self._HISTOGRAM_SPECS[metric]
        alert_gauge = self._alerts.labels(metric=metric, symbol=symbol, account_id=account_id)
        if snapshot.p95 > threshold:
            alert_gauge.set(1)
        else:
            alert_gauge.set(0)
        return snapshot

    def get_snapshot(self, metric: str, symbol: str, account_id: str) -> LatencySnapshot:
        if metric not in self._histograms:
            raise ValueError(
                f"Unknown metric '{metric}'. Expected one of {tuple(self._histograms)}"
            )
        window = self._windows[metric].get((symbol, account_id))
        if window is None:
            return LatencySnapshot(0.0, 0.0, 0.0)
        return window.snapshot()

    def iter_snapshots(self) -> Iterable[tuple[str, str, str, LatencySnapshot]]:
        """Iterate over all known metric/symbol/account snapshots."""

        for metric, per_entity in self._windows.items():
            for (symbol, account_id), window in per_entity.items():
                yield metric, symbol, account_id, window.snapshot()


def create_metrics_app(metrics: LatencyMetrics) -> FastAPI:
    """Return a FastAPI application exposing the Prometheus registry."""

    app = FastAPI()

    @app.get("/metrics")
    def metrics_endpoint() -> Response:
        payload = generate_latest(metrics.registry)
        return Response(content=payload, media_type=CONTENT_TYPE_LATEST)

    return app
