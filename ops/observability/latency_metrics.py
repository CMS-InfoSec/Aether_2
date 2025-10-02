"""Latency observability helpers for core trading services.

This module keeps a rolling window of latency samples for key services and
exposes latency distributions via Prometheus histograms.  For each service we
track the p50, p95 and p99 latencies and mark SLO violations whenever the
p95 latency exceeds ``ALERT_THRESHOLD_MS``.
"""

from __future__ import annotations

from bisect import bisect_left, insort
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, MutableMapping, Optional

from prometheus_client import CollectorRegistry, Gauge, Histogram

__all__ = [
    "LatencySnapshot",
    "LatencyMetrics",
]


@dataclass(frozen=True)
class LatencySnapshot:
    """Latency percentiles for a service stage."""

    p50: float
    p95: float
    p99: float


class _PercentileWindow:
    """Maintain a rolling, order-aware collection of samples.

    The window keeps the last ``maxlen`` samples in both insertion order and
    sorted order to provide deterministic percentile calculations without
    relying on external dependencies.  While this comes with a modest memory
    footprint, it keeps the implementation portable and easy to unit test.
    """

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
                # Remove the first match. There may be duplicates so we check
                # that the element exists before deleting.
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
    """Collect latency metrics for mission critical services."""

    _DEFAULT_BUCKETS = (
        5,
        10,
        25,
        50,
        100,
        250,
        500,
        1000,
        2000,
        float("inf"),
    )

    _STAGES = ("policy_inference", "risk_validation", "oms_execution")
    ALERT_THRESHOLD_MS = 250.0

    def __init__(
        self,
        registry: Optional[CollectorRegistry] = None,
        window_size: int = 1024,
    ) -> None:
        self._registry = registry or CollectorRegistry()
        self._histograms: Dict[str, Histogram] = {
            stage: Histogram(
                f"{stage}_latency_ms",
                f"Latency distribution for {stage.replace('_', ' ')} in milliseconds.",
                ["service"],
                buckets=self._DEFAULT_BUCKETS,
                registry=self._registry,
            )
            for stage in self._STAGES
        }
        self._alerts = Gauge(
            "latency_slo_violation",
            "Whether the p95 latency breached the 250ms SLO.",
            ["stage", "service"],
            registry=self._registry,
        )
        self._windows: Dict[str, MutableMapping[str, _PercentileWindow]] = {
            stage: defaultdict(lambda: _PercentileWindow(window_size))
            for stage in self._STAGES
        }

    @property
    def registry(self) -> CollectorRegistry:
        return self._registry

    def observe(self, stage: str, service: str, latency_ms: float) -> LatencySnapshot:
        """Record a new latency observation.

        Parameters
        ----------
        stage:
            One of ``policy_inference``, ``risk_validation`` or ``oms_execution``.
        service:
            Name of the service emitting the metric.
        latency_ms:
            Observed latency in milliseconds.
        """

        if stage not in self._STAGES:
            raise ValueError(f"Unknown stage '{stage}'. Expected one of {self._STAGES}")
        if latency_ms < 0:
            raise ValueError("latency_ms must be non-negative")

        histogram = self._histograms[stage]
        histogram.labels(service=service).observe(latency_ms)

        window = self._windows[stage][service]
        window.add(latency_ms)
        snapshot = window.snapshot()

        alert_gauge = self._alerts.labels(stage=stage, service=service)
        if snapshot.p95 > self.ALERT_THRESHOLD_MS:
            alert_gauge.set(1)
        else:
            alert_gauge.set(0)
        return snapshot

    def get_snapshot(self, stage: str, service: str) -> LatencySnapshot:
        if stage not in self._STAGES:
            raise ValueError(f"Unknown stage '{stage}'. Expected one of {self._STAGES}")
        window = self._windows[stage].get(service)
        if window is None:
            return LatencySnapshot(0.0, 0.0, 0.0)
        return window.snapshot()

    def iter_snapshots(self) -> Iterable[tuple[str, str, LatencySnapshot]]:
        """Iterate over all known stage/service snapshots."""

        for stage, per_service in self._windows.items():
            for service, window in per_service.items():
                yield stage, service, window.snapshot()
