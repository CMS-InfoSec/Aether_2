"""Latency metrics collection and exposition helpers.

This module wires Prometheus histograms for the critical stages of the
trade lifecycle and maintains rolling windows that allow us to compute
p95 latency based alerts.  The metrics are exported through a lightweight
FastAPI application that serves the ``/metrics`` endpoint which can be
scraped by a Prometheus server.
"""

from __future__ import annotations

import logging
from bisect import bisect_left, insort
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, MutableMapping, Optional, Set, Tuple

from fastapi import FastAPI, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from metrics import AccountSegment, SymbolTier

LOGGER = logging.getLogger(__name__)

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


_MEGA_CAP_SYMBOLS = {"btc", "eth"}
_MAJOR_SYMBOLS = {"sol", "ada", "dot", "ltc", "matic", "atom"}
_STABLECOINS = {"usdt", "usdc", "dai", "busd", "tusd", "usdp"}


def _derive_account_segment(account_id: Optional[str]) -> AccountSegment:
    if not account_id:
        return AccountSegment.UNKNOWN

    value = account_id.strip().lower()
    if not value:
        return AccountSegment.UNKNOWN

    if value.startswith(("inst-", "fund-", "prime-")) or "fund" in value:
        return AccountSegment.INSTITUTIONAL
    if value.startswith(("mm-", "prop-", "desk-")) or "prop" in value:
        return AccountSegment.PROP
    if value.startswith(("sys-", "svc-", "svc_")) or value.endswith("-svc"):
        return AccountSegment.INTERNAL
    return AccountSegment.RETAIL


def _derive_symbol_tier(symbol: Optional[str]) -> SymbolTier:
    if not symbol:
        return SymbolTier.UNKNOWN

    value = symbol.strip().lower()
    if not value:
        return SymbolTier.UNKNOWN

    if value in _STABLECOINS:
        return SymbolTier.STABLECOIN
    if value in _MEGA_CAP_SYMBOLS:
        return SymbolTier.MEGA_CAP
    if value in _MAJOR_SYMBOLS:
        return SymbolTier.MAJOR
    if len(value) <= 5:
        return SymbolTier.MID_CAP
    return SymbolTier.LONG_TAIL


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
        allowed_label_pairs: Optional[Set[Tuple[str, str]]] = None,
    ) -> None:
        self._registry = registry or CollectorRegistry()
        allowed_label_pairs = allowed_label_pairs or {
            (tier.value, segment.value)
            for tier in SymbolTier
            for segment in AccountSegment
        }

        self._allowed_label_pairs: Set[Tuple[str, str]] = set(allowed_label_pairs)

        self._histograms: Dict[str, Histogram] = {
            name: Histogram(
                name,
                f"Latency distribution for {name.replace('_', ' ')} in milliseconds.",
                ["symbol_tier", "account_segment"],
                buckets=self._BUCKETS,
                registry=self._registry,
            )
            for name in self._HISTOGRAM_SPECS
        }
        self._alerts = Gauge(
            "latency_p95_alert",
            "1 when the p95 latency breaches the configured SLO threshold.",
            ["metric", "symbol_tier", "account_segment"],
            registry=self._registry,
        )
        self._discarded_pairs = Counter(
            "latency_discarded_label_pairs_total",
            "Number of latency samples discarded due to unsupported label pairs.",
            ["metric", "symbol_tier", "account_segment"],
            registry=self._registry,
        )
        self._windows: Dict[str, MutableMapping[Tuple[str, str], _PercentileWindow]] = {}
        for name in self._HISTOGRAM_SPECS:
            self._windows[name] = defaultdict(lambda: _PercentileWindow(window_size))

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

        symbol_tier = _derive_symbol_tier(symbol)
        account_segment = _derive_account_segment(account_id)
        label_pair = (symbol_tier.value, account_segment.value)

        if label_pair not in self._allowed_label_pairs:
            LOGGER.warning(
                "Discarding latency sample for metric=%s labels symbol_tier=%s account_segment=%s",
                metric,
                symbol_tier.value,
                account_segment.value,
            )
            self._discarded_pairs.labels(
                metric=metric,
                symbol_tier=symbol_tier.value,
                account_segment=account_segment.value,
            ).inc()
            self._windows[metric].pop(label_pair, None)
            return LatencySnapshot(0.0, 0.0, 0.0)

        histogram = self._histograms[metric]
        histogram.labels(
            symbol_tier=symbol_tier.value, account_segment=account_segment.value
        ).observe(latency_ms)

        window = self._windows[metric][label_pair]
        window.add(latency_ms)
        snapshot = window.snapshot()

        threshold = self._HISTOGRAM_SPECS[metric]
        alert_gauge = self._alerts.labels(
            metric=metric, symbol_tier=symbol_tier.value, account_segment=account_segment.value
        )
        if snapshot.p95 > threshold:
            alert_gauge.set(1)
        else:
            alert_gauge.set(0)
        return snapshot

    def get_snapshot(
        self, metric: str, symbol: str, account_id: str
    ) -> LatencySnapshot:
        if metric not in self._histograms:
            raise ValueError(
                f"Unknown metric '{metric}'. Expected one of {tuple(self._histograms)}"
            )
        symbol_tier = _derive_symbol_tier(symbol).value
        account_segment = _derive_account_segment(account_id).value
        label_pair = (symbol_tier, account_segment)
        if label_pair not in self._allowed_label_pairs:
            return LatencySnapshot(0.0, 0.0, 0.0)
        window = self._windows[metric].get(label_pair)
        if window is None:
            return LatencySnapshot(0.0, 0.0, 0.0)
        return window.snapshot()

    def iter_snapshots(self) -> Iterable[tuple[str, str, str, LatencySnapshot]]:
        """Iterate over all known metric/symbol_tier/account_segment snapshots."""

        for metric, per_entity in self._windows.items():
            for (symbol_tier, account_segment), window in list(per_entity.items()):
                label_pair = (symbol_tier, account_segment)
                if label_pair not in self._allowed_label_pairs:
                    LOGGER.debug(
                        "Evicting unsupported latency window metric=%s symbol_tier=%s account_segment=%s",
                        metric,
                        symbol_tier,
                        account_segment,
                    )
                    per_entity.pop(label_pair, None)
                    continue
                yield metric, symbol_tier, account_segment, window.snapshot()


def create_metrics_app(metrics: LatencyMetrics) -> FastAPI:
    """Return a FastAPI application exposing the Prometheus registry."""

    app = FastAPI()

    @app.get("/metrics")
    def metrics_endpoint() -> Response:
        payload = generate_latest(metrics.registry)
        return Response(content=payload, media_type=CONTENT_TYPE_LATEST)

    return app
