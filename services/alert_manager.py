"""Alert manager utilities for Prometheus metrics and Alertmanager pushes."""
from __future__ import annotations

import json
import logging
import os
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional
from urllib import error as urllib_error
from urllib import request as urllib_request

from fastapi import FastAPI

try:  # pragma: no cover - prefer the real Prometheus client when available
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, REGISTRY
except ModuleNotFoundError:  # pragma: no cover - fall back to in-repo metrics shims
    from metrics import (  # type: ignore[attr-defined]
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        _REGISTRY as REGISTRY,
    )

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RiskEvent:
    """Structured risk engine event emitted by the trading system."""

    event_type: str
    severity: str
    description: str
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class OMSError:
    """Order management system error."""

    error_code: str
    description: str
    severity: str = "warning"
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class DriftSignal:
    """Model drift detection signal from monitoring services."""

    detector: str
    model: str
    score: float
    threshold: float
    labels: Dict[str, str] = field(default_factory=dict)


class AlertMetrics:
    """Container for Prometheus metrics used by alerting pathways."""

    def __init__(self, registry: Optional[CollectorRegistry] = None) -> None:
        metric_kwargs = {"registry": registry} if registry is not None else {}

        self.registry = registry or REGISTRY
        self.risk_events_total = Counter(
            "aether_risk_events_total",
            "Risk engine events observed by the alert manager.",
            ("event_type", "severity"),
            **metric_kwargs,
        )
        self.oms_errors_total = Counter(
            "aether_oms_errors_total",
            "Order management system errors observed by the alert manager.",
            ("error_code", "severity"),
            **metric_kwargs,
        )
        self.drift_score = Gauge(
            "aether_drift_detector_score",
            "Latest score reported by each drift detector.",
            ("detector", "model"),
            **metric_kwargs,
        )
        self.latency_ms = Histogram(
            "aether_alert_latency_ms",
            "Latency in milliseconds for named operations monitored by alerting.",
            ("name",),
            buckets=(5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 10000),
            **metric_kwargs,
        )
        self.fee_spike_pct = Gauge(
            "aether_fee_spike_pct",
            "Fee spike percentage observed for an account.",
            ("account_id",),
            **metric_kwargs,
        )
        self.trade_rejections_total = Counter(
            "aether_trade_rejections_total",
            "Total trade rejections observed per account.",
            ("account_id",),
            **metric_kwargs,
        )


_metrics_lock = threading.Lock()
_metrics: Optional[AlertMetrics] = None


def get_alert_metrics(registry: Optional[CollectorRegistry] = None) -> AlertMetrics:
    """Return the configured alert metrics, creating them if necessary."""

    global _metrics
    with _metrics_lock:
        if _metrics is None:
            _metrics = AlertMetrics(registry=registry)
        return _metrics


def configure_metrics(registry: Optional[CollectorRegistry] = None) -> AlertMetrics:
    """Reset and configure metrics, primarily useful for tests."""

    global _metrics
    with _metrics_lock:
        _metrics = AlertMetrics(registry=registry)
        return _metrics


class AlertManager:
    """Coordinates Prometheus metrics and Alertmanager notifications."""

    def __init__(
        self,
        metrics: AlertMetrics,
        alertmanager_url: Optional[str] = None,
        timeout: float = 5.0,
        push_severities: Optional[Iterable[str]] = None,
    ) -> None:
        self.metrics = metrics
        self.alertmanager_url = alertmanager_url or os.getenv("ALERTMANAGER_URL")
        self.timeout = timeout
        self.push_severities = {
            s.lower() for s in (push_severities or ("critical", "high", "warning"))
        }
        self._trade_rejection_counts: Dict[str, int] = defaultdict(int)

    # ------------------------------------------------------------------
    # Event ingestion helpers
    # ------------------------------------------------------------------
    def handle_risk_event(self, event: RiskEvent) -> None:
        """Consume a risk event and emit metrics/alerts."""

        severity = event.severity.lower()
        self.metrics.risk_events_total.labels(event.event_type, severity).inc()
        if severity in self.push_severities:
            self._push_alert(
                alert_name="RiskEngineEvent",
                severity=severity,
                description=event.description or event.event_type,
                labels={"event_type": event.event_type, **event.labels},
            )

    def handle_oms_error(self, error: OMSError) -> None:
        """Consume an OMS error and emit metrics/alerts."""

        severity = error.severity.lower()
        self.metrics.oms_errors_total.labels(error.error_code, severity).inc()
        if severity in self.push_severities:
            self._push_alert(
                alert_name="OMSError",
                severity=severity,
                description=error.description or error.error_code,
                labels={"error_code": error.error_code, **error.labels},
            )

    def handle_drift_signal(self, signal: DriftSignal) -> None:
        """Consume a drift signal and emit metrics/alerts when threshold reached."""

        self.metrics.drift_score.labels(signal.detector, signal.model).set(signal.score)
        if signal.score >= signal.threshold:
            self._push_alert(
                alert_name="ModelDriftDetected",
                severity="warning",
                description=
                f"Drift detector {signal.detector} for {signal.model} exceeded threshold {signal.threshold} with score {signal.score}",
                labels={
                    "detector": signal.detector,
                    "model": signal.model,
                    **signal.labels,
                },
            )

    def on_latency(self, name: str, latency_ms: float) -> None:
        self.metrics.latency_ms.labels(name).observe(latency_ms)
        if latency_ms >= 5000:
            self._push_alert(
                alert_name="LatencySpike",
                severity="warning",
                description=f"Observed latency of {latency_ms:.2f}ms for {name}",
                labels={"name": name},
            )

    def on_fee_spike(self, account_id: str, fee_pct: float) -> None:
        self.metrics.fee_spike_pct.labels(account_id).set(fee_pct)
        if fee_pct >= 5:
            self._push_alert(
                alert_name="FeeSpike",
                severity="warning",
                description=f"Account {account_id} fee spike {fee_pct:.2f}%",
                labels={"account_id": account_id},
            )

    def on_trade_rejection(self, account_id: str) -> None:
        self.metrics.trade_rejections_total.labels(account_id).inc()
        self._trade_rejection_counts[account_id] += 1
        if self._trade_rejection_counts[account_id] >= 5:
            self._push_alert(
                alert_name="TradeRejections",
                severity="warning",
                description=f"Account {account_id} experienced repeated trade rejections.",
                labels={"account_id": account_id},
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _push_alert(
        self,
        alert_name: str,
        severity: str,
        description: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        if not self.alertmanager_url:
            return

        payload = {
            "labels": {
                "alertname": alert_name,
                "severity": severity,
                **(labels or {}),
            },
            "annotations": {
                "description": description,
            },
            "startsAt": datetime.now(timezone.utc).isoformat(),
        }

        try:
            encoded_payload = json.dumps([payload]).encode("utf-8")
            req = urllib_request.Request(
                self.alertmanager_url,
                data=encoded_payload,
                headers={"Content-Type": "application/json"},
            )
            with urllib_request.urlopen(req, timeout=self.timeout):
                pass
        except urllib_error.URLError as exc:  # pragma: no cover - network failures are logged
            logger.warning("Failed to push alert to Alertmanager", exc_info=exc)


_alert_manager_lock = threading.Lock()
_alert_manager: Optional[AlertManager] = None


def set_alert_manager(manager: Optional[AlertManager]) -> None:
    global _alert_manager
    with _alert_manager_lock:
        _alert_manager = manager


def get_alert_manager_instance() -> Optional[AlertManager]:
    return _alert_manager


# ----------------------------------------------------------------------
# Helper functions requested by the prompt
# ----------------------------------------------------------------------
def alert_latency(name: str, ms: float) -> None:
    manager = get_alert_manager_instance()
    metrics = get_alert_metrics()
    metrics.latency_ms.labels(name).observe(ms)
    if manager:
        manager.on_latency(name, ms)


def alert_fee_spike(account_id: str, fee_pct: float) -> None:
    manager = get_alert_manager_instance()
    metrics = get_alert_metrics()
    metrics.fee_spike_pct.labels(account_id).set(fee_pct)
    if manager:
        manager.on_fee_spike(account_id, fee_pct)


def alert_trade_rejections(account_id: str) -> None:
    manager = get_alert_manager_instance()
    metrics = get_alert_metrics()
    metrics.trade_rejections_total.labels(account_id).inc()
    if manager:
        manager.on_trade_rejection(account_id)


def setup_alerting(app: FastAPI, alertmanager_url: Optional[str] = None) -> None:
    """Register startup/shutdown hooks for metrics and alert manager wiring."""

    @app.on_event("startup")
    async def _configure_alerting() -> None:  # pragma: no cover - FastAPI lifecycle
        metrics = get_alert_metrics()
        manager = AlertManager(metrics=metrics, alertmanager_url=alertmanager_url)
        set_alert_manager(manager)
        app.state.alert_manager = manager

    @app.on_event("shutdown")
    async def _clear_alerting() -> None:  # pragma: no cover - FastAPI lifecycle
        set_alert_manager(None)
        if hasattr(app.state, "alert_manager"):
            delattr(app.state, "alert_manager")


__all__ = [
    "AlertManager",
    "AlertMetrics",
    "DriftSignal",
    "OMSError",
    "RiskEvent",
    "alert_fee_spike",
    "alert_latency",
    "alert_trade_rejections",
    "configure_metrics",
    "get_alert_manager_instance",
    "get_alert_metrics",
    "setup_alerting",
]

