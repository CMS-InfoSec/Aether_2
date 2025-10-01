"""Alertmanager notification helpers for operational events."""

from __future__ import annotations

import os
from typing import Dict, Optional

import httpx

_DEFAULT_ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://alertmanager:9093")
_ALERT_ENDPOINT = "/api/v2/alerts"


class AlertPushError(RuntimeError):
    """Raised when sending an alert to Alertmanager fails."""


def _post_alert(payload: Dict[str, object], alertmanager_url: Optional[str] = None) -> None:
    url = (alertmanager_url or _DEFAULT_ALERTMANAGER_URL).rstrip("/") + _ALERT_ENDPOINT
    with httpx.Client(timeout=5.0) as client:
        response = client.post(url, json=[payload])
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:  # pragma: no cover - defensive
            raise AlertPushError(f"Failed to push alert: {exc}") from exc


def _build_payload(alertname: str, labels: Dict[str, str], annotations: Dict[str, str]) -> Dict[str, object]:
    payload_labels = {"alertname": alertname, **labels}
    return {"labels": payload_labels, "annotations": annotations}


def push_latency_breach(
    account_id: str,
    symbol: str,
    observed_ms: float,
    threshold_ms: float,
    alertmanager_url: Optional[str] = None,
) -> None:
    payload = _build_payload(
        alertname="LatencyBreach",
        labels={"severity": "critical", "account_id": account_id, "symbol": symbol},
        annotations={
            "summary": "Latency breach detected",
            "description": (
                f"Observed latency {observed_ms:.2f}ms exceeds threshold {threshold_ms:.2f}ms"
            ),
        },
    )
    _post_alert(payload, alertmanager_url)


def push_fee_spike(
    account_id: str,
    symbol: str,
    fees_nav_pct: float,
    budget_pct: float,
    alertmanager_url: Optional[str] = None,
) -> None:
    payload = _build_payload(
        alertname="FeeSpike",
        labels={"severity": "warning", "account_id": account_id, "symbol": symbol},
        annotations={
            "summary": "Fee budget spike detected",
            "description": (
                f"Fees to NAV {fees_nav_pct:.2f}% exceeded budget {budget_pct:.2f}%"
            ),
        },
    )
    _post_alert(payload, alertmanager_url)


def push_no_trade_stall(
    account_id: str,
    symbol: str,
    duration_seconds: float,
    alertmanager_url: Optional[str] = None,
) -> None:
    payload = _build_payload(
        alertname="NoTradeStall",
        labels={"severity": "critical", "account_id": account_id, "symbol": symbol},
        annotations={
            "summary": "No trades observed",
            "description": (
                f"No trades executed for {duration_seconds:.0f}s on {symbol}"
            ),
        },
    )
    _post_alert(payload, alertmanager_url)


def push_universe_shrink(
    account_id: str,
    removed_symbols: int,
    alertmanager_url: Optional[str] = None,
) -> None:
    payload = _build_payload(
        alertname="UniverseShrink",
        labels={"severity": "warning", "account_id": account_id},
        annotations={
            "summary": "Universe size reduction detected",
            "description": f"{removed_symbols} symbols were removed from the trading universe",
        },
    )
    _post_alert(payload, alertmanager_url)


def push_drift_event(
    account_id: str,
    symbol: str,
    drift_score: float,
    alertmanager_url: Optional[str] = None,
) -> None:
    payload = _build_payload(
        alertname="ModelDrift",
        labels={"severity": "warning", "account_id": account_id, "symbol": symbol},
        annotations={
            "summary": "Model drift detected",
            "description": f"Drift score reported as {drift_score:.4f}",
        },
    )
    _post_alert(payload, alertmanager_url)
