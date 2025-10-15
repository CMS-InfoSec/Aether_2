"""Alertmanager notification helpers for operational events."""

from __future__ import annotations

import os
from typing import Dict, Mapping, Optional

from shared.common_bootstrap import ensure_httpx_ready

httpx = ensure_httpx_ready()

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


def push_exchange_adapter_failure(
    adapter: str,
    account_id: str,
    operation: str,
    error: str,
    alertmanager_url: Optional[str] = None,
) -> None:
    """Emit a high severity alert when an exchange adapter call fails."""

    summary = f"{adapter} adapter {operation} failure"
    description = f"Operation {operation} failed for {account_id}: {error}"
    payload = _build_payload(
        alertname="ExchangeAdapterFailure",
        labels={
            "severity": "critical",
            "adapter": adapter,
            "account_id": account_id,
            "operation": operation,
        },
        annotations={"summary": summary, "description": description},
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


def push_dependency_fallback(
    component: str,
    dependency: str,
    fallback: str,
    *,
    severity: str = "critical",
    environment: Optional[str] = None,
    details: Optional[Mapping[str, str]] = None,
    alertmanager_url: Optional[str] = None,
) -> None:
    """Emit an alert when a critical dependency is unavailable and a fallback activates."""

    labels: Dict[str, str] = {
        "severity": severity,
        "component": component,
        "dependency": dependency,
        "fallback": fallback,
    }
    if environment:
        labels["environment"] = environment

    summary = f"{component} activated fallback for missing {dependency}"
    description_parts = [
        f"{component} is using {fallback} because the {dependency} dependency is unavailable.",
    ]

    annotations: Dict[str, str] = {"summary": summary}
    if details:
        normalized = {key: str(value) for key, value in details.items()}
        detail_pairs = ", ".join(f"{key}={value}" for key, value in sorted(normalized.items()))
        description_parts.append(f"Details: {detail_pairs}")
        for key, value in normalized.items():
            annotations[f"detail_{key}"] = value

    annotations["description"] = " ".join(description_parts)

    payload = _build_payload(
        alertname="CriticalDependencyFallback",
        labels=labels,
        annotations=annotations,
    )

    try:
        _post_alert(payload, alertmanager_url)
    except AlertPushError:
        raise
    except Exception as exc:  # pragma: no cover - defensive guard
        raise AlertPushError(f"Failed to push dependency fallback alert: {exc}") from exc
