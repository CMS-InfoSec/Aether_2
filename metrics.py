"""Prometheus metrics helpers shared across microservices."""

from __future__ import annotations

from typing import Dict

from fastapi import FastAPI, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

_REGISTRY = CollectorRegistry()

_ws_latency_ms = Gauge(
    "ws_latency_ms",
    "Latency of websocket interactions in milliseconds.",
    labelnames=("account_id", "symbol"),
    registry=_REGISTRY,
)

_oms_submit_ack_ms = Gauge(
    "oms_submit_ack_ms",
    "End-to-end latency between OMS submit and acknowledgement in milliseconds.",
    labelnames=("account_id", "symbol"),
    registry=_REGISTRY,
)

_oms_latency_ms = Gauge(
    "oms_latency_ms",
    "Latency of OMS order placement by transport in milliseconds.",
    labelnames=("account_id", "symbol", "transport"),
    registry=_REGISTRY,
)

_oms_error_count = Counter(
    "oms_error_count",
    "Count of OMS transport errors by account, symbol and transport.",
    labelnames=("account_id", "symbol", "transport"),
    registry=_REGISTRY,
)

_oms_child_orders_total = Counter(
    "oms_child_orders_total",
    "Number of OMS child orders created per account and symbol.",
    labelnames=("account_id", "symbol"),
    registry=_REGISTRY,
)

_trade_rejections_total = Counter(
    "trade_rejections_total",
    "Count of rejected trades by account and symbol.",
    labelnames=("account_id", "symbol"),
    registry=_REGISTRY,
)

_fees_nav_pct = Gauge(
    "fees_nav_pct",
    "Fees paid as a percentage of NAV by account and symbol.",
    labelnames=("account_id", "symbol"),
    registry=_REGISTRY,
)

_drift_score = Gauge(
    "drift_score",
    "Model drift score by account and symbol.",
    labelnames=("account_id", "symbol"),
    registry=_REGISTRY,
)

_abstention_rate = Gauge(
    "abstention_rate",
    "Observed abstention rate for policy decisions by account and symbol.",
    labelnames=("account_id", "symbol"),
    registry=_REGISTRY,
)

_METRICS: Dict[str, Gauge | Counter] = {
    "ws_latency_ms": _ws_latency_ms,
    "oms_submit_ack_ms": _oms_submit_ack_ms,
    "oms_latency_ms": _oms_latency_ms,
    "oms_error_count": _oms_error_count,
    "oms_child_orders_total": _oms_child_orders_total,
    "trade_rejections_total": _trade_rejections_total,
    "fees_nav_pct": _fees_nav_pct,
    "drift_score": _drift_score,
    "abstention_rate": _abstention_rate,
}

_INITIALISED = False


def init_metrics() -> Dict[str, Gauge | Counter]:
    """Ensure metrics are initialised before use."""

    global _INITIALISED
    if not _INITIALISED:
        _INITIALISED = True
    return _METRICS


def setup_metrics(app: FastAPI) -> None:
    """Attach the Prometheus metrics endpoint and initialise counters."""

    init_metrics()

    @app.on_event("startup")
    async def _setup_metrics() -> None:  # pragma: no cover - FastAPI lifecycle
        init_metrics()

    if not any(route.path == "/metrics" for route in app.routes):
        @app.get("/metrics")
        async def metrics_endpoint() -> Response:  # pragma: no cover - simple I/O
            init_metrics()
            payload = generate_latest(_REGISTRY)
            return Response(payload, media_type=CONTENT_TYPE_LATEST)


def record_ws_latency(account_id: str, symbol: str, latency_ms: float) -> None:
    init_metrics()
    _ws_latency_ms.labels(account_id=account_id, symbol=symbol).set(latency_ms)


def record_oms_submit_ack(account_id: str, symbol: str, latency_ms: float) -> None:
    init_metrics()
    _oms_submit_ack_ms.labels(account_id=account_id, symbol=symbol).set(latency_ms)


def record_oms_latency(account_id: str, symbol: str, transport: str, latency_ms: float) -> None:
    init_metrics()
    _oms_latency_ms.labels(
        account_id=account_id, symbol=symbol, transport=transport
    ).set(latency_ms)


def increment_oms_error_count(account_id: str, symbol: str, transport: str) -> None:
    init_metrics()
    _oms_error_count.labels(
        account_id=account_id, symbol=symbol, transport=transport
    ).inc()


def increment_oms_child_orders_total(account_id: str, symbol: str, count: int) -> None:
    init_metrics()
    _oms_child_orders_total.labels(account_id=account_id, symbol=symbol).inc(count)


def increment_trade_rejection(account_id: str, symbol: str) -> None:
    init_metrics()
    _trade_rejections_total.labels(account_id=account_id, symbol=symbol).inc()


def record_fees_nav_pct(account_id: str, symbol: str, pct: float) -> None:
    init_metrics()
    _fees_nav_pct.labels(account_id=account_id, symbol=symbol).set(pct)


def record_drift_score(account_id: str, symbol: str, score: float) -> None:
    init_metrics()
    _drift_score.labels(account_id=account_id, symbol=symbol).set(score)


def record_abstention_rate(account_id: str, symbol: str, rate: float) -> None:
    init_metrics()
    _abstention_rate.labels(account_id=account_id, symbol=symbol).set(rate)


def get_registry() -> CollectorRegistry:
    """Expose the CollectorRegistry for advanced customisation or testing."""

    init_metrics()
    return _REGISTRY
