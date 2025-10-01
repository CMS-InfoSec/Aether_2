"""Shared Prometheus metrics and tracing helpers for microservices."""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from typing import Dict, Generator, Optional
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from starlette.middleware.base import BaseHTTPMiddleware

try:  # pragma: no cover - OpenTelemetry may be optional in some deployments
    from opentelemetry import trace
    from opentelemetry.trace import Span, SpanKind
except Exception:  # pragma: no cover - fall back to no-op tracing
    trace = None  # type: ignore
    Span = object  # type: ignore
    SpanKind = None  # type: ignore


_REGISTRY = CollectorRegistry()

_trades_submitted_total = Counter(
    "trades_submitted_total",
    "Total number of trades successfully submitted.",
    registry=_REGISTRY,
)

_trades_rejected_total = Counter(
    "trades_rejected_total",
    "Total number of trades rejected during validation.",
    registry=_REGISTRY,
)

_oms_errors_total = Counter(
    "oms_errors_total",
    "Total number of errors returned by the OMS.",
    registry=_REGISTRY,
)

_oms_latency_ms = Gauge(
    "oms_latency_ms",
    "Current latency of OMS interactions in milliseconds.",
    registry=_REGISTRY,
)

_pipeline_latency_ms = Gauge(
    "pipeline_latency_ms",
    "Current latency of the trading decision pipeline in milliseconds.",
    registry=_REGISTRY,
)

_policy_inference_latency = Histogram(
    "policy_inference_latency",
    "Latency distribution for policy inference in milliseconds.",
    registry=_REGISTRY,
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, float("inf")),
)

_risk_validation_latency = Histogram(
    "risk_validation_latency",
    "Latency distribution for risk validation in milliseconds.",
    registry=_REGISTRY,
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, float("inf")),
)

_METRICS: Dict[str, Counter | Gauge | Histogram] = {
    "trades_submitted_total": _trades_submitted_total,
    "trades_rejected_total": _trades_rejected_total,
    "oms_errors_total": _oms_errors_total,
    "oms_latency_ms": _oms_latency_ms,
    "pipeline_latency_ms": _pipeline_latency_ms,
    "policy_inference_latency": _policy_inference_latency,
    "risk_validation_latency": _risk_validation_latency,
}

_INITIALISED = False
_SERVICE_NAME = "service"
_REQUEST_ID: ContextVar[Optional[str]] = ContextVar("request_id", default=None)


class RequestTracingMiddleware(BaseHTTPMiddleware):
    """Starlette middleware that wires request IDs and tracing spans."""

    def __init__(self, app: FastAPI, service_name: str):
        super().__init__(app)
        self._service_name = service_name
        self._tracer = trace.get_tracer(service_name) if trace else None
        self._span_kind = SpanKind.SERVER if SpanKind else None

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id") or str(uuid4())
        token = _REQUEST_ID.set(request_id)

        span_cm = (
            self._tracer.start_as_current_span(
                f"{self._service_name}.request", kind=self._span_kind
            )
            if self._tracer
            else nullcontext(None)
        )

        with span_cm as span:  # type: ignore[assignment]
            if span:
                span.set_attribute("service.name", self._service_name)
                span.set_attribute("request.id", request_id)
                span.set_attribute("http.method", request.method)
                span.set_attribute("http.url", str(request.url))

            response = await call_next(request)
            if span:
                span.set_attribute("http.status_code", response.status_code)

        _REQUEST_ID.reset(token)
        response.headers["x-request-id"] = request_id
        return response


def init_metrics(service_name: str = "service") -> Dict[str, Counter | Gauge | Histogram]:
    """Initialise metrics once and store the configured service name."""

    global _INITIALISED, _SERVICE_NAME
    if not _INITIALISED:
        _INITIALISED = True
    _SERVICE_NAME = service_name
    return _METRICS


def setup_metrics(app: FastAPI, service_name: str = "service") -> None:
    """Attach Prometheus /metrics endpoint and tracing middleware."""

    init_metrics(service_name)

    if not any(
        getattr(middleware, "cls", None) is RequestTracingMiddleware
        for middleware in app.user_middleware
    ):
        app.add_middleware(RequestTracingMiddleware, service_name=service_name)

    if not any(route.path == "/metrics" for route in app.routes):
        @app.get("/metrics")
        async def metrics_endpoint() -> Response:  # pragma: no cover - simple I/O
            payload = generate_latest(_REGISTRY)
            return Response(payload, media_type=CONTENT_TYPE_LATEST)


def increment_trades_submitted(amount: float = 1.0) -> None:
    init_metrics(_SERVICE_NAME)
    _trades_submitted_total.inc(amount)


def increment_trades_rejected(amount: float = 1.0) -> None:
    init_metrics(_SERVICE_NAME)
    _trades_rejected_total.inc(amount)


def increment_oms_errors(amount: float = 1.0) -> None:
    init_metrics(_SERVICE_NAME)
    _oms_errors_total.inc(amount)


def set_oms_latency(latency_ms: float) -> None:
    init_metrics(_SERVICE_NAME)
    _oms_latency_ms.set(latency_ms)


def set_pipeline_latency(latency_ms: float) -> None:
    init_metrics(_SERVICE_NAME)
    _pipeline_latency_ms.set(latency_ms)


def observe_policy_inference_latency(latency_ms: float) -> None:
    init_metrics(_SERVICE_NAME)
    _policy_inference_latency.observe(latency_ms)


def observe_risk_validation_latency(latency_ms: float) -> None:
    init_metrics(_SERVICE_NAME)
    _risk_validation_latency.observe(latency_ms)


def get_request_id() -> Optional[str]:
    """Return the current request identifier, if available."""

    return _REQUEST_ID.get()


@contextmanager
def traced_span(name: str, **attributes: object) -> Generator[Optional[Span], None, None]:
    """Create a child span that automatically injects the request ID."""

    tracer = trace.get_tracer(_SERVICE_NAME) if trace else None
    if tracer is None:
        yield None
        return

    with tracer.start_as_current_span(name) as span:
        request_id = get_request_id()
        if request_id:
            span.set_attribute("request.id", request_id)
        for key, value in attributes.items():
            span.set_attribute(key, value)
        yield span


__all__ = [
    "init_metrics",
    "setup_metrics",
    "increment_trades_submitted",
    "increment_trades_rejected",
    "increment_oms_errors",
    "set_oms_latency",
    "set_pipeline_latency",
    "observe_policy_inference_latency",
    "observe_risk_validation_latency",
    "get_request_id",
    "traced_span",
]
