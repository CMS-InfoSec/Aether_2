"""Shared Prometheus metrics and tracing helpers for microservices."""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, replace
from enum import Enum
import logging
import os
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Dict, Generator, Iterable, Optional
from uuid import uuid4

from fastapi import FastAPI, Request, Response

try:  # pragma: no cover - prometheus client may be unavailable in lightweight tests
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
        start_http_server,
    )
except ImportError:  # pragma: no cover - provide a no-op metrics backend
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4"  # type: ignore[assignment]

    class CollectorRegistry:  # type: ignore[override]
        """Lightweight registry compatible with the Prometheus client interface."""

        def __init__(self) -> None:
            self._collectors: Dict[int, object] = {}
            self._names_to_collectors: Dict[str, object] = {}

        def register(self, collector: object) -> None:
            self._collectors[id(collector)] = collector
            name = getattr(collector, "_name", None)
            if isinstance(name, str):
                self._names_to_collectors[name] = collector

        def unregister(self, collector: object) -> None:
            self._collectors.pop(id(collector), None)
            name = getattr(collector, "_name", None)
            if isinstance(name, str):
                self._names_to_collectors.pop(name, None)

        def collect(self):  # pragma: no cover - used in optional dependency paths
            for collector in list(self._collectors.values()):
                if hasattr(collector, "collect"):
                    yield from collector.collect()

        def get_sample_value(
            self, name: str, labels: Optional[Dict[str, str]] = None
        ) -> float | None:
            collector = self._names_to_collectors.get(name)
            if collector is None:
                for candidate in self._collectors.values():
                    if getattr(candidate, "_name", None) == name:
                        collector = candidate
                        break
            if collector is None or not hasattr(collector, "collect"):
                return None
            for family in collector.collect():
                samples = getattr(family, "samples", [])
                for sample in samples:
                    if labels:
                        if all(sample.labels.get(k) == v for k, v in labels.items()):
                            return sample.value
                    else:
                        return sample.value
            return None

    @dataclass
    class _Sample:
        name: str
        labels: Dict[str, str]
        value: float

    class _Metric:
        def __init__(
            self,
            name: str,
            documentation: str,
            labelnames: Optional[Iterable[str]] = None,
            *,
            registry: CollectorRegistry | None = None,
            **_: Any,
        ) -> None:
            self._name = name
            self._documentation = documentation
            self._labelnames = tuple(labelnames or ())
            self._values: Dict[tuple[str, ...], float] = {}
            if registry is not None:
                try:
                    registry.register(self)
                except AttributeError:  # pragma: no cover - defensive guard
                    pass

        def labels(self, *args: Any, **kwargs: Any) -> "_Metric":
            if args and kwargs:
                raise ValueError("Use either positional or keyword labels, not both")
            if args:
                key = tuple(str(value) for value in args)
            else:
                key = tuple(str(kwargs.get(name, "")) for name in self._labelnames)
            self._current_key = key
            self._values.setdefault(key, 0.0)
            return self

        def inc(self, amount: float = 1.0) -> None:
            key = getattr(self, "_current_key", ())
            self._values[key] = self._values.get(key, 0.0) + amount

        def dec(self, amount: float = 1.0) -> None:
            key = getattr(self, "_current_key", ())
            self._values[key] = self._values.get(key, 0.0) - amount

        def set(self, value: float) -> None:
            key = getattr(self, "_current_key", ())
            self._values[key] = value

        def observe(self, value: float) -> None:
            self.set(value)

        def collect(self):  # pragma: no cover - exercised via registry.collect
            samples = [
                _Sample(
                    name=self._name,
                    labels={name: label for name, label in zip(self._labelnames, key)},
                    value=value,
                )
                for key, value in self._values.items()
            ]
            yield SimpleNamespace(name=self._name, documentation=self._documentation, samples=samples)

    def Counter(name: str, documentation: str, labelnames=(), **kwargs: Any) -> _Metric:  # type: ignore[override]
        return _Metric(name, documentation, labelnames, **kwargs)

    def Gauge(name: str, documentation: str, labelnames=(), **kwargs: Any) -> _Metric:  # type: ignore[override]
        return _Metric(name, documentation, labelnames, **kwargs)

    def Histogram(name: str, documentation: str, labelnames=(), **kwargs: Any) -> _Metric:  # type: ignore[override]
        return _Metric(name, documentation, labelnames, **kwargs)

    def generate_latest(*args: Any, **kwargs: Any) -> bytes:  # type: ignore[override]
        return b""

    def start_http_server(*args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        logger = logging.getLogger(__name__)
        logger.warning("prometheus_client.start_http_server unavailable; exporter disabled")
try:  # pragma: no cover - optional dependency in lightweight test environments
    from starlette.middleware.base import BaseHTTPMiddleware
except ImportError:  # pragma: no cover - exercised in unit-only environments
    class BaseHTTPMiddleware:  # type: ignore[override]
        def __init__(self, app: Any) -> None:
            self.app = app

        async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]):  # type: ignore[override]
            return await call_next(request)

try:  # pragma: no cover - OpenTelemetry may be optional in some deployments
    from opentelemetry import trace
    from opentelemetry.trace import Span, SpanKind
except Exception:  # pragma: no cover - fall back to no-op tracing
    trace = None  # type: ignore
    Span = object  # type: ignore
    SpanKind = None  # type: ignore


logger = logging.getLogger(__name__)

_REGISTRY = CollectorRegistry()

_PROMETHEUS_EXPORTER_STARTED = False
_TRACING_CONFIGURED = False
_TRACING_SERVICE_NAME: Optional[str] = None


def _configure_prometheus_exporter_from_env() -> None:
    """Start a Prometheus scrape endpoint when configured via environment variables."""

    global _PROMETHEUS_EXPORTER_STARTED
    if _PROMETHEUS_EXPORTER_STARTED:
        return

    port_env = os.getenv("PROMETHEUS_EXPORTER_PORT")
    if not port_env:
        return

    try:
        port = int(port_env)
    except ValueError:
        logger.warning(
            "Invalid PROMETHEUS_EXPORTER_PORT value '%s'; expected an integer", port_env
        )
        return

    addr = os.getenv("PROMETHEUS_EXPORTER_ADDR", "0.0.0.0")
    try:
        start_http_server(port, addr=addr, registry=_REGISTRY)
    except Exception as exc:  # pragma: no cover - depends on prometheus client implementation
        logger.warning(
            "Failed to start Prometheus exporter on %s:%s: %s", addr, port, exc
        )
        return

    _PROMETHEUS_EXPORTER_STARTED = True
    logger.info("Prometheus exporter listening on %s:%s", addr, port)


def _parse_otlp_headers(raw: Optional[str]) -> Dict[str, str]:
    if not raw:
        return {}
    headers: Dict[str, str] = {}
    for item in raw.split(","):
        key, _, value = item.partition("=")
        if not key or not _:
            continue
        headers[key.strip()] = value.strip()
    return headers


def _configure_tracing_from_env(service_name: str, *, endpoint: Optional[str] = None) -> None:
    """Wire an OTLP exporter when OpenTelemetry is installed and configured."""

    global _TRACING_CONFIGURED, _TRACING_SERVICE_NAME
    if _TRACING_CONFIGURED:
        return

    endpoint = endpoint or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return

    if trace is None:
        logger.warning(
            "OpenTelemetry API unavailable; skipping OTLP tracing exporter for %s",
            service_name,
        )
        return

    try:  # pragma: no cover - depends on optional OpenTelemetry SDK
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    except Exception as exc:  # pragma: no cover - optional dependency may be missing
        logger.warning(
            "OpenTelemetry SDK unavailable; skipping OTLP exporter for %s: %s",
            service_name,
            exc,
        )
        return

    headers = _parse_otlp_headers(os.getenv("OTEL_EXPORTER_OTLP_HEADERS"))
    insecure = os.getenv("OTEL_EXPORTER_OTLP_INSECURE") == "1"
    certificate = os.getenv("OTEL_EXPORTER_OTLP_CERTIFICATE")
    timeout_env = os.getenv("OTEL_EXPORTER_OTLP_TIMEOUT")
    exporter_kwargs: Dict[str, Any] = {"endpoint": endpoint}
    if headers:
        exporter_kwargs["headers"] = headers
    if insecure:
        exporter_kwargs["insecure"] = True
    if certificate:
        exporter_kwargs["certificates"] = certificate
    if timeout_env:
        try:
            exporter_kwargs["timeout"] = float(timeout_env)
        except ValueError:
            logger.warning(
                "Invalid OTEL_EXPORTER_OTLP_TIMEOUT value '%s'; ignoring", timeout_env
            )

    try:
        exporter = OTLPSpanExporter(**exporter_kwargs)
    except Exception as exc:  # pragma: no cover - depends on exporter implementation
        logger.warning(
            "Failed to initialise OTLP exporter for %s: %s", service_name, exc
        )
        return

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    try:
        trace.set_tracer_provider(provider)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to register tracer provider for %s: %s", service_name, exc)
        return

    _TRACING_CONFIGURED = True
    _TRACING_SERVICE_NAME = service_name
    logger.info("Configured OTLP tracing exporter for %s", service_name)


def configure_observability(service_name: str) -> None:
    """Configure Prometheus and OpenTelemetry exporters based on environment settings."""

    _configure_prometheus_exporter_from_env()
    _configure_tracing_from_env(service_name)


class AccountSegment(str, Enum):
    """Bounded aggregation buckets for account level metrics."""

    RETAIL = "retail"
    INSTITUTIONAL = "institutional"
    PROP = "prop"
    INTERNAL = "internal"
    UNKNOWN = "unknown"


class SymbolTier(str, Enum):
    """Market capitalisation buckets for instruments."""

    MEGA_CAP = "mega_cap"
    MAJOR = "major"
    MID_CAP = "mid_cap"
    LONG_TAIL = "long_tail"
    STABLECOIN = "stablecoin"
    UNKNOWN = "unknown"


class TransportType(str, Enum):
    """Normalised OMS transport channels."""

    REST = "rest"
    WEBSOCKET = "websocket"
    FIX = "fix"
    INTERNAL = "internal"
    BATCH = "batch"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class MetricContext:
    """Bounded labels derived from request context for metric emission."""

    service: Optional[str] = None
    account_id: str = "unknown"
    account_segment: AccountSegment = AccountSegment.UNKNOWN
    symbol_tier: SymbolTier = SymbolTier.UNKNOWN
    transport: TransportType = TransportType.UNKNOWN

    def for_service(self, service: Optional[str]) -> "MetricContext":
        if not service:
            return self
        return replace(self, service=_service_value(service))


_REQUEST_CONTEXT: ContextVar[MetricContext] = ContextVar(
    "metric_context", default=MetricContext()
)

_trades_submitted_total = Counter(
    "trades_submitted_total",
    "Total number of trades successfully submitted.",
    ["service"],
    registry=_REGISTRY,
)

_trades_rejected_total = Counter(
    "trades_rejected_total",
    "Total number of trades rejected during validation.",
    ["service", "reason"],
    registry=_REGISTRY,
)

_trade_rejections_total = Counter(
    "trade_rejections_total",
    "Total number of trades rejected by risk evaluation.",
    ["service", "account_id", "account_segment", "symbol_tier"],
    registry=_REGISTRY,
)

_rejected_intents_total = Counter(
    "rejected_intents_total",
    "Number of intents rejected during the sequencing pipeline.",
    ["service", "stage", "reason"],
    registry=_REGISTRY,
)

_safe_mode_triggers_total = Counter(
    "safe_mode_triggers_total",
    "Number of times safe mode has been engaged.",
    ["service", "reason"],
    registry=_REGISTRY,
)

_oms_errors_total = Counter(
    "oms_errors_total",
    "Total number of errors returned by the OMS.",
    ["service", "account_segment", "symbol_tier", "transport"],
    registry=_REGISTRY,
)

_oms_auth_failures_total = Counter(
    "oms_auth_failures_total",
    "Number of authentication or authorization failures encountered by the OMS API.",
    ["service", "reason"],
    registry=_REGISTRY,
)

_oms_child_orders_total = Counter(
    "oms_child_orders_total",
    "Number of child orders generated by the OMS.",
    ["service", "account_segment", "symbol_tier", "transport"],
    registry=_REGISTRY,
)

_oms_stale_feed_total = Counter(
    "oms_stale_feed_total",
    "Number of OMS operations impacted by stale data feeds.",
    ["service", "account_segment", "symbol_tier", "source", "action"],
    registry=_REGISTRY,
)

_oms_latency_ms = Gauge(
    "oms_latency_ms",
    "Current latency of OMS interactions in milliseconds.",
    ["service", "account_segment", "symbol_tier", "transport"],
    registry=_REGISTRY,
)

_pipeline_latency_ms = Gauge(
    "pipeline_latency_ms",
    "Current latency of the trading decision pipeline in milliseconds.",
    ["service"],
    registry=_REGISTRY,
)

_scaling_oms_replicas = Gauge(
    "scaling_oms_replicas",
    "Current OMS replica count recorded by the scaling controller.",
    ["service"],
    registry=_REGISTRY,
)

_scaling_gpu_nodes = Gauge(
    "scaling_gpu_nodes",
    "Number of GPU nodes the scaling controller believes are provisioned.",
    ["service"],
    registry=_REGISTRY,
)

_scaling_pending_training_jobs = Gauge(
    "scaling_pending_training_jobs",
    "Pending training jobs observed by the scaling controller.",
    ["service"],
    registry=_REGISTRY,
)

_scaling_evaluation_duration_seconds = Histogram(
    "scaling_evaluation_duration_seconds",
    "Duration of scaling controller evaluation cycles in seconds.",
    ["service"],
    registry=_REGISTRY,
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, float("inf")),
)

_scaling_evaluations_total = Counter(
    "scaling_evaluations_total",
    "Total number of scaling controller evaluations executed.",
    ["service"],
    registry=_REGISTRY,
)

_policy_abstention_rate = Gauge(
    "policy_abstention_rate",
    "Observed abstention rate from policy decisions.",
    ["service", "account_id", "account_segment", "symbol_tier"],
    registry=_REGISTRY,
)

_policy_drift_score = Gauge(
    "policy_drift_score",
    "Policy drift score per account and symbol.",
    ["service", "account_id", "account_segment", "symbol_tier"],
    registry=_REGISTRY,
)

_fees_nav_pct = Gauge(
    "fees_nav_pct",
    "Percentage of fees to net asset value for risk evaluation.",
    ["service", "account_id", "account_segment", "symbol_tier"],
    registry=_REGISTRY,
)

_account_daily_realized_pnl = Gauge(
    "account_daily_realized_pnl_usd",
    "Daily realized profit and loss per account in USD.",
    ["service", "account_id"],
    registry=_REGISTRY,
)

_account_daily_unrealized_pnl = Gauge(
    "account_daily_unrealized_pnl_usd",
    "Daily unrealized profit and loss per account in USD.",
    ["service", "account_id"],
    registry=_REGISTRY,
)

_account_daily_net_pnl = Gauge(
    "account_daily_net_pnl_usd",
    "Net daily profit and loss per account in USD after fees.",
    ["service", "account_id"],
    registry=_REGISTRY,
)

_account_drawdown_pct = Gauge(
    "account_drawdown_pct",
    "Latest observed drawdown for the account expressed as a percentage.",
    ["service", "account_id"],
    registry=_REGISTRY,
)

_account_exposure_usd = Gauge(
    "account_exposure_usd",
    "Open exposure per account and symbol in USD.",
    ["service", "account_id", "symbol", "symbol_tier"],
    registry=_REGISTRY,
)

_policy_inference_latency = Histogram(
    "policy_inference_latency",
    "Latency distribution for policy inference in milliseconds.",
    ["service"],
    registry=_REGISTRY,
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, float("inf")),
)

_risk_validation_latency = Histogram(
    "risk_validation_latency",
    "Latency distribution for risk validation in milliseconds.",
    ["service"],
    registry=_REGISTRY,
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, float("inf")),
)

_oms_submit_latency = Histogram(
    "oms_submit_latency",
    "Latency distribution for submitting intents to the OMS in milliseconds.",
    ["service", "transport"],
    registry=_REGISTRY,
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, float("inf")),
)

_late_events_total = Counter(
    "late_events_total",
    "Number of market data events that exceeded the lateness watermark.",
    ["service", "stream"],
    registry=_REGISTRY,
)

_reorder_buffer_depth = Gauge(
    "reorder_buffer_depth",
    "Current depth of the event reordering buffer.",
    ["service", "stream"],
    registry=_REGISTRY,
)

_METRICS: Dict[str, Counter | Gauge | Histogram] = {
    "trades_submitted_total": _trades_submitted_total,
    "trades_rejected_total": _trades_rejected_total,
    "trade_rejections_total": _trade_rejections_total,
    "rejected_intents_total": _rejected_intents_total,
    "safe_mode_triggers_total": _safe_mode_triggers_total,
    "oms_errors_total": _oms_errors_total,
    "oms_auth_failures_total": _oms_auth_failures_total,
    "oms_child_orders_total": _oms_child_orders_total,
    "oms_stale_feed_total": _oms_stale_feed_total,
    "oms_latency_ms": _oms_latency_ms,
    "pipeline_latency_ms": _pipeline_latency_ms,
    "policy_abstention_rate": _policy_abstention_rate,
    "policy_drift_score": _policy_drift_score,
    "fees_nav_pct": _fees_nav_pct,
    "account_daily_realized_pnl_usd": _account_daily_realized_pnl,
    "account_daily_unrealized_pnl_usd": _account_daily_unrealized_pnl,
    "account_daily_net_pnl_usd": _account_daily_net_pnl,
    "account_drawdown_pct": _account_drawdown_pct,
    "account_exposure_usd": _account_exposure_usd,
    "policy_inference_latency": _policy_inference_latency,
    "risk_validation_latency": _risk_validation_latency,
    "oms_submit_latency": _oms_submit_latency,
    "late_events_total": _late_events_total,
    "reorder_buffer_depth": _reorder_buffer_depth,
    "scaling_oms_replicas": _scaling_oms_replicas,
    "scaling_gpu_nodes": _scaling_gpu_nodes,
    "scaling_pending_training_jobs": _scaling_pending_training_jobs,
    "scaling_evaluation_duration_seconds": _scaling_evaluation_duration_seconds,
    "scaling_evaluations_total": _scaling_evaluations_total,
}

_INITIALISED = False
_SERVICE_NAME = "service"
_REQUEST_ID: ContextVar[Optional[str]] = ContextVar("request_id", default=None)


def _account_value(account_id: Optional[str] = None) -> str:
    if account_id is None:
        return "unknown"
    value = str(account_id).strip()
    return value or "unknown"


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


_MEGA_CAP_SYMBOLS = {"btc", "eth"}
_MAJOR_SYMBOLS = {"sol", "ada", "dot", "ltc", "matic", "atom"}
_STABLECOINS = {"usdt", "usdc", "dai", "busd", "tusd", "usdp"}


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


def _derive_transport(value: Optional[str | TransportType]) -> TransportType:
    if isinstance(value, TransportType):
        return value
    if not value:
        return TransportType.UNKNOWN

    normalised = value.strip().lower()
    if normalised in {"rest", "http"}:
        return TransportType.REST
    if normalised in {"ws", "websocket"}:
        return TransportType.WEBSOCKET
    if normalised == "fix":
        return TransportType.FIX
    if normalised in {"grpc", "internal"}:
        return TransportType.INTERNAL
    if normalised in {"batch", "sftp"}:
        return TransportType.BATCH
    return TransportType.UNKNOWN


def _resolve_metric_context(
    *,
    context: Optional[MetricContext] = None,
    service: Optional[str] = None,
    account_id: Optional[str] = None,
    account_segment: Optional[AccountSegment] = None,
    symbol: Optional[str] = None,
    symbol_tier: Optional[SymbolTier] = None,
    transport: Optional[str | TransportType] = None,
) -> MetricContext:
    base = context or _REQUEST_CONTEXT.get()

    resolved_service = _service_value(service or base.service)
    resolved_account_id = (
        _account_value(account_id) if account_id is not None else base.account_id
    )
    resolved_account_segment = account_segment or (
        _derive_account_segment(account_id) if account_id else base.account_segment
    )
    resolved_symbol_tier = symbol_tier or (
        _derive_symbol_tier(symbol) if symbol else base.symbol_tier
    )
    resolved_transport = _derive_transport(transport) if transport else base.transport

    return MetricContext(
        service=resolved_service,
        account_id=resolved_account_id,
        account_segment=resolved_account_segment,
        symbol_tier=resolved_symbol_tier,
        transport=resolved_transport,
    )


def _account_symbol_labels(ctx: MetricContext) -> Dict[str, str]:
    return {
        "service": _service_value(ctx.service),
        "account_id": ctx.account_id,
        "account_segment": ctx.account_segment.value,
        "symbol_tier": ctx.symbol_tier.value,
    }


def _transport_labels(ctx: MetricContext) -> Dict[str, str]:
    labels = _account_symbol_labels(ctx)
    labels["transport"] = ctx.transport.value
    return labels


def _service_value(service: Optional[str] = None) -> str:
    value = service or _SERVICE_NAME or "service"
    return value


def _normalised(value: Optional[str], default: str) -> str:
    if value and value.strip():
        return value.strip()
    return default


def _coerce_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


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
    _SERVICE_NAME = _normalised(service_name, "service")
    if not _INITIALISED:
        _INITIALISED = True

    # Prime the core series so they appear immediately for the service.
    base_service = _service_value()
    base_account_id = _account_value()
    base_account_labels = {
        "service": base_service,
        "account_id": base_account_id,
        "account_segment": AccountSegment.UNKNOWN.value,
        "symbol_tier": SymbolTier.UNKNOWN.value,
    }

    _trades_submitted_total.labels(service=base_service)
    _trades_rejected_total.labels(service=base_service, reason="unknown")
    _trade_rejections_total.labels(**base_account_labels)
    _rejected_intents_total.labels(
        service=base_service, stage="unknown", reason="unknown"
    )
    _safe_mode_triggers_total.labels(service=base_service, reason="unknown")
    _oms_errors_total.labels(
        service=base_service,
        account_segment=AccountSegment.UNKNOWN.value,
        symbol_tier=SymbolTier.UNKNOWN.value,
        transport=TransportType.UNKNOWN.value,
    )
    _oms_auth_failures_total.labels(service=base_service, reason="unknown")
    _oms_child_orders_total.labels(
        service=base_service,
        account_segment=AccountSegment.UNKNOWN.value,
        symbol_tier=SymbolTier.UNKNOWN.value,
        transport=TransportType.UNKNOWN.value,
    )
    _oms_latency_ms.labels(
        service=base_service,
        account_segment=AccountSegment.UNKNOWN.value,
        symbol_tier=SymbolTier.UNKNOWN.value,
        transport=TransportType.UNKNOWN.value,
    )
    _pipeline_latency_ms.labels(service=base_service)
    _policy_abstention_rate.labels(**base_account_labels)
    _policy_drift_score.labels(**base_account_labels)
    _fees_nav_pct.labels(**base_account_labels)
    _policy_inference_latency.labels(service=base_service)
    _risk_validation_latency.labels(service=base_service)
    _oms_submit_latency.labels(
        service=base_service, transport=TransportType.UNKNOWN.value
    )
    _late_events_total.labels(service=base_service, stream="unknown")
    _reorder_buffer_depth.labels(service=base_service, stream="unknown").set(0)
    _scaling_oms_replicas.labels(service=base_service).set(0)
    _scaling_gpu_nodes.labels(service=base_service).set(0)
    _scaling_pending_training_jobs.labels(service=base_service).set(0)
    evaluation_metric = _scaling_evaluation_duration_seconds.labels(service=base_service)
    if hasattr(evaluation_metric, "observe"):
        evaluation_metric.observe(0)
    _scaling_evaluations_total.labels(service=base_service)

    simple_account_labels = {"service": base_service, "account_id": base_account_id}
    _account_daily_realized_pnl.labels(**simple_account_labels).set(0.0)
    _account_daily_unrealized_pnl.labels(**simple_account_labels).set(0.0)
    _account_daily_net_pnl.labels(**simple_account_labels).set(0.0)
    _account_drawdown_pct.labels(**simple_account_labels).set(0.0)
    _account_exposure_usd.labels(
        service=base_service,
        account_id=base_account_id,
        symbol="unknown",
        symbol_tier=SymbolTier.UNKNOWN.value,
    ).set(0.0)

    return _METRICS


def metric_context(
    *,
    service: Optional[str] = None,
    account_id: Optional[str] = None,
    account_segment: Optional[AccountSegment] = None,
    symbol: Optional[str] = None,
    symbol_tier: Optional[SymbolTier] = None,
    transport: Optional[str | TransportType] = None,
) -> MetricContext:
    """Create a MetricContext with normalised bounded labels."""

    return _resolve_metric_context(
        service=service,
        account_id=account_id,
        account_segment=account_segment,
        symbol=symbol,
        symbol_tier=symbol_tier,
        transport=transport,
    )


@contextmanager
def bind_metric_context(
    **kwargs: object,
) -> Generator[MetricContext, None, None]:
    """Bind a metric context to the current request lifecycle."""

    kwargs = dict(kwargs)
    ctx = metric_context(**kwargs)  # type: ignore[arg-type]
    token = _REQUEST_CONTEXT.set(ctx)
    try:
        yield ctx
    finally:
        _REQUEST_CONTEXT.reset(token)


def current_metric_context() -> MetricContext:
    """Return the currently bound metric context."""

    return _REQUEST_CONTEXT.get()


def setup_metrics(app: FastAPI, service_name: str = "service") -> None:
    """Attach Prometheus /metrics endpoint and tracing middleware."""

    init_metrics(service_name)
    configure_observability(_SERVICE_NAME)

    if not any(
        getattr(middleware, "cls", None) is RequestTracingMiddleware
        for middleware in app.user_middleware
    ):
        app.add_middleware(RequestTracingMiddleware, service_name=_SERVICE_NAME)

    if not any(route.path == "/metrics" for route in app.routes):
        @app.get("/metrics")
        async def metrics_endpoint() -> Response:  # pragma: no cover - simple I/O
            payload = generate_latest(_REGISTRY)
            return Response(payload, media_type=CONTENT_TYPE_LATEST)


def increment_trades_submitted(
    amount: float = 1.0,
    *,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(service=service)
    _trades_submitted_total.labels(service=_service_value(ctx.service)).inc(amount)


def increment_trades_rejected(
    amount: float = 1.0,
    *,
    reason: str = "unknown",
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(service=service)
    _trades_rejected_total.labels(
        service=_service_value(ctx.service), reason=_normalised(reason, "unknown")
    ).inc(amount)


def increment_late_events(
    stream: str,
    amount: float = 1.0,
    *,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(service=service)
    _late_events_total.labels(
        service=_service_value(ctx.service), stream=_normalised(stream, "unknown")
    ).inc(amount)


def set_reorder_buffer_depth(
    stream: str, depth: int, *, service: Optional[str] = None
) -> None:
    ctx = _resolve_metric_context(service=service)
    _reorder_buffer_depth.labels(
        service=_service_value(ctx.service), stream=_normalised(stream, "unknown")
    ).set(depth)


def increment_trade_rejection(
    account_id: str,
    symbol: str,
    *,
    context: Optional[MetricContext] = None,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(
        context=context, service=service, account_id=account_id, symbol=symbol
    )
    _trade_rejections_total.labels(**_account_symbol_labels(ctx)).inc()
    increment_trades_rejected(service=ctx.service, reason="risk")


def increment_rejected_intents(
    stage: str,
    reason: str,
    *,
    service: Optional[str] = None,
) -> None:
    _rejected_intents_total.labels(
        service=_service_value(service),
        stage=_normalised(stage, "unknown"),
        reason=_normalised(reason, "unknown"),
    ).inc()


def increment_safe_mode_triggers(
    reason: str,
    *,
    service: Optional[str] = None,
) -> None:
    _safe_mode_triggers_total.labels(
        service=_service_value(service), reason=_normalised(reason, "unknown")
    ).inc()


def increment_oms_errors(
    *,
    context: Optional[MetricContext] = None,
    account: Optional[str] = None,
    symbol: Optional[str] = None,
    transport: Optional[str | TransportType] = None,
    amount: float = 1.0,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(
        context=context,
        service=service,
        account_id=account,
        symbol=symbol,
        transport=transport,
    )
    _oms_errors_total.labels(**_transport_labels(ctx)).inc(amount)


def increment_oms_error_count(
    account: str,
    symbol: str,
    transport: str,
    *,
    context: Optional[MetricContext] = None,
    service: Optional[str] = None,
) -> None:
    increment_oms_errors(
        context=context,
        account=account,
        symbol=symbol,
        transport=transport,
        service=service,
    )


def increment_oms_auth_failures(
    *, reason: str, service: Optional[str] = None
) -> None:
    ctx = _resolve_metric_context(service=service)
    _oms_auth_failures_total.labels(
        service=_service_value(ctx.service), reason=_normalised(reason, "unknown")
    ).inc()


def increment_oms_child_orders_total(
    account: str,
    symbol: str,
    transport: str,
    *,
    context: Optional[MetricContext] = None,
    service: Optional[str] = None,
    count: float = 1.0,
) -> None:
    ctx = _resolve_metric_context(
        context=context,
        service=service,
        account_id=account,
        symbol=symbol,
        transport=transport,
    )
    _oms_child_orders_total.labels(**_transport_labels(ctx)).inc(count)


def increment_oms_stale_feed(
    account: str,
    symbol: str,
    *,
    source: str,
    action: str,
    context: Optional[MetricContext] = None,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(
        context=context,
        service=service,
        account_id=account,
        symbol=symbol,
    )
    _oms_stale_feed_total.labels(
        **_account_symbol_labels(ctx),
        source=_normalised(source, "unknown"),
        action=_normalised(action, "unknown"),
    ).inc()


def set_oms_latency(
    latency_ms: float,
    *,
    context: Optional[MetricContext] = None,
    account: Optional[str] = None,
    symbol: Optional[str] = None,
    transport: Optional[str | TransportType] = None,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(
        context=context,
        service=service,
        account_id=account,
        symbol=symbol,
        transport=transport,
    )
    _oms_latency_ms.labels(**_transport_labels(ctx)).set(latency_ms)


def record_oms_latency(
    account: str,
    symbol: str,
    transport: str,
    latency_ms: float,
    *,
    context: Optional[MetricContext] = None,
    service: Optional[str] = None,
) -> None:
    set_oms_latency(
        latency_ms,
        context=context,
        account=account,
        symbol=symbol,
        transport=transport,
        service=service,
    )
    observe_oms_submit_latency(
        latency_ms,
        transport=transport,
        context=context,
        service=service,
    )


def record_ws_latency(
    account: str,
    symbol: str,
    latency_ms: float,
    *,
    transport: str = "websocket",
    context: Optional[MetricContext] = None,
    service: Optional[str] = None,
) -> None:
    """Record latency observations for Kraken acknowledgements by transport."""

    set_oms_latency(
        latency_ms,
        context=context,
        account=account,
        symbol=symbol,
        transport=transport,
        service=service,
    )


def set_pipeline_latency(latency_ms: float, *, service: Optional[str] = None) -> None:
    ctx = _resolve_metric_context(service=service)
    _pipeline_latency_ms.labels(service=_service_value(ctx.service)).set(latency_ms)


def observe_policy_inference_latency(
    latency_ms: float,
    *,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(service=service)
    _policy_inference_latency.labels(service=_service_value(ctx.service)).observe(
        latency_ms
    )


def observe_risk_validation_latency(
    latency_ms: float,
    *,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(service=service)
    _risk_validation_latency.labels(service=_service_value(ctx.service)).observe(
        latency_ms
    )


def observe_oms_submit_latency(
    latency_ms: float,
    *,
    context: Optional[MetricContext] = None,
    transport: Optional[str | TransportType] = None,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(
        context=context, service=service, transport=transport
    )
    _oms_submit_latency.labels(
        service=_service_value(ctx.service), transport=ctx.transport.value
    ).observe(latency_ms)


def record_oms_submit_ack(
    account: str,
    symbol: str,
    latency_ms: float,
    *,
    transport: str | None = None,
    context: Optional[MetricContext] = None,
    service: Optional[str] = None,
) -> None:
    """Record metrics for OMS submission acknowledgements."""

    resolved_transport = transport or "unknown"
    ctx = _resolve_metric_context(
        context=context,
        service=service,
        account_id=account,
        symbol=symbol,
        transport=resolved_transport,
    )
    set_oms_latency(
        latency_ms,
        context=ctx,
        service=ctx.service,
    )
    observe_oms_submit_latency(
        latency_ms,
        context=ctx,
        service=ctx.service,
    )


def record_abstention_rate(
    account_id: str,
    symbol: str,
    value: float,
    *,
    context: Optional[MetricContext] = None,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(
        context=context,
        service=service,
        account_id=account_id,
        symbol=symbol,
    )
    _policy_abstention_rate.labels(**_account_symbol_labels(ctx)).set(value)


def record_drift_score(
    account_id: str,
    symbol: str,
    value: float,
    *,
    context: Optional[MetricContext] = None,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(
        context=context,
        service=service,
        account_id=account_id,
        symbol=symbol,
    )
    _policy_drift_score.labels(**_account_symbol_labels(ctx)).set(value)


def record_fees_nav_pct(
    account_id: str,
    symbol: str,
    value: float,
    *,
    context: Optional[MetricContext] = None,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(
        context=context,
        service=service,
        account_id=account_id,
        symbol=symbol,
    )
    _fees_nav_pct.labels(**_account_symbol_labels(ctx)).set(value)


def record_account_daily_pnl(
    account_id: str,
    *,
    realized: object,
    unrealized: object,
    fees: object = 0.0,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(service=service, account_id=account_id)
    labels = {"service": _service_value(ctx.service), "account_id": ctx.account_id}
    realized_value = _coerce_float(realized)
    unrealized_value = _coerce_float(unrealized)
    fees_value = _coerce_float(fees)
    _account_daily_realized_pnl.labels(**labels).set(realized_value)
    _account_daily_unrealized_pnl.labels(**labels).set(unrealized_value)
    _account_daily_net_pnl.labels(**labels).set(
        realized_value + unrealized_value - fees_value
    )


def record_account_drawdown(
    account_id: str,
    drawdown_pct: object,
    *,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(service=service, account_id=account_id)
    labels = {"service": _service_value(ctx.service), "account_id": ctx.account_id}
    value = max(_coerce_float(drawdown_pct), 0.0)
    _account_drawdown_pct.labels(**labels).set(value)


def record_account_exposure(
    account_id: str,
    symbol: str,
    exposure_usd: object,
    *,
    service: Optional[str] = None,
) -> None:
    ctx = _resolve_metric_context(service=service, account_id=account_id, symbol=symbol)
    labels = {
        "service": _service_value(ctx.service),
        "account_id": ctx.account_id,
        "symbol": _normalised(symbol, "unknown"),
        "symbol_tier": ctx.symbol_tier.value,
    }
    _account_exposure_usd.labels(**labels).set(_coerce_float(exposure_usd))


def record_scaling_state(
    *,
    oms_replicas: int,
    gpu_nodes: int,
    pending_jobs: int,
    service: Optional[str] = None,
) -> None:
    labels = {"service": _service_value(service)}
    _scaling_oms_replicas.labels(**labels).set(max(oms_replicas, 0))
    _scaling_gpu_nodes.labels(**labels).set(max(gpu_nodes, 0))
    _scaling_pending_training_jobs.labels(**labels).set(max(pending_jobs, 0))


def observe_scaling_evaluation(
    duration_seconds: float, *, service: Optional[str] = None
) -> None:
    labels = {"service": _service_value(service)}
    _scaling_evaluation_duration_seconds.labels(**labels).observe(max(duration_seconds, 0.0))
    _scaling_evaluations_total.labels(**labels).inc()


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
        span.set_attribute("service.name", _SERVICE_NAME)
        ctx = current_metric_context()
        span.set_attribute("metric.account_id", ctx.account_id)
        span.set_attribute("metric.account_segment", ctx.account_segment.value)
        span.set_attribute("metric.symbol_tier", ctx.symbol_tier.value)
        span.set_attribute("metric.transport", ctx.transport.value)
        for key, value in attributes.items():
            span.set_attribute(key, value)
        yield span


__all__ = [
    "AccountSegment",
    "SymbolTier",
    "TransportType",
    "MetricContext",
    "init_metrics",
    "setup_metrics",
    "metric_context",
    "bind_metric_context",
    "current_metric_context",
    "increment_trades_submitted",
    "increment_trades_rejected",
    "increment_trade_rejection",
    "increment_rejected_intents",
    "increment_safe_mode_triggers",
    "increment_oms_errors",
    "increment_oms_error_count",
    "increment_oms_auth_failures",
    "increment_oms_child_orders_total",
    "increment_oms_stale_feed",
    "set_oms_latency",
    "record_oms_latency",
    "set_pipeline_latency",
    "observe_policy_inference_latency",
    "observe_risk_validation_latency",
    "observe_oms_submit_latency",
    "record_abstention_rate",
    "record_drift_score",
    "record_fees_nav_pct",
    "record_account_daily_pnl",
    "record_account_drawdown",
    "record_account_exposure",
    "record_scaling_state",
    "observe_scaling_evaluation",
    "get_request_id",
    "traced_span",
    "_REGISTRY",
]
