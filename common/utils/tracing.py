"""Tracing helpers for correlation identifiers and OpenTelemetry instrumentation."""
from __future__ import annotations

import inspect
import logging
import os
import time
from contextlib import contextmanager, nullcontext
from functools import wraps
from typing import Any, Awaitable, Callable, Dict, Iterator, Mapping, MutableMapping, Optional, TypeVar, cast
from uuid import uuid4

from shared.correlation import CorrelationContext, get_correlation_id

try:  # pragma: no cover - OpenTelemetry may be optional
    from opentelemetry import trace
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,
        ConsoleSpanExporter,
        ReadableSpan,
        SpanExporter,
        SpanExportResult,
    )
    from opentelemetry.trace import Span, SpanKind
except Exception:  # pragma: no cover - gracefully degrade when OTel unavailable
    trace = None  # type: ignore
    JaegerExporter = None  # type: ignore
    TracerProvider = None  # type: ignore
    BatchSpanProcessor = None  # type: ignore
    ConsoleSpanExporter = None  # type: ignore
    ReadableSpan = Any  # type: ignore
    SpanExporter = object  # type: ignore
    SpanExportResult = None  # type: ignore
    Span = object  # type: ignore
    SpanKind = None  # type: ignore

_HISTOGRAM_REGISTRY: Any | None = None

try:  # pragma: no cover - prometheus client may be optional
    from prometheus_client import Histogram as _PrometheusHistogram
except Exception:  # pragma: no cover - gracefully degrade when the library is absent
    _PrometheusHistogram = None  # type: ignore[assignment]

if _PrometheusHistogram is not None:
    Histogram = _PrometheusHistogram
else:  # pragma: no cover - exercised in optional dependency environments
    try:
        import metrics as _metrics_module
    except Exception:  # pragma: no cover - fallback metrics unavailable
        Histogram = None  # type: ignore[assignment]
    else:
        Histogram = getattr(_metrics_module, "Histogram", None)
        _HISTOGRAM_REGISTRY = getattr(_metrics_module, "_REGISTRY", None)

try:  # pragma: no cover - FastAPI/Starlette may be optional
    from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
    from starlette.requests import Request
    from starlette.responses import Response
except Exception:  # pragma: no cover - ensure module import does not fail
    BaseHTTPMiddleware = cast(Any, object)
    RequestResponseEndpoint = Callable[..., Awaitable[Any]]  # type: ignore
    Request = Any  # type: ignore
    Response = Any  # type: ignore

LOGGER = logging.getLogger(__name__)

_SERVICE_NAME = "service"
_TRACING_INITIALISED = False
_LOG_FILTER_INSTALLED = False

R = TypeVar("R")

_STAGE_LATENCY_HISTOGRAM = None
if Histogram is not None:
    kwargs: Dict[str, Any] = {}
    if _HISTOGRAM_REGISTRY is not None:
        kwargs["registry"] = _HISTOGRAM_REGISTRY
    _STAGE_LATENCY_HISTOGRAM = Histogram(
        "trading_stage_latency_seconds",
        "Latency of trading pipeline stages in seconds.",
        ["stage"],
        **kwargs,
    )


class _CorrelationIdFilter(logging.Filter):
    """Logging filter that injects correlation IDs on every record."""

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - exercised implicitly
        record.correlation_id = get_correlation_id() or "-"
        return True


class _PrometheusSpanExporter(SpanExporter):
    """Export span latency metrics into Prometheus histograms."""

    def export(self, spans: Any) -> Any:  # type: ignore[override]
        if SpanExportResult is None or _STAGE_LATENCY_HISTOGRAM is None:
            return getattr(SpanExportResult, "SUCCESS", True) if SpanExportResult else True

        for span in spans:  # pragma: no branch - minimal iteration
            try:
                attributes = getattr(span, "attributes", {}) or {}
                stage = attributes.get("trading.stage") or attributes.get("stage")
                if not stage:
                    continue
                start_time = getattr(span, "start_time", None)
                end_time = getattr(span, "end_time", None)
                if start_time is None or end_time is None:
                    continue
                duration_seconds = max((end_time - start_time) / 1_000_000_000, 0.0)
                _STAGE_LATENCY_HISTOGRAM.labels(stage=str(stage)).observe(duration_seconds)
            except Exception:  # pragma: no cover - defensive guard
                continue

        return getattr(SpanExportResult, "SUCCESS", True)

    def shutdown(self) -> None:  # pragma: no cover - no-op for in-memory exporter
        return None

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # pragma: no cover - eager success
        return True


def _install_logging_filter() -> None:
    global _LOG_FILTER_INSTALLED
    if _LOG_FILTER_INSTALLED:
        return
    logging.getLogger().addFilter(_CorrelationIdFilter())
    _LOG_FILTER_INSTALLED = True


def init_tracing(
    service_name: str,
    *,
    jaeger_host: Optional[str] = None,
    jaeger_port: Optional[int] = None,
    enable_console_exporter: bool | None = None,
) -> None:
    """Initialise OpenTelemetry tracing for the provided service.

    The function is idempotent and safe to call multiple times. When OpenTelemetry
    (or its exporters) are not available the function degrades gracefully while still
    installing correlation ID log filters.
    """

    global _SERVICE_NAME, _TRACING_INITIALISED

    _SERVICE_NAME = service_name or _SERVICE_NAME
    _install_logging_filter()

    if trace is None or TracerProvider is None or BatchSpanProcessor is None:
        LOGGER.debug("OpenTelemetry SDK not available; skipping tracer initialisation")
        return

    if _TRACING_INITIALISED:
        return

    resource = Resource.create({"service.name": _SERVICE_NAME})
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    jaeger_enabled = False
    if JaegerExporter is not None:
        host = jaeger_host or os.getenv("JAEGER_AGENT_HOST", "localhost")
        port_value = jaeger_port or os.getenv("JAEGER_AGENT_PORT", "6831")
        try:
            port = int(port_value) if not isinstance(port_value, int) else port_value
        except (TypeError, ValueError):  # pragma: no cover - defensive guard
            port = 6831
        try:
            exporter = JaegerExporter(agent_host_name=host, agent_port=port)
        except Exception as exc:  # pragma: no cover - failure logged for visibility
            LOGGER.warning("Unable to configure Jaeger exporter: %s", exc)
        else:
            provider.add_span_processor(BatchSpanProcessor(exporter))
            jaeger_enabled = True
    else:  # pragma: no cover - optional dependency missing
        LOGGER.debug("Jaeger exporter not available; spans will remain local")

    if _STAGE_LATENCY_HISTOGRAM is not None:
        provider.add_span_processor(BatchSpanProcessor(_PrometheusSpanExporter()))

    console_requested = enable_console_exporter is True or (
        enable_console_exporter is None and ConsoleSpanExporter is not None
    )
    if console_requested and ConsoleSpanExporter is not None:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    LOGGER.info(
        "Tracing initialised for service=%s jaeger_enabled=%s", _SERVICE_NAME, jaeger_enabled
    )
    _TRACING_INITIALISED = True


def current_correlation_id(default: Optional[str] = None) -> Optional[str]:
    """Return the active correlation identifier, if bound to the context."""

    value = get_correlation_id()
    if value:
        return value
    return default


def generate_correlation_id() -> str:
    """Return a brand new correlation identifier."""

    return str(uuid4())


@contextmanager
def correlation_scope(correlation_id: Optional[str] = None) -> Iterator[str]:
    """Bind *correlation_id* to the current context, generating one if needed."""

    candidate = (correlation_id or get_correlation_id() or "").strip()
    if not candidate:
        candidate = generate_correlation_id()
    with CorrelationContext(candidate) as bound:
        yield bound


def attach_correlation(
    payload: Mapping[str, Any],
    *,
    correlation_id: Optional[str] = None,
    mutate: bool = False,
) -> Dict[str, Any]:
    """Return *payload* with a correlation identifier attached.

    When ``mutate`` is ``True`` the incoming mapping is updated in-place (when
    mutable), otherwise a shallow copy is returned.
    """

    corr = correlation_id or current_correlation_id()
    if not corr:
        corr = generate_correlation_id()
    if corr:
        if mutate and isinstance(payload, MutableMapping):
            payload["correlation_id"] = corr
            return payload  # type: ignore[return-value]
        enriched = dict(payload)
        enriched["correlation_id"] = corr
        return enriched
    if mutate and isinstance(payload, MutableMapping):
        return payload  # type: ignore[return-value]
    return dict(payload)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """FastAPI/Starlette middleware that manages correlation identifiers."""

    def __init__(
        self,
        app: Any,
        *,
        header_name: str = "x-correlation-id",
        response_header: Optional[str] = None,
    ) -> None:
        if BaseHTTPMiddleware is object:  # type: ignore[comparison-overlap]
            raise RuntimeError(
                "CorrelationIdMiddleware requires starlette.middleware.base.BaseHTTPMiddleware"
            )
        super().__init__(app)
        self._header_name = header_name
        self._response_header = response_header or header_name

    async def dispatch(
        self,
        request: "Request",
        call_next: "RequestResponseEndpoint",
    ) -> "Response":  # pragma: no cover - exercised via FastAPI integration tests
        incoming = None
        if hasattr(request, "headers"):
            try:
                incoming = request.headers.get(self._header_name)
            except Exception:  # pragma: no cover - defensive guard for exotic headers
                incoming = None

        with correlation_scope(incoming) as correlation_id:
            if hasattr(request, "state"):
                setattr(request.state, "correlation_id", correlation_id)

            response = await call_next(request)
            if hasattr(response, "headers"):
                response.headers[self._response_header] = correlation_id
            return response


def _observe_stage_latency(stage: str, duration_seconds: float) -> None:
    if _STAGE_LATENCY_HISTOGRAM is None:
        return
    _STAGE_LATENCY_HISTOGRAM.labels(stage=stage).observe(duration_seconds)


@contextmanager
def stage_span(
    stage: str,
    *,
    correlation_id: Optional[str] = None,
    span_name: Optional[str] = None,
    span_kind: Optional["SpanKind"] = None,
    intent: Optional[Mapping[str, Any]] = None,
    **attributes: Any,
) -> Iterator[Optional["Span"]]:
    """Create a tracing span for a pipeline stage.

    The span automatically records the stage name, correlation identifier and intent
    metadata when present. The elapsed time is also recorded in Prometheus when
    available, even if OpenTelemetry is absent.
    """

    stage_name = stage.strip() or "stage"
    normalized_stage = stage_name.lower()
    span_label = span_name or f"{_SERVICE_NAME}.{normalized_stage}"

    with correlation_scope(correlation_id) as corr:
        start_time = time.perf_counter()
        tracer = trace.get_tracer(_SERVICE_NAME) if trace is not None else None
        cm = (
            tracer.start_as_current_span(span_label, kind=span_kind or SpanKind.INTERNAL)
            if tracer is not None and SpanKind is not None
            else nullcontext(None)
        )
        with cm as span:  # type: ignore[assignment]
            if span is not None:
                span.set_attribute("service.name", _SERVICE_NAME)
                span.set_attribute("trading.stage", normalized_stage)
                span.set_attribute("trading.correlation_id", corr)
                span.set_attribute("trading.span_name", span_label)
                if intent:
                    account = intent.get("account_id") or intent.get("account")
                    symbol = intent.get("symbol") or intent.get("instrument")
                    if account:
                        span.set_attribute("intent.account_id", str(account))
                    if symbol:
                        span.set_attribute("intent.symbol", str(symbol))
                for key, value in attributes.items():
                    span.set_attribute(key, value)
            yield span
        duration_seconds = max(time.perf_counter() - start_time, 0.0)
        _observe_stage_latency(normalized_stage, duration_seconds)


@contextmanager
def policy_span(**attributes: Any) -> Iterator[Optional["Span"]]:
    """Convenience wrapper for ``stage_span`` with the ``policy`` label."""

    attributes.setdefault("span_name", "policy_decision")
    with stage_span("policy", **attributes) as span:
        yield span


@contextmanager
def risk_span(**attributes: Any) -> Iterator[Optional["Span"]]:
    """Convenience wrapper for ``stage_span`` with the ``risk`` label."""

    attributes.setdefault("span_name", "risk_validation")
    with stage_span("risk", **attributes) as span:
        yield span


@contextmanager
def oms_span(**attributes: Any) -> Iterator[Optional["Span"]]:
    """Convenience wrapper for ``stage_span`` with the ``oms`` label."""

    attributes.setdefault("span_name", "oms_execution")
    with stage_span("oms", **attributes) as span:
        yield span


@contextmanager
def fill_span(**attributes: Any) -> Iterator[Optional["Span"]]:
    """Convenience wrapper for ``stage_span`` with the ``fill`` label."""

    attributes.setdefault("span_name", "fill_event")
    with stage_span("fill", **attributes) as span:
        yield span


def _extract_request(args: tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
    """Return a FastAPI/Starlette request object when present in call arguments."""

    for value in list(args) + list(kwargs.values()):
        if value is None:
            continue
        if hasattr(value, "headers") and hasattr(value, "state"):
            return value
    return None


def _request_correlation_hint(request: Any) -> Optional[str]:
    if request is None:
        return None
    headers = getattr(request, "headers", None)
    if headers is None:
        return None
    for key in ("x-correlation-id", "X-Correlation-ID"):
        if key in headers:
            value = headers[key]
            if value:
                return str(value)
    return None


def _enrich_result(result: R, correlation_id: str) -> R:
    if isinstance(result, MutableMapping):
        attach_correlation(result, correlation_id=correlation_id, mutate=True)
    return result


def trace_request(func: Callable[..., R] | Callable[..., Awaitable[R]]) -> Callable[..., Awaitable[R] | R]:
    """Wrap a FastAPI endpoint ensuring correlation IDs propagate to handlers."""

    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> R:
            request = _extract_request(args, kwargs)
            hint = _request_correlation_hint(request)
            with correlation_scope(hint) as corr:
                if request is not None and hasattr(request, "state"):
                    setattr(request.state, "correlation_id", corr)
                result = await cast(Callable[..., Awaitable[R]], func)(*args, **kwargs)
                return _enrich_result(result, corr)

        return async_wrapper

    @wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> R:
        request = _extract_request(args, kwargs)
        hint = _request_correlation_hint(request)
        with correlation_scope(hint) as corr:
            if request is not None and hasattr(request, "state"):
                setattr(request.state, "correlation_id", corr)
            result = cast(Callable[..., R], func)(*args, **kwargs)
            return _enrich_result(result, corr)

    return sync_wrapper


__all__ = [
    "CorrelationIdMiddleware",
    "attach_correlation",
    "correlation_scope",
    "current_correlation_id",
    "generate_correlation_id",
    "fill_span",
    "init_tracing",
    "oms_span",
    "policy_span",
    "risk_span",
    "trace_request",
    "stage_span",
]
