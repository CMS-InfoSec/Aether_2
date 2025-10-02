"""OpenTelemetry tracing helpers for the trading policy/risk/OMS endpoints."""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager, contextmanager, nullcontext
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional

from .latency_metrics import LatencyMetrics, LatencySnapshot, create_metrics_app

try:  # pragma: no cover - OpenTelemetry is optional in some environments
    from opentelemetry import trace
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.trace import Span, SpanKind
except Exception:  # pragma: no cover - degrade gracefully without OpenTelemetry
    trace = None  # type: ignore
    JaegerExporter = None  # type: ignore
    Resource = None  # type: ignore
    TracerProvider = None  # type: ignore
    BatchSpanProcessor = None  # type: ignore
    Span = object  # type: ignore
    SpanKind = None  # type: ignore

LOGGER = logging.getLogger(__name__)

__all__ = [
    "EndpointTracing",
    "LatencyMetrics",
    "LatencySnapshot",
    "create_metrics_app",
]


@dataclass(frozen=True)
class EndpointConfig:
    """Configuration describing a traced endpoint."""

    metric_name: str
    slo_threshold_ms: float


_ENDPOINTS: Dict[str, EndpointConfig] = {
    "policy": EndpointConfig("policy_latency_ms", 200.0),
    "risk": EndpointConfig("risk_latency_ms", 200.0),
    "oms": EndpointConfig("oms_latency_ms", 500.0),
}


_TRACER_CONFIGURED = False
_JAEGER_PROCESSOR_ATTACHED = False
_PROMETHEUS_EXPORTER_ATTACHED = False


def _normalise(value: Optional[str], *, default: str) -> str:
    if value and value.strip():
        return value.strip()
    return default


class EndpointTracing:
    """Trace Policy, Risk and OMS endpoint calls with latency metrics."""

    def __init__(
        self,
        *,
        service_name: str = "trading-orchestrator",
        metrics: Optional[LatencyMetrics] = None,
        jaeger_host: Optional[str] = None,
        jaeger_port: Optional[int] = None,
    ) -> None:
        self._service_name = _normalise(service_name, default="trading-orchestrator")
        self._metrics = metrics or LatencyMetrics()
        self._jaeger_host = jaeger_host
        self._jaeger_port = jaeger_port
        self._tracer = self._configure_tracer()
        self._span_kind = SpanKind.CLIENT if SpanKind is not None else None

    @property
    def metrics(self) -> LatencyMetrics:
        """Return the latency metrics registry used by the tracer."""

        return self._metrics

    def _configure_tracer(self) -> Optional[Any]:
        global _TRACER_CONFIGURED, _JAEGER_PROCESSOR_ATTACHED

        if trace is None or TracerProvider is None:
            LOGGER.debug("OpenTelemetry SDK not available; spans disabled")
            return None

        provider = trace.get_tracer_provider()
        if not isinstance(provider, TracerProvider):
            resource = None
            if Resource is not None:
                resource = Resource.create({"service.name": self._service_name})
            provider = TracerProvider(resource=resource)
            trace.set_tracer_provider(provider)
            _TRACER_CONFIGURED = True
        elif not _TRACER_CONFIGURED and Resource is not None:
            # Replace the provider with one that includes the service resource.
            resource = Resource.create({"service.name": self._service_name})
            provider = TracerProvider(resource=resource)
            trace.set_tracer_provider(provider)
            _TRACER_CONFIGURED = True

        tracer = trace.get_tracer(self._service_name)

        if BatchSpanProcessor is None:
            return tracer

        if JaegerExporter is not None and not _JAEGER_PROCESSOR_ATTACHED:
            host = self._jaeger_host or os.getenv("JAEGER_AGENT_HOST", "localhost")
            port_value: Any = self._jaeger_port or os.getenv("JAEGER_AGENT_PORT", "6831")
            try:
                port = int(port_value)
            except (TypeError, ValueError):  # pragma: no cover - defensive guard
                port = 6831
            try:
                exporter = JaegerExporter(agent_host_name=host, agent_port=port)
            except Exception as exc:  # pragma: no cover - log but keep tracing alive
                LOGGER.warning("Unable to configure Jaeger exporter: %s", exc)
            else:
                provider.add_span_processor(BatchSpanProcessor(exporter))
                _JAEGER_PROCESSOR_ATTACHED = True
        elif JaegerExporter is None:
            LOGGER.debug("Jaeger exporter unavailable; spans will remain local")

        global _PROMETHEUS_EXPORTER_ATTACHED
        if not _PROMETHEUS_EXPORTER_ATTACHED:
            provider.add_span_processor(
                BatchSpanProcessor(_PrometheusSpanExporter(self._metrics))
            )
            _PROMETHEUS_EXPORTER_ATTACHED = True

        return tracer

    def trace_policy(
        self,
        symbol: str,
        account_id: str,
        *,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Optional[Span]]:
        """Context manager recording latency and tracing for policy calls."""

        return self._trace_endpoint("policy", symbol, account_id, attributes=attributes)

    def trace_risk(
        self,
        symbol: str,
        account_id: str,
        *,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Optional[Span]]:
        """Context manager recording latency and tracing for risk validation."""

        return self._trace_endpoint("risk", symbol, account_id, attributes=attributes)

    def trace_oms(
        self,
        symbol: str,
        account_id: str,
        *,
        transport: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Optional[Span]]:
        """Context manager recording latency and tracing for OMS submission."""

        attrs = dict(attributes or {})
        if transport:
            attrs.setdefault("oms.transport", transport)
        return self._trace_endpoint("oms", symbol, account_id, attributes=attrs)

    @asynccontextmanager
    async def trace_policy_async(
        self,
        symbol: str,
        account_id: str,
        *,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Optional[Span]]:
        with self.trace_policy(symbol, account_id, attributes=attributes) as span:
            yield span

    @asynccontextmanager
    async def trace_risk_async(
        self,
        symbol: str,
        account_id: str,
        *,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Optional[Span]]:
        with self.trace_risk(symbol, account_id, attributes=attributes) as span:
            yield span

    @asynccontextmanager
    async def trace_oms_async(
        self,
        symbol: str,
        account_id: str,
        *,
        transport: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Optional[Span]]:
        with self.trace_oms(symbol, account_id, transport=transport, attributes=attributes) as span:
            yield span

    def _trace_endpoint(
        self,
        endpoint: str,
        symbol: str,
        account_id: str,
        *,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Optional[Span]]:
        if endpoint not in _ENDPOINTS:
            raise ValueError(f"Unknown endpoint '{endpoint}'")

        config = _ENDPOINTS[endpoint]
        metric_name = config.metric_name
        attributes = dict(attributes or {})

        symbol_value = _normalise(symbol, default="unknown")
        account_value = _normalise(account_id, default="unknown")

        start = time.perf_counter()
        span_cm = (
            self._tracer.start_as_current_span(
                f"{endpoint}.endpoint",
                kind=self._span_kind,
            )
            if getattr(self, "_tracer", None)
            else nullcontext(None)
        )

        @contextmanager
        def _wrapped() -> Iterator[Optional[Span]]:
            with span_cm as span:  # type: ignore[assignment]
                if span is not None:
                    span.set_attribute("service.name", self._service_name)
                    span.set_attribute("trading.endpoint", endpoint)
                    span.set_attribute("trading.symbol", symbol_value)
                    span.set_attribute("trading.account_id", account_value)
                    span.set_attribute("metrics.recorded", False)
                    for key, value in attributes.items():
                        span.set_attribute(str(key), value)
                try:
                    yield span
                except Exception as exc:
                    if span is not None:
                        span.record_exception(exc)
                    raise
                finally:
                    latency_ms = (time.perf_counter() - start) * 1000.0
                    snapshot = self._metrics.observe(
                        metric_name,
                        symbol=symbol_value,
                        account_id=account_value,
                        latency_ms=latency_ms,
                    )
                    if span is not None:
                        span.set_attribute("latency.ms", latency_ms)
                        span.set_attribute("latency.p50", snapshot.p50)
                        span.set_attribute("latency.p95", snapshot.p95)
                        span.set_attribute("latency.p99", snapshot.p99)
                        span.set_attribute("metrics.recorded", True)
                    if snapshot.p95 > config.slo_threshold_ms:
                        if span is not None:
                            span.add_event(
                                "latency.threshold_exceeded",
                                {
                                    "metric": metric_name,
                                    "p95": snapshot.p95,
                                    "threshold": config.slo_threshold_ms,
                                },
                            )
                        LOGGER.warning(
                            "%s latency p95 breach symbol=%s account=%s p95=%.2f threshold=%.2f",
                            endpoint,
                            symbol_value,
                            account_value,
                            snapshot.p95,
                            config.slo_threshold_ms,
                        )

        return _wrapped()


class _PrometheusSpanExporter:  # pragma: no cover - simple exporter glue
    """Export span durations into Prometheus latency histograms."""

    def __init__(self, metrics: LatencyMetrics) -> None:
        self._metrics = metrics

    def export(self, spans: Any) -> Any:  # type: ignore[override]
        if BatchSpanProcessor is None:  # pragma: no cover - defensive guard
            return True
        result = True
        for span in spans:
            try:
                attributes = getattr(span, "attributes", {}) or {}
                endpoint = attributes.get("trading.endpoint")
                symbol = attributes.get("trading.symbol", "unknown")
                account_id = attributes.get("trading.account_id", "unknown")
                if attributes.get("metrics.recorded"):
                    continue
                if endpoint not in _ENDPOINTS:
                    continue
                config = _ENDPOINTS[endpoint]
                start_time = getattr(span, "start_time", None)
                end_time = getattr(span, "end_time", None)
                if start_time is None or end_time is None:
                    continue
                latency_ms = max((end_time - start_time) / 1_000_000, 0.0)
                self._metrics.observe(
                    config.metric_name,
                    symbol=str(symbol),
                    account_id=str(account_id),
                    latency_ms=latency_ms,
                )
            except Exception:
                continue
        return result

    def shutdown(self) -> None:
        return None

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True
