"""Request latency instrumentation for FastAPI services.

This module provides a reusable ``LatencyProfiler`` that instruments FastAPI
applications with per-endpoint latency tracking.  It records latency
observations in Prometheus histograms, maintains in-memory percentile
summaries, persists aggregated statistics to PostgreSQL, and exposes a
standard ``/observability/latency`` endpoint for surfacing the latest
snapshot.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Deque, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence

try:  # pragma: no cover - prefer real FastAPI when available
    from fastapi import FastAPI, Request
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import FastAPI, Request

try:  # pragma: no cover - prefer real FastAPI responses
    from fastapi.responses import JSONResponse
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import JSONResponse
from metrics import CollectorRegistry, Histogram, _REGISTRY as _METRICS_REGISTRY
from sqlalchemy import Column, DateTime, Float, MetaData, String, Table, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker


logger = logging.getLogger(__name__)

_DEFAULT_BUCKETS: Sequence[float] = (
    1,
    5,
    10,
    25,
    50,
    75,
    100,
    150,
    250,
    500,
    750,
    1000,
    2000,
    5000,
    10000,
)


@dataclass(slots=True)
class PercentileSnapshot:
    """Percentile summary of request latency for an endpoint."""

    p50: float
    p95: float
    p99: float
    count: int
    ts: datetime

    def as_dict(self) -> Dict[str, float | int | str]:
        return {
            "p50": self.p50,
            "p95": self.p95,
            "p99": self.p99,
            "count": self.count,
            "ts": self.ts.isoformat(),
        }


class LatencyProfiler:
    """Capture and persist latency percentile distributions per endpoint."""

    def __init__(
        self,
        service_name: str,
        *,
        database_url: Optional[str] = None,
        registry: Optional[CollectorRegistry] = None,
        max_samples_per_endpoint: int = 1024,
        histogram_buckets: Sequence[float] = _DEFAULT_BUCKETS,
    ) -> None:
        self.service_name = service_name
        self._registry = registry or _METRICS_REGISTRY
        metric_kwargs = {"registry": self._registry}
        self._histogram = Histogram(
            "latency_ms",
            "HTTP request latency in milliseconds.",
            ("service", "endpoint"),
            buckets=tuple(histogram_buckets),
            **metric_kwargs,
        )

        self._max_samples = max_samples_per_endpoint
        self._latency_windows: MutableMapping[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=self._max_samples)
        )
        self._snapshots: Dict[str, PercentileSnapshot] = {}
        self._lock = asyncio.Lock()

        db_url = database_url or os.getenv("LATENCY_DATABASE_URL") or os.getenv(
            "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
        )
        self._engine: Engine = create_engine(
            db_url,
            pool_pre_ping=True,
            future=True,
        )
        self._Session = sessionmaker(bind=self._engine, expire_on_commit=False, future=True)

        metadata = MetaData()
        self._table = Table(
            "latency_metrics",
            metadata,
            Column("service", String, nullable=False, index=True),
            Column("endpoint", String, nullable=False, index=True),
            Column("p50", Float, nullable=False),
            Column("p95", Float, nullable=False),
            Column("p99", Float, nullable=False),
            Column("ts", DateTime(timezone=True), nullable=False, index=True),
        )
        metadata.create_all(self._engine, checkfirst=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def observe(self, endpoint: str, latency_ms: float) -> None:
        """Record a latency sample for an endpoint."""

        if latency_ms < 0:
            return

        self._histogram.labels(service=self.service_name, endpoint=endpoint).observe(
            latency_ms
        )

        now = datetime.now(timezone.utc)
        async with self._lock:
            window = self._latency_windows[endpoint]
            window.append(latency_ms)
            snapshot = PercentileSnapshot(
                p50=_percentile(window, 50),
                p95=_percentile(window, 95),
                p99=_percentile(window, 99),
                count=len(window),
                ts=now,
            )
            self._snapshots[endpoint] = snapshot

        await self._persist_snapshot(endpoint, snapshot)

    async def snapshot(self) -> Dict[str, Dict[str, float | int | str]]:
        """Return the latest latency distribution snapshot per endpoint."""

        async with self._lock:
            return {endpoint: snap.as_dict() for endpoint, snap in self._snapshots.items()}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    async def _persist_snapshot(self, endpoint: str, snapshot: PercentileSnapshot) -> None:
        """Persist the percentile snapshot for the endpoint to PostgreSQL."""

        def _write() -> None:
            try:
                with self._Session() as session:  # type: Session
                    session.execute(
                        self._table.insert(),
                        [
                            {
                                "service": self.service_name,
                                "endpoint": endpoint,
                                "p50": snapshot.p50,
                                "p95": snapshot.p95,
                                "p99": snapshot.p99,
                                "ts": snapshot.ts,
                            }
                        ],
                    )
                    session.commit()
            except SQLAlchemyError:
                logger.exception("Failed to persist latency snapshot for %s", endpoint)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _write)


def setup_latency_profiler(
    app: FastAPI,
    service_name: str,
    *,
    database_url: Optional[str] = None,
    registry: Optional[CollectorRegistry] = None,
    max_samples_per_endpoint: int = 1024,
    histogram_buckets: Sequence[float] = _DEFAULT_BUCKETS,
) -> LatencyProfiler:
    """Attach latency profiling middleware and observability endpoint."""

    profiler = LatencyProfiler(
        service_name,
        database_url=database_url,
        registry=registry,
        max_samples_per_endpoint=max_samples_per_endpoint,
        histogram_buckets=histogram_buckets,
    )

    @app.middleware("http")
    async def _middleware(request: Request, call_next):  # type: ignore[override]
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:  # pragma: no cover - simple exception path
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            status_code = getattr(exc, "status_code", 500)
            endpoint = _resolve_endpoint(request.scope, status_code=status_code)
            await profiler.observe(endpoint, elapsed_ms)
            raise
        else:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            status_code = getattr(response, "status_code", None)
            endpoint = _resolve_endpoint(request.scope, status_code=status_code)
            await profiler.observe(endpoint, elapsed_ms)
            return response

    @app.get("/observability/latency")
    async def _latency_endpoint() -> JSONResponse:  # pragma: no cover - simple I/O
        payload = {
            "service": service_name,
            "endpoints": await profiler.snapshot(),
        }
        return JSONResponse(payload)

    app.state.latency_profiler = profiler
    return profiler


def _resolve_endpoint(scope: Mapping[str, object], *, status_code: Optional[int]) -> str:
    """Derive a stable endpoint label for metrics/percentiles."""

    if scope.get("type") != "http":
        return "unknown"

    route = scope.get("route")
    if route is not None and hasattr(route, "path"):
        return str(getattr(route, "path"))

    path = scope.get("path") or scope.get("raw_path")
    if isinstance(path, (bytes, bytearray)):
        try:
            return path.decode("utf-8")
        except UnicodeDecodeError:
            return "<binary-path>"
    if isinstance(path, str):
        return path

    method = scope.get("method", "?")
    status_repr = status_code if status_code is not None else "?"
    return f"{method} unknown ({status_repr})"


def _percentile(samples: Iterable[float], percentile: float) -> float:
    """Compute a percentile value from the provided samples."""

    values = sorted(float(v) for v in samples)
    if not values:
        return float("nan")

    if percentile <= 0:
        return values[0]
    if percentile >= 100:
        return values[-1]

    k = (len(values) - 1) * (percentile / 100.0)
    lower_index = int(k)
    upper_index = min(lower_index + 1, len(values) - 1)
    lower_value = values[lower_index]
    upper_value = values[upper_index]
    interpolation = k - lower_index
    return lower_value + (upper_value - lower_value) * interpolation


__all__ = ["LatencyProfiler", "setup_latency_profiler"]
