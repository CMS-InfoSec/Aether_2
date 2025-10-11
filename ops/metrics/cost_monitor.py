"""Cost monitoring service exposing Prometheus metrics and JSON status."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, Iterable, Mapping, MutableMapping, Optional, Tuple

import httpx
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Gauge,
    generate_latest,
)
from shared.health import setup_health_checks

try:  # pragma: no cover - kubernetes client optional for tests
    from kubernetes import client, config
    from kubernetes.client import CustomObjectsApi
except Exception:  # pragma: no cover - allow operation without kubernetes client
    client = None  # type: ignore
    config = None  # type: ignore
    CustomObjectsApi = None  # type: ignore


LOGGER = logging.getLogger(__name__)

__all__ = ["CostMonitorConfig", "CostMonitor", "create_app", "main"]


@dataclass
class ResourceUsage:
    """Snapshot of resource usage aggregated per service."""

    cpu_cores: float = 0.0
    memory_bytes: float = 0.0
    gpu: float = 0.0
    pods: int = 0

    def copy(self) -> "ResourceUsage":
        return ResourceUsage(
            cpu_cores=self.cpu_cores,
            memory_bytes=self.memory_bytes,
            gpu=self.gpu,
            pods=self.pods,
        )

    @property
    def memory_gb(self) -> float:
        return self.memory_bytes / float(1024 ** 3)


@dataclass
class TradeStats:
    """Rolling trading activity metrics used to compute KPIs."""

    trades_24h: float = 0.0
    pnl_24h: float = 0.0


@dataclass
class CostMonitorConfig:
    """Configuration for the cost monitor service."""

    namespace: Optional[str] = field(default=None)
    service_label: str = field(default="app.kubernetes.io/name")
    cpu_usd_per_hour: float = field(default=0.04)
    mem_usd_per_gb_hour: float = field(default=0.0045)
    gpu_usd_per_hour: float = field(default=2.5)
    sample_interval_seconds: float = field(default=30.0)
    rolling_window_hours: float = field(default=24.0)
    cost_per_pnl_alert_threshold: float = field(default=5.0)
    trade_stats_url: Optional[str] = field(default=None)
    trade_stats_path: Optional[str] = field(default=None)
    gpu_service_name: str = field(default="gpu-shared")
    http_timeout_seconds: float = field(default=3.0)

    @classmethod
    def from_env(cls) -> "CostMonitorConfig":
        def _env_float(name: str, default: float) -> float:
            value = os.getenv(name)
            if value is None:
                return default
            try:
                return float(value)
            except ValueError:
                LOGGER.warning("Invalid float for %s: %s", name, value)
                return default

        def _env_str(name: str, default: Optional[str]) -> Optional[str]:
            value = os.getenv(name)
            if value is None or value.strip() == "":
                return default
            return value

        config = cls()
        config.namespace = _env_str("COST_MONITOR_NAMESPACE", config.namespace)
        config.service_label = os.getenv(
            "COST_MONITOR_SERVICE_LABEL", config.service_label
        )
        config.cpu_usd_per_hour = _env_float(
            "COST_MONITOR_CPU_USD_PER_HOUR", config.cpu_usd_per_hour
        )
        config.mem_usd_per_gb_hour = _env_float(
            "COST_MONITOR_MEM_USD_PER_GB_HOUR", config.mem_usd_per_gb_hour
        )
        config.gpu_usd_per_hour = _env_float(
            "COST_MONITOR_GPU_USD_PER_HOUR", config.gpu_usd_per_hour
        )
        config.sample_interval_seconds = _env_float(
            "COST_MONITOR_SAMPLE_INTERVAL", config.sample_interval_seconds
        )
        config.rolling_window_hours = _env_float(
            "COST_MONITOR_ROLLING_WINDOW", config.rolling_window_hours
        )
        config.cost_per_pnl_alert_threshold = _env_float(
            "COST_MONITOR_COST_PER_PNL_THRESHOLD",
            config.cost_per_pnl_alert_threshold,
        )
        config.trade_stats_url = _env_str(
            "COST_MONITOR_TRADE_STATS_URL", config.trade_stats_url
        )
        config.trade_stats_path = _env_str(
            "COST_MONITOR_TRADE_STATS_PATH", config.trade_stats_path
        )
        config.gpu_service_name = os.getenv(
            "COST_MONITOR_GPU_SERVICE_NAME", config.gpu_service_name
        )
        config.http_timeout_seconds = _env_float(
            "COST_MONITOR_HTTP_TIMEOUT", config.http_timeout_seconds
        )
        return config


class CostMonitor:
    """Periodically sample Kubernetes metrics to estimate infrastructure cost."""

    _CPU_UNITS = {
        "n": 1e-9,
        "u": 1e-6,
        "m": 1e-3,
    }

    _MEMORY_UNITS = {
        "Ki": 1024,
        "Mi": 1024 ** 2,
        "Gi": 1024 ** 3,
        "Ti": 1024 ** 4,
        "Pi": 1024 ** 5,
        "Ei": 1024 ** 6,
        "K": 1000,
        "M": 1000 ** 2,
        "G": 1000 ** 3,
        "T": 1000 ** 4,
        "P": 1000 ** 5,
        "E": 1000 ** 6,
    }

    def __init__(
        self,
        config: Optional[CostMonitorConfig] = None,
        *,
        registry: Optional[CollectorRegistry] = None,
    ) -> None:
        self._config = config or CostMonitorConfig.from_env()
        self._registry = registry or CollectorRegistry()
        self._cost_total = Gauge(
            "aether_cost_usd_total",
            "Estimated cumulative infrastructure cost by service (USD).",
            ["service"],
            registry=self._registry,
        )
        self._cost_per_trade = Gauge(
            "aether_cost_per_trade",
            "Rolling 24h cost per trade in USD.",
            registry=self._registry,
        )
        self._cost_per_pnl = Gauge(
            "aether_cost_per_pnl",
            "Rolling 24h cost per dollar of PnL.",
            registry=self._registry,
        )
        self._lock = threading.Lock()
        self._service_cost_totals: Dict[str, float] = defaultdict(float)
        self._service_hourly_cost: Dict[str, float] = defaultdict(float)
        self._latest_usage: Dict[str, ResourceUsage] = {}
        self._cost_window: Deque[Tuple[datetime, float]] = deque()
        self._last_sample_time: Optional[datetime] = None
        self._last_trade_stats = TradeStats()
        self._cost_per_trade_value = 0.0
        self._cost_per_pnl_value = 0.0
        self._alert_active = False
        self._alert_state_emitted: Optional[bool] = None
        self._stop_event = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._metrics_api = self._load_metrics_api()
        self._gpu_supported = shutil.which("nvidia-smi") is not None

    @property
    def registry(self) -> CollectorRegistry:
        return self._registry

    def start(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._stop_event.clear()
        self._worker = threading.Thread(target=self._run_loop, daemon=True)
        self._worker.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._worker and self._worker.is_alive():
            self._worker.join(timeout=self._config.sample_interval_seconds or 1.0)

    # ------------------------------------------------------------------
    # Sampling loop
    # ------------------------------------------------------------------
    def _run_loop(self) -> None:
        interval = max(self._config.sample_interval_seconds, 1.0)
        while not self._stop_event.is_set():
            start = time.monotonic()
            try:
                self.sample()
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("Cost monitor sampling failed")
            elapsed = time.monotonic() - start
            sleep_for = max(0.0, interval - elapsed)
            self._stop_event.wait(sleep_for)

    def sample(self) -> None:
        now = datetime.now(timezone.utc)
        pod_metrics = self._fetch_pod_metrics()
        trade_stats = self._fetch_trade_stats()

        with self._lock:
            if trade_stats is not None:
                self._last_trade_stats = trade_stats

            if pod_metrics is None:
                self._last_sample_time = now
                self._latest_usage = {}
                self._service_hourly_cost.clear()
                self._trim_cost_window(now)
                self._update_trade_kpis_locked()
                self._update_metrics_locked()
                return

            usage_by_service = self._aggregate_usage(pod_metrics)
            gpu_equivalent = self._sample_gpu_utilization()
            if gpu_equivalent is not None and gpu_equivalent > 0:
                declared_gpu = sum(usage.gpu for usage in usage_by_service.values())
                if declared_gpu > 0:
                    scale = gpu_equivalent / declared_gpu
                    for usage in usage_by_service.values():
                        usage.gpu *= scale
                else:
                    usage_by_service[self._config.gpu_service_name] = ResourceUsage(
                        gpu=gpu_equivalent
                    )

            delta_seconds = (
                (now - self._last_sample_time).total_seconds()
                if self._last_sample_time is not None
                else None
            )
            self._last_sample_time = now
            self._latest_usage = {
                service: usage.copy() for service, usage in usage_by_service.items()
            }

            if delta_seconds is None or delta_seconds <= 0:
                self._service_hourly_cost.clear()
                for service, usage in usage_by_service.items():
                    self._service_hourly_cost[service] = self._hourly_cost(usage)
                self._trim_cost_window(now)
                self._update_trade_kpis_locked()
                self._update_metrics_locked()
                return

            self._service_hourly_cost.clear()
            cost_increment: Dict[str, float] = {}
            for service, usage in usage_by_service.items():
                hourly_cost = self._hourly_cost(usage)
                self._service_hourly_cost[service] = hourly_cost
                increment = hourly_cost * (delta_seconds / 3600.0)
                if increment > 0:
                    cost_increment[service] = increment
                    self._service_cost_totals[service] += increment

            for service in list(self._service_cost_totals.keys()):
                if service not in usage_by_service:
                    self._service_hourly_cost.setdefault(service, 0.0)

            total_increment = sum(cost_increment.values())
            if total_increment > 0:
                self._cost_window.append((now, total_increment))
            self._trim_cost_window(now)

            self._update_trade_kpis_locked()
            self._update_metrics_locked()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_metrics_api(self) -> Optional[CustomObjectsApi]:
        if client is None or config is None:
            return None
        try:  # pragma: no cover - environment dependent
            config.load_incluster_config()
        except Exception:  # pragma: no cover - fall back to kubeconfig
            try:
                config.load_kube_config()
            except Exception:
                LOGGER.debug("Unable to load Kubernetes configuration", exc_info=True)
                return None
        try:
            return client.CustomObjectsApi()
        except Exception:
            LOGGER.debug("Failed to instantiate Kubernetes CustomObjectsApi", exc_info=True)
            return None

    def _fetch_pod_metrics(self) -> Optional[Iterable[Mapping[str, object]]]:
        if self._metrics_api is None:
            LOGGER.debug("Kubernetes metrics API unavailable")
            return None
        try:
            if self._config.namespace:
                response = self._metrics_api.list_namespaced_custom_object(
                    group="metrics.k8s.io",
                    version="v1beta1",
                    namespace=self._config.namespace,
                    plural="pods",
                )
            else:
                response = self._metrics_api.list_cluster_custom_object(
                    group="metrics.k8s.io",
                    version="v1beta1",
                    plural="pods",
                )
        except Exception:
            LOGGER.warning("Failed to fetch pod metrics", exc_info=True)
            return None
        items = response.get("items") if isinstance(response, Mapping) else None
        if not isinstance(items, list):
            return None
        return [item for item in items if isinstance(item, Mapping)]

    def _aggregate_usage(
        self, pods: Iterable[Mapping[str, object]]
    ) -> Dict[str, ResourceUsage]:
        usage_by_service: MutableMapping[str, ResourceUsage] = defaultdict(ResourceUsage)
        label_key = self._config.service_label
        for pod in pods:
            metadata = pod.get("metadata") if isinstance(pod, Mapping) else None
            labels = {}
            if isinstance(metadata, Mapping):
                raw_labels = metadata.get("labels")
                if isinstance(raw_labels, Mapping):
                    labels = {
                        str(key): str(value)
                        for key, value in raw_labels.items()
                        if isinstance(key, str)
                    }
            service = labels.get(label_key) or str(
                metadata.get("namespace") if isinstance(metadata, Mapping) else "unknown"
            )
            usage = usage_by_service[service]
            usage.pods += 1
            containers = pod.get("containers") if isinstance(pod, Mapping) else None
            if not isinstance(containers, list):
                continue
            for container in containers:
                if not isinstance(container, Mapping):
                    continue
                raw_usage = container.get("usage")
                if not isinstance(raw_usage, Mapping):
                    continue
                cpu_value = raw_usage.get("cpu")
                if isinstance(cpu_value, str):
                    usage.cpu_cores += self._parse_cpu(cpu_value)
                mem_value = raw_usage.get("memory")
                if isinstance(mem_value, str):
                    usage.memory_bytes += self._parse_memory(mem_value)
                gpu_value = None
                for gpu_key in ("nvidia.com/gpu", "gpu"):
                    candidate = raw_usage.get(gpu_key)
                    if isinstance(candidate, str):
                        gpu_value = candidate
                        break
                if gpu_value is not None:
                    usage.gpu += self._parse_gpu(gpu_value)
        return {service: value.copy() for service, value in usage_by_service.items()}

    def _parse_cpu(self, value: str) -> float:
        value = value.strip()
        if not value:
            return 0.0
        suffix = value[-1]
        if suffix in self._CPU_UNITS:
            return float(value[:-1] or 0.0) * self._CPU_UNITS[suffix]
        return float(value)

    def _parse_memory(self, value: str) -> float:
        value = value.strip()
        if not value:
            return 0.0
        for suffix, multiplier in self._MEMORY_UNITS.items():
            if value.endswith(suffix):
                number = value[: -len(suffix)]
                return float(number or 0.0) * multiplier
        return float(value)

    def _parse_gpu(self, value: str) -> float:
        value = value.strip()
        if not value:
            return 0.0
        try:
            return float(value)
        except ValueError:
            return 0.0

    def _hourly_cost(self, usage: ResourceUsage) -> float:
        return (
            usage.cpu_cores * self._config.cpu_usd_per_hour
            + usage.memory_gb * self._config.mem_usd_per_gb_hour
            + usage.gpu * self._config.gpu_usd_per_hour
        )

    def _trim_cost_window(self, now: datetime) -> None:
        horizon = now - timedelta(hours=self._config.rolling_window_hours)
        while self._cost_window and self._cost_window[0][0] < horizon:
            self._cost_window.popleft()

    def _rolling_cost_total_locked(self) -> float:
        return sum(amount for _, amount in self._cost_window)

    def _fetch_trade_stats(self) -> Optional[TradeStats]:
        if self._config.trade_stats_url:
            try:
                response = httpx.get(
                    self._config.trade_stats_url,
                    timeout=self._config.http_timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                stats = self._parse_trade_payload(payload)
                if stats:
                    return stats
            except (httpx.HTTPError, ValueError):
                LOGGER.warning(
                    "Failed to fetch trade stats from %s", self._config.trade_stats_url,
                    exc_info=True,
                )
        if self._config.trade_stats_path:
            try:
                with open(self._config.trade_stats_path, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                stats = self._parse_trade_payload(payload)
                if stats:
                    return stats
            except (OSError, ValueError):
                LOGGER.warning(
                    "Failed to load trade stats from %s", self._config.trade_stats_path,
                    exc_info=True,
                )
        return None

    def _parse_trade_payload(self, payload: object) -> Optional[TradeStats]:
        if not isinstance(payload, Mapping):
            return None
        trades = payload.get("trades_24h") or payload.get("trade_count_24h")
        pnl = payload.get("pnl_24h") or payload.get("pnl_usd_24h")
        try:
            trades_value = float(trades) if trades is not None else 0.0
            pnl_value = float(pnl) if pnl is not None else 0.0
        except (TypeError, ValueError):
            return None
        return TradeStats(trades_24h=trades_value, pnl_24h=pnl_value)

    def _update_trade_kpis_locked(self) -> None:
        cost_24h = self._rolling_cost_total_locked()
        trades = self._last_trade_stats.trades_24h
        pnl = abs(self._last_trade_stats.pnl_24h)
        self._cost_per_trade_value = cost_24h / trades if trades > 0 else 0.0
        self._cost_per_pnl_value = cost_24h / pnl if pnl > 0 else 0.0
        threshold = self._config.cost_per_pnl_alert_threshold
        alert_active = threshold > 0 and self._cost_per_pnl_value > threshold
        self._alert_active = alert_active
        if self._alert_state_emitted is None or alert_active != self._alert_state_emitted:
            self._alert_state_emitted = alert_active
            if alert_active:
                LOGGER.error(
                    "Cost per PnL %.4f breached threshold %.4f", 
                    self._cost_per_pnl_value,
                    threshold,
                )
            else:
                LOGGER.info(
                    "Cost per PnL recovered below threshold: %.4f <= %.4f",
                    self._cost_per_pnl_value,
                    threshold,
                )

    def _update_metrics_locked(self) -> None:
        services = set(self._service_cost_totals) | set(self._latest_usage)
        for service in services:
            total = self._service_cost_totals.get(service, 0.0)
            self._cost_total.labels(service=service).set(total)
        self._cost_per_trade.set(self._cost_per_trade_value)
        self._cost_per_pnl.set(self._cost_per_pnl_value)

    def status_snapshot(self) -> Dict[str, object]:
        with self._lock:
            now = datetime.now(timezone.utc)
            self._trim_cost_window(now)
            cost_24h = self._rolling_cost_total_locked()
            services: Dict[str, Dict[str, float]] = {}
            totals = {
                "cpu_cores": 0.0,
                "memory_gb": 0.0,
                "gpu_units": 0.0,
                "cost_total_usd": 0.0,
                "pods": 0,
            }
            for service, usage in self._latest_usage.items():
                service_total = self._service_cost_totals.get(service, 0.0)
                services[service] = {
                    "cpu_cores": usage.cpu_cores,
                    "memory_gb": usage.memory_gb,
                    "gpu_units": usage.gpu,
                    "pods": usage.pods,
                    "cost_total_usd": service_total,
                    "cost_hourly_usd": self._service_hourly_cost.get(service, 0.0),
                }
                totals["cpu_cores"] += usage.cpu_cores
                totals["memory_gb"] += usage.memory_gb
                totals["gpu_units"] += usage.gpu
                totals["cost_total_usd"] += service_total
                totals["pods"] += usage.pods
            for service, total in self._service_cost_totals.items():
                if service in services:
                    continue
                services[service] = {
                    "cpu_cores": 0.0,
                    "memory_gb": 0.0,
                    "gpu_units": 0.0,
                    "pods": 0,
                    "cost_total_usd": total,
                    "cost_hourly_usd": self._service_hourly_cost.get(service, 0.0),
                }
            snapshot = {
                "timestamp": now.isoformat(),
                "services": services,
                "totals": {
                    **totals,
                    "cost_24h_usd": cost_24h,
                },
                "kpis": {
                    "cost_per_trade": self._cost_per_trade_value,
                    "cost_per_pnl": self._cost_per_pnl_value,
                    "trades_24h": self._last_trade_stats.trades_24h,
                    "pnl_24h": self._last_trade_stats.pnl_24h,
                },
                "alert": {
                    "active": self._alert_active,
                    "threshold": self._config.cost_per_pnl_alert_threshold,
                },
            }
            return snapshot

    def _sample_gpu_utilization(self) -> Optional[float]:
        if not self._gpu_supported:
            return None
        try:
            completed = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=utilization.gpu",
                    "--format=csv,noheader,nounits",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=2.0,
            )
        except (subprocess.SubprocessError, OSError):  # pragma: no cover - depends on env
            LOGGER.debug("nvidia-smi unavailable", exc_info=True)
            self._gpu_supported = False
            return None
        values = []
        for line in completed.stdout.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                values.append(float(stripped))
            except ValueError:
                continue
        if not values:
            return None
        return sum(values) / 100.0


def create_app(config: Optional[CostMonitorConfig] = None) -> FastAPI:
    monitor = CostMonitor(config=config)
    app = FastAPI()
    app.state.monitor = monitor
    setup_health_checks(app, {"cost_monitor": monitor.status_snapshot})

    @app.on_event("startup")
    async def _startup() -> None:
        monitor.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        monitor.stop()

    @app.get("/metrics")
    def metrics_endpoint() -> Response:
        payload = generate_latest(monitor.registry)
        return Response(content=payload, media_type=CONTENT_TYPE_LATEST)

    @app.get("/cost/status")
    def status_endpoint() -> JSONResponse:
        return JSONResponse(monitor.status_snapshot())

    return app


def main() -> None:
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover - uvicorn always available in prod
        raise SystemExit("uvicorn is required to run the cost monitor") from exc

    host = os.getenv("COST_MONITOR_HOST", "0.0.0.0")
    port = int(os.getenv("COST_MONITOR_PORT", "8080"))
    uvicorn.run(create_app(), host=host, port=port)


if __name__ == "__main__":
    main()
