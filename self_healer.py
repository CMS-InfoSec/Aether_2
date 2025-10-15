"""Self healing control loop for Aether micro-services.

This module periodically polls the health endpoints that every critical service
is expected to expose and restarts unhealthy pods through the Kubernetes API.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Protocol

import httpx

try:  # pragma: no cover - prefer FastAPI when available
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import FastAPI, JSONResponse  # type: ignore[assignment]
from kubernetes import client, config
from kubernetes.client import CoreV1Api
from kubernetes.config.config_exception import ConfigException

from common.utils.redis import create_redis_from_url


logger = logging.getLogger("self_healer")


def _int_from_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid value for %s=%s; falling back to %s", name, value, default)
        return default


class RestartLogStore(Protocol):
    """Persistence interface for restart events."""

    def record_restart(self, service: str, reason: str, when: datetime) -> None:
        ...

    def last_actions(self, limit: int) -> List[Dict[str, Any]]:
        ...

    def count_recent_restarts(
        self, service: str, window_seconds: int, *, now: Optional[datetime] = None
    ) -> int:
        ...


class RedisRestartLogStore:
    """Redis-backed implementation of the restart log."""

    def __init__(
        self,
        redis_client: Any,
        *,
        log_key: str,
        service_index_prefix: str,
        retention: int,
        per_service_retention_seconds: int,
    ) -> None:
        self._redis = redis_client
        self._log_key = log_key
        self._service_index_prefix = service_index_prefix
        self._retention = max(1, retention)
        self._per_service_retention_seconds = max(1, per_service_retention_seconds)

    def _service_key(self, service: str) -> str:
        return f"{self._service_index_prefix}{service}"

    def record_restart(self, service: str, reason: str, when: datetime) -> None:
        timestamp = when.astimezone(timezone.utc)
        entry = json.dumps(
            {"service": service, "reason": reason, "ts": timestamp.isoformat()},
            separators=(",", ":"),
        )
        epoch = timestamp.timestamp()
        service_member = f"{timestamp.isoformat()}:{uuid.uuid4().hex}"
        service_key = self._service_key(service)

        pipe = self._redis.pipeline()
        pipe.lpush(self._log_key, entry)
        pipe.ltrim(self._log_key, 0, self._retention - 1)
        pipe.zadd(service_key, {service_member: epoch})
        cutoff = epoch - self._per_service_retention_seconds
        pipe.zremrangebyscore(service_key, "-inf", cutoff)
        pipe.expire(service_key, self._per_service_retention_seconds)
        pipe.execute()

    def last_actions(self, limit: int) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        entries = self._redis.lrange(self._log_key, 0, limit - 1)
        actions: List[Dict[str, Any]] = []
        for raw in entries:
            try:
                payload = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                logger.debug("Discarding malformed restart log payload: %s", raw)
                continue
            if isinstance(payload, dict):
                actions.append(payload)
        return actions

    def count_recent_restarts(
        self, service: str, window_seconds: int, *, now: Optional[datetime] = None
    ) -> int:
        if window_seconds <= 0:
            return 0
        timestamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        cutoff = timestamp.timestamp() - window_seconds
        service_key = self._service_key(service)
        # Prune stale entries opportunistically before counting.
        self._redis.zremrangebyscore(
            service_key, "-inf", timestamp.timestamp() - self._per_service_retention_seconds
        )
        return int(self._redis.zcount(service_key, cutoff, "+inf"))


@dataclass(slots=True)
class ServiceConfig:
    """Metadata required to probe and restart a service."""

    name: str
    base_url: str
    namespace: str
    label_selector: str
    readiness_path: str = "/ready"
    latency_path: Optional[str] = "/health/latency"
    latency_threshold_ms: float = 1500.0
    functional_path: Optional[str] = "/health"
    functional_expectations: Dict[str, Any] = field(default_factory=dict)

    def url_for(self, path: Optional[str]) -> Optional[str]:
        if not path:
            return None
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return self.base_url.rstrip("/") + path


class SelfHealer:
    """Service watchdog that can restart unhealthy pods."""

    def __init__(
        self,
        services: Iterable[ServiceConfig],
        *,
        poll_interval: float = 30.0,
        http_timeout: float = 5.0,
        redis_url: Optional[str] = None,
        restart_store: Optional["RestartLogStore"] = None,
    ) -> None:
        self.services: List[ServiceConfig] = list(services)
        self.poll_interval = poll_interval
        self.http_timeout = http_timeout
        self._task: Optional[asyncio.Task[None]] = None

        self._core_v1_api: Optional[CoreV1Api] = self._load_kubernetes_api()
        self._rate_limit_per_service = _int_from_env("SELF_HEALER_RESTART_LIMIT", 3)
        self._rate_limit_window = _int_from_env("SELF_HEALER_RESTART_WINDOW_SECONDS", 900)
        log_retention = max(1, _int_from_env("SELF_HEALER_LOG_RETENTION", 200))
        service_log_retention_seconds = max(
            1,
            _int_from_env(
                "SELF_HEALER_SERVICE_LOG_RETENTION_SECONDS",
                max(self._rate_limit_window, 86400),
            ),
        )

        allow_stub = "pytest" in sys.modules

        if restart_store is None:
            resolved_redis_url = redis_url or os.getenv("SELF_HEALER_REDIS_URL")

            if resolved_redis_url is None or not resolved_redis_url.strip():
                if allow_stub:
                    resolved_redis_url = "redis://localhost:6379/0"
                else:
                    raise RuntimeError(
                        "SELF_HEALER_REDIS_URL must be configured before starting the self-healer service"
                    )

            redis_client = self._create_redis_client(
                resolved_redis_url.strip(), allow_stub=allow_stub
            )

            self._restart_store = RedisRestartLogStore(
                redis_client,
                log_key=os.getenv("SELF_HEALER_REDIS_LOG_KEY", "self_healer:restart_log"),
                service_index_prefix=os.getenv(
                    "SELF_HEALER_REDIS_SERVICE_PREFIX", "self_healer:service:"
                ),
                retention=log_retention,
                per_service_retention_seconds=service_log_retention_seconds,
            )
        else:
            self._restart_store = restart_store

    @staticmethod
    def _load_kubernetes_api() -> Optional[CoreV1Api]:
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration")
        except ConfigException:
            try:
                config.load_kube_config()
                logger.info("Loaded local Kubernetes configuration")
            except ConfigException:
                logger.warning("Unable to load Kubernetes configuration; restarts disabled")
                return None
        return CoreV1Api()

    @staticmethod
    def _create_redis_client(redis_url: str, *, allow_stub: bool):
        client, used_stub = create_redis_from_url(
            redis_url, decode_responses=True, logger=logger
        )
        if used_stub and not allow_stub:
            raise RuntimeError(
                "Failed to connect to Redis at SELF_HEALER_REDIS_URL; a reachable Redis instance is required"
            )
        return client

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_loop(), name="self-heal-loop")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run_loop(self) -> None:
        async with httpx.AsyncClient(timeout=self.http_timeout) as client_session:
            while True:
                for service in self.services:
                    try:
                        await self._evaluate_service(client_session, service)
                    except Exception:  # pragma: no cover - defensive logging
                        logger.exception("Unexpected error while checking %s", service.name)
                await asyncio.sleep(self.poll_interval)

    async def _evaluate_service(self, http_client: httpx.AsyncClient, service: ServiceConfig) -> None:
        readiness_url = service.url_for(service.readiness_path)
        if readiness_url and not await self._check_ok(http_client, readiness_url):
            await self._restart(service, "Readiness probe failed")
            return

        if service.latency_path:
            latency_url = service.url_for(service.latency_path)
            latency = await self._fetch_latency(http_client, latency_url) if latency_url else None
            if latency is not None and latency > service.latency_threshold_ms:
                await self._restart(service, f"Latency {latency:.0f}ms above threshold {service.latency_threshold_ms:.0f}ms")
                return

        if not await self._check_functional(http_client, service):
            await self._restart(service, "Functional health check failed")

    async def _check_ok(self, http_client: httpx.AsyncClient, url: str) -> bool:
        try:
            response = await http_client.get(url)
        except httpx.HTTPError as exc:
            logger.warning("Health request to %s failed: %s", url, exc)
            return False

        if response.status_code != 200:
            logger.warning("Health request to %s returned status %s", url, response.status_code)
            return False

        if response.headers.get("content-type", "").startswith("application/json"):
            payload = response.json()
            status = payload.get("status")
            if status and status.lower() != "ok":
                logger.warning("Health request to %s returned payload %s", url, payload)
                return False
        return True

    async def _fetch_latency(self, http_client: httpx.AsyncClient, url: Optional[str]) -> Optional[float]:
        if not url:
            return None
        try:
            response = await http_client.get(url)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Latency request to %s failed: %s", url, exc)
            return None

        try:
            payload = response.json()
        except ValueError:
            logger.debug("Latency request to %s returned non-JSON payload", url)
            return None

        for key in ("latency_ms", "latency", "p99_latency_ms", "p95_latency_ms"):
            value = payload.get(key)
            if isinstance(value, (int, float)):
                return float(value)
        return None

    async def _check_functional(self, http_client: httpx.AsyncClient, service: ServiceConfig) -> bool:
        functional_url = service.url_for(service.functional_path)
        if functional_url is None:
            return True

        try:
            response = await http_client.get(functional_url)
        except httpx.HTTPError as exc:
            logger.warning("Functional check for %s failed: %s", service.name, exc)
            return False

        if response.status_code != 200:
            logger.warning(
                "Functional check for %s returned status %s", service.name, response.status_code
            )
            return False

        payload: Dict[str, Any]
        try:
            payload = response.json()
        except ValueError:
            logger.warning("Functional check for %s returned invalid JSON", service.name)
            return False

        if service.name.lower() == "oms":
            return self._validate_oms_loop(payload)

        for key, expected in service.functional_expectations.items():
            if payload.get(key) != expected:
                logger.warning(
                    "Functional check for %s did not match expectation %s=%s (payload=%s)",
                    service.name,
                    key,
                    expected,
                    payload,
                )
                return False
        return True

    @staticmethod
    def _validate_oms_loop(payload: Dict[str, Any]) -> bool:
        markers = (
            payload.get("order_loop"),
            payload.get("loop_status"),
            payload.get("loop"),
            payload.get("status"),
        )
        if any(str(marker).lower() in {"ok", "closed", "complete", "healthy", "true"} for marker in markers if marker is not None):
            return True
        if payload.get("orders_processed") and payload.get("orders_acknowledged"):
            return payload["orders_processed"] == payload["orders_acknowledged"]
        logger.warning("OMS order loop payload unexpected: %s", payload)
        return False

    async def _restart(self, service: ServiceConfig, reason: str) -> None:
        if (
            self._rate_limit_per_service > 0
            and self._rate_limit_window > 0
            and self._restart_store.count_recent_restarts(
                service.name, self._rate_limit_window
            )
            >= self._rate_limit_per_service
        ):
            logger.warning(
                "Restart for %s suppressed due to rate limit (%s restarts within %ss)",
                service.name,
                self._rate_limit_per_service,
                self._rate_limit_window,
            )
            return

        logger.error("Restarting %s due to %s", service.name, reason)
        self._record_restart(service.name, reason)
        if not self._core_v1_api:
            logger.warning("Kubernetes client unavailable; skipping restart for %s", service.name)
            return

        await asyncio.get_running_loop().run_in_executor(
            None,
            self._delete_pods,
            service,
        )

    def _delete_pods(self, service: ServiceConfig) -> None:
        try:
            pods = self._core_v1_api.list_namespaced_pod(
                namespace=service.namespace,
                label_selector=service.label_selector or None,
            )
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("Failed to list pods for %s", service.name)
            return

        for pod in pods.items:
            pod_name = pod.metadata.name if pod.metadata else "<unknown>"
            try:
                self._core_v1_api.delete_namespaced_pod(
                    name=pod_name,
                    namespace=service.namespace,
                    body=client.V1DeleteOptions(grace_period_seconds=0),
                )
                logger.info("Requested deletion of pod %s for service %s", pod_name, service.name)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Failed to delete pod %s for %s", pod_name, service.name)

    def _record_restart(self, service: str, reason: str) -> None:
        when = datetime.now(timezone.utc)
        self._restart_store.record_restart(service, reason, when)

    def last_actions(self, limit: int = 20) -> List[Dict[str, Any]]:
        return self._restart_store.last_actions(limit)


def _build_service_configs() -> List[ServiceConfig]:
    defaults = {
        "policy": ServiceConfig(
            name="policy",
            base_url=os.getenv("POLICY_SERVICE_URL", "http://policy-service"),
            namespace=os.getenv("POLICY_SERVICE_NAMESPACE", "aether"),
            label_selector=os.getenv("POLICY_SERVICE_SELECTOR", "app=policy-service"),
            functional_expectations={"status": "ok"},
        ),
        "risk": ServiceConfig(
            name="risk",
            base_url=os.getenv("RISK_SERVICE_URL", "http://risk-service"),
            namespace=os.getenv("RISK_SERVICE_NAMESPACE", "aether"),
            label_selector=os.getenv("RISK_SERVICE_SELECTOR", "app=risk-service"),
            functional_expectations={"status": "ok"},
        ),
        "oms": ServiceConfig(
            name="oms",
            base_url=os.getenv("OMS_SERVICE_URL", "http://oms-service"),
            namespace=os.getenv("OMS_SERVICE_NAMESPACE", "aether"),
            label_selector=os.getenv("OMS_SERVICE_SELECTOR", "app=oms-service"),
            functional_path=os.getenv("OMS_FUNCTIONAL_PATH", "/health/order-loop"),
        ),
        "fees": ServiceConfig(
            name="fees",
            base_url=os.getenv("FEES_SERVICE_URL", "http://fees-service"),
            namespace=os.getenv("FEES_SERVICE_NAMESPACE", "aether"),
            label_selector=os.getenv("FEES_SERVICE_SELECTOR", "app=fees-service"),
            functional_expectations={"status": "ok"},
        ),
        "secrets": ServiceConfig(
            name="secrets",
            base_url=os.getenv("SECRETS_SERVICE_URL", "http://secrets-service"),
            namespace=os.getenv("SECRETS_SERVICE_NAMESPACE", "aether"),
            label_selector=os.getenv("SECRETS_SERVICE_SELECTOR", "app=secrets-service"),
            functional_expectations={"status": "ok"},
        ),
        "universe": ServiceConfig(
            name="universe",
            base_url=os.getenv("UNIVERSE_SERVICE_URL", "http://universe-service"),
            namespace=os.getenv("UNIVERSE_SERVICE_NAMESPACE", "aether"),
            label_selector=os.getenv("UNIVERSE_SERVICE_SELECTOR", "app=universe-service"),
            functional_expectations={"status": "ok"},
        ),
    }

    latency_threshold = float(os.getenv("DEFAULT_LATENCY_THRESHOLD_MS", "1500"))
    for service in defaults.values():
        service.latency_threshold_ms = float(
            os.getenv(f"{service.name.upper()}_LATENCY_THRESHOLD_MS", str(latency_threshold))
        )
    return list(defaults.values())


self_healer = SelfHealer(_build_service_configs())
app = FastAPI()


@app.on_event("startup")
async def _startup() -> None:
    await self_healer.start()


@app.on_event("shutdown")
async def _shutdown() -> None:
    await self_healer.stop()


@app.get("/selfheal/status")
async def get_status() -> JSONResponse:
    return JSONResponse({"actions": self_healer.last_actions()})


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("SELF_HEALER_PORT", "8080")))
