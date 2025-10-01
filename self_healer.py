"""Self healing control loop for Aether micro-services.

This module periodically polls the health endpoints that every critical service
is expected to expose and restarts unhealthy pods through the Kubernetes API.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from kubernetes import client, config
from kubernetes.client import CoreV1Api
from kubernetes.config.config_exception import ConfigException


logger = logging.getLogger("self_healer")


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
        db_path: str = "data/self_healer.db",
    ) -> None:
        self.services: List[ServiceConfig] = list(services)
        self.poll_interval = poll_interval
        self.http_timeout = http_timeout
        self.db_path = db_path
        self._task: Optional[asyncio.Task[None]] = None
        self._last_actions: List[Dict[str, Any]] = []

        self._core_v1_api: Optional[CoreV1Api] = self._load_kubernetes_api()
        self._ensure_db()

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

    def _ensure_db(self) -> None:
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        with self._db_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS self_heal_log (
                    service TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    ts TEXT NOT NULL
                )
                """
            )
            conn.commit()

    @contextmanager
    def _db_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

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
        timestamp = datetime.now(timezone.utc).isoformat()
        with self._db_connection() as conn:
            conn.execute(
                "INSERT INTO self_heal_log(service, reason, ts) VALUES (?, ?, ?)",
                (service, reason, timestamp),
            )
            conn.commit()

        self._last_actions.append({"service": service, "reason": reason, "ts": timestamp})
        self._last_actions = self._last_actions[-20:]

    def last_actions(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._db_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT service, reason, ts FROM self_heal_log ORDER BY ts DESC LIMIT ?",
                (limit,),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]


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
