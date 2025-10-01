"""Adaptive infrastructure scaling controller.

This module wires together OMS throughput, policy inference load and model
training queue telemetry to make basic scaling decisions.  It can scale the
OMS deployment replicas through the Kubernetes API and provision or
deprovision GPU node pools in Linode when model training workloads arrive or
become idle.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, TypeVar

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

try:  # pragma: no cover - optional dependency in CI
    from kubernetes import client, config
    from kubernetes.client import AppsV1Api
    from kubernetes.config.config_exception import ConfigException
except Exception:  # pragma: no cover - kubernetes is optional for tests
    client = None  # type: ignore
    config = None  # type: ignore
    AppsV1Api = None  # type: ignore
    ConfigException = Exception  # type: ignore


logger = logging.getLogger("scaling_controller")


T = TypeVar("T")
MaybeAwaitable = Awaitable[T] | T

ThroughputGetter = Callable[[], MaybeAwaitable[float]]
InferenceLoadGetter = Callable[[], MaybeAwaitable[float]]
TrainingPendingGetter = Callable[[], MaybeAwaitable[int]]


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _resolve(value: MaybeAwaitable[T]) -> T:
    if inspect.isawaitable(value):
        return await value  # type: ignore[arg-type]
    return value  # type: ignore[return-value]


@dataclass(slots=True)
class _ScalingState:
    oms_replicas: int
    gpu_nodes: int
    pending_jobs: int
    last_policy_load: float | None = None


class ScalingStatus(BaseModel):
    """API response payload describing current scaling state."""

    oms_replicas: int = Field(..., description="Current OMS deployment replica count")
    gpu_nodes: int = Field(..., description="Number of GPU worker nodes provisioned")
    pending_jobs: int = Field(..., description="Number of pending model training jobs")


class OmsDeploymentScaler:
    """Adapter that scales a Kubernetes deployment for the OMS."""

    def __init__(
        self,
        *,
        namespace: str,
        deployment: str,
        fallback_replicas: int = 1,
    ) -> None:
        self.namespace = namespace
        self.deployment = deployment
        self._fallback_replicas = max(fallback_replicas, 0)
        self._replica_cache = max(fallback_replicas, 0)
        self._apps_v1 = self._load_api()

    def _load_api(self) -> Optional[AppsV1Api]:
        if client is None or config is None:
            logger.warning("Kubernetes client libraries unavailable; using fallback replica tracking")
            return None
        try:  # pragma: no cover - depends on cluster configuration
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration for scaling controller")
        except ConfigException:
            try:
                config.load_kube_config()
                logger.info("Loaded local Kubernetes configuration for scaling controller")
            except ConfigException:
                logger.warning(
                    "Unable to load Kubernetes configuration; falling back to in-memory replica tracking",
                )
                return None
        return client.AppsV1Api()

    async def get_replicas(self) -> int:
        if self._apps_v1 is None:
            return self._replica_cache
        loop = asyncio.get_running_loop()
        try:
            deployment = await loop.run_in_executor(
                None,
                lambda: self._apps_v1.read_namespaced_deployment(
                    name=self.deployment,
                    namespace=self.namespace,
                ),
            )
        except Exception as exc:  # pragma: no cover - depends on kubernetes client behaviour
            logger.warning("Failed to read deployment %s/%s replicas: %s", self.namespace, self.deployment, exc)
            return self._replica_cache
        spec = getattr(deployment, "spec", None)
        replicas = getattr(spec, "replicas", None)
        if replicas is None:
            return self._replica_cache
        try:
            self._replica_cache = int(replicas)
        except (TypeError, ValueError):
            logger.debug("Unexpected replica count %s for %s/%s", replicas, self.namespace, self.deployment)
        return self._replica_cache

    async def scale_to(self, replicas: int) -> None:
        replicas = max(int(replicas), 0)
        self._replica_cache = replicas
        if self._apps_v1 is None:
            logger.info("Recording desired OMS replicas=%s using fallback tracker", replicas)
            return
        loop = asyncio.get_running_loop()
        body = {"spec": {"replicas": replicas}}
        try:
            await loop.run_in_executor(
                None,
                lambda: self._apps_v1.patch_namespaced_deployment_scale(
                    name=self.deployment,
                    namespace=self.namespace,
                    body=body,
                ),
            )
            logger.info("Scaled deployment %s/%s to %s replicas", self.namespace, self.deployment, replicas)
        except Exception as exc:  # pragma: no cover - depends on client
            logger.warning(
                "Failed to scale deployment %s/%s to %s replicas: %s",
                self.namespace,
                self.deployment,
                replicas,
                exc,
            )


class LinodeGPUManager:
    """Minimal Linode LKE node pool manager for GPU workloads."""

    def __init__(
        self,
        *,
        token: str,
        cluster_id: str,
        node_type: str,
        node_count: int,
        pool_label: str = "gpu-training",
        timeout: float = 10.0,
        base_url: str = "https://api.linode.com/v4",
    ) -> None:
        self.token = token
        self.cluster_id = cluster_id
        self.node_type = node_type
        self.node_count = max(node_count, 1)
        self.pool_label = pool_label
        self.timeout = timeout
        self.base_url = base_url.rstrip("/")
        self._pool_id: Optional[int] = None

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_payload: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client_session:
            response = await client_session.request(
                method,
                url,
                headers=self._headers(),
                json=json_payload,
            )
            response.raise_for_status()
            try:
                return response.json()
            except ValueError:
                return {}

    def _matches_gpu_pool(self, pool: Dict[str, Any]) -> bool:
        label = str(pool.get("label", ""))
        pool_type = str(pool.get("type", ""))
        tags = pool.get("tags") or []
        return (
            label == self.pool_label
            or pool_type == self.node_type
            or self.pool_label in tags
        )

    async def list_gpu_nodes(self) -> Sequence[str]:
        try:
            payload = await self._request(
                "GET",
                f"/lke/clusters/{self.cluster_id}/nodepools",
            )
        except httpx.HTTPError as exc:
            logger.warning("Failed to list Linode GPU pools: %s", exc)
            return []
        pools = payload.get("data") or []
        nodes: List[str] = []
        for pool in pools:
            if not isinstance(pool, dict):
                continue
            if self._matches_gpu_pool(pool):
                self._pool_id = int(pool.get("id") or 0) or self._pool_id
                for node in pool.get("nodes", []):
                    node_id = node.get("id") if isinstance(node, dict) else None
                    if node_id is not None:
                        nodes.append(str(node_id))
        return nodes

    async def provision_gpu_pool(self) -> Sequence[str]:
        current_nodes = await self.list_gpu_nodes()
        if current_nodes:
            if len(current_nodes) >= self.node_count:
                return current_nodes
            if self._pool_id:
                try:
                    await self._request(
                        "PUT",
                        f"/lke/clusters/{self.cluster_id}/nodepools/{self._pool_id}",
                        json_payload={
                            "count": self.node_count,
                            "type": self.node_type,
                            "tags": [self.pool_label],
                        },
                    )
                except httpx.HTTPError as exc:
                    logger.warning("Failed to scale Linode GPU pool %s: %s", self._pool_id, exc)
            return await self.list_gpu_nodes()
        try:
            payload = await self._request(
                "POST",
                f"/lke/clusters/{self.cluster_id}/nodepools",
                json_payload={
                    "type": self.node_type,
                    "count": self.node_count,
                    "tags": [self.pool_label],
                    "label": self.pool_label,
                },
            )
        except httpx.HTTPError as exc:
            logger.warning("Failed to provision Linode GPU pool: %s", exc)
            return current_nodes
        pool_id = payload.get("id")
        if isinstance(pool_id, int):
            self._pool_id = pool_id
        return await self.list_gpu_nodes()

    async def deprovision_gpu_pool(self) -> None:
        if not self._pool_id:
            logger.info("No Linode GPU pool provisioned; skipping deprovision")
            return
        try:
            await self._request(
                "DELETE",
                f"/lke/clusters/{self.cluster_id}/nodepools/{self._pool_id}",
            )
            logger.info("Deprovisioned Linode GPU pool %s", self._pool_id)
        except httpx.HTTPError as exc:
            logger.warning("Failed to deprovision Linode GPU pool %s: %s", self._pool_id, exc)
        finally:
            self._pool_id = None


class NullGPUManager:
    """Fallback GPU manager used when Linode credentials are absent."""

    def __init__(self, *, default_count: int = 0) -> None:
        self._nodes: List[str] = []
        self._default_count = max(default_count, 0)

    async def list_gpu_nodes(self) -> Sequence[str]:
        return list(self._nodes)

    async def provision_gpu_pool(self) -> Sequence[str]:
        if not self._nodes:
            self._nodes = [f"gpu-sim-{i}" for i in range(1, self._default_count + 1)]
            if self._nodes:
                logger.info("Simulated provisioning of %s GPU nodes", len(self._nodes))
        return list(self._nodes)

    async def deprovision_gpu_pool(self) -> None:
        if self._nodes:
            logger.info("Simulated deprovisioning of GPU nodes")
        self._nodes = []


class ScalingController:
    """Background control loop implementing infrastructure scaling policies."""

    def __init__(
        self,
        *,
        throughput_getter: ThroughputGetter,
        policy_load_getter: InferenceLoadGetter,
        pending_job_getter: TrainingPendingGetter,
        oms_scaler: OmsDeploymentScaler,
        gpu_manager: LinodeGPUManager | NullGPUManager,
        oms_scale_threshold: float = 500.0,
        check_interval: float = 60.0,
        gpu_idle_timeout: timedelta = timedelta(hours=1),
    ) -> None:
        self._throughput_getter = throughput_getter
        self._policy_load_getter = policy_load_getter
        self._pending_job_getter = pending_job_getter
        self._oms_scaler = oms_scaler
        self._gpu_manager = gpu_manager
        self._threshold = max(oms_scale_threshold, 0.0)
        self._check_interval = max(check_interval, 5.0)
        self._gpu_idle_timeout = max(gpu_idle_timeout, timedelta(minutes=5))

        self._state = _ScalingState(oms_replicas=oms_scaler._replica_cache, gpu_nodes=0, pending_jobs=0)
        self._task: Optional[asyncio.Task[None]] = None
        self._lock = asyncio.Lock()
        self._last_gpu_activity: datetime | None = None

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_loop(), name="scaling-controller-loop")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run_loop(self) -> None:
        while True:
            try:
                await self.evaluate_once()
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Unexpected error while running scaling evaluation")
            await asyncio.sleep(self._check_interval)

    async def evaluate_once(self) -> None:
        async with self._lock:
            now = _now()
            throughput = await _resolve(self._throughput_getter())
            policy_load = await _resolve(self._policy_load_getter())
            pending_jobs_raw = await _resolve(self._pending_job_getter())
            try:
                pending_jobs = max(int(pending_jobs_raw), 0)
            except (TypeError, ValueError):
                pending_jobs = 0

            logger.debug(
                "Scaling evaluation: throughput=%.2f orders/min, policy_load=%.2f, pending_jobs=%s",
                throughput,
                policy_load,
                pending_jobs,
            )

            replicas = await self._oms_scaler.get_replicas()
            if throughput > self._threshold:
                desired = replicas + 1
                logger.info(
                    "OMS throughput %.2f orders/min above threshold %.2f, scaling replicas %s -> %s",
                    throughput,
                    self._threshold,
                    replicas,
                    desired,
                )
                await self._oms_scaler.scale_to(desired)
                replicas = desired

            gpu_nodes = list(await self._gpu_manager.list_gpu_nodes())
            if pending_jobs > 0:
                if not gpu_nodes:
                    logger.info("Training jobs pending; provisioning GPU node pool")
                    gpu_nodes = list(await self._gpu_manager.provision_gpu_pool())
                self._last_gpu_activity = now
            else:
                if gpu_nodes and self._last_gpu_activity is not None:
                    idle_duration = now - self._last_gpu_activity
                    if idle_duration >= self._gpu_idle_timeout:
                        logger.info(
                            "GPU node pool idle for %s, deprovisioning", idle_duration,
                        )
                        await self._gpu_manager.deprovision_gpu_pool()
                        gpu_nodes = list(await self._gpu_manager.list_gpu_nodes())
                        self._last_gpu_activity = None

            self._state = _ScalingState(
                oms_replicas=replicas,
                gpu_nodes=len(gpu_nodes),
                pending_jobs=pending_jobs,
                last_policy_load=float(policy_load) if isinstance(policy_load, (int, float)) else None,
            )

    @property
    def status(self) -> ScalingStatus:
        return ScalingStatus(
            oms_replicas=self._state.oms_replicas,
            gpu_nodes=self._state.gpu_nodes,
            pending_jobs=self._state.pending_jobs,
        )


def _constant_async(value: T) -> Callable[[], Awaitable[T]]:
    async def _inner() -> T:
        return value

    return _inner


def _dig(payload: Any, field: str) -> Any:
    if not field:
        return payload
    parts = field.split(".")
    current = payload
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def http_json_field_getter(
    url: str,
    field: str,
    *,
    default: T,
    cast: Callable[[Any], T],
    timeout: float = 5.0,
) -> Callable[[], Awaitable[T]]:
    async def _getter() -> T:
        if not url:
            return default
        try:
            async with httpx.AsyncClient(timeout=timeout) as client_session:
                response = await client_session.get(url)
                response.raise_for_status()
                try:
                    payload = response.json()
                except ValueError:
                    logger.debug("Failed decoding JSON payload from %s", url)
                    return default
        except httpx.HTTPError as exc:
            logger.warning("HTTP error while fetching %s: %s", url, exc)
            return default
        value = _dig(payload, field)
        try:
            return cast(value)
        except Exception:
            return default

    return _getter


def build_scaling_controller_from_env() -> ScalingController:
    """Construct a ``ScalingController`` wired with environment configured adapters."""

    throughput_url = os.getenv("OMS_THROUGHPUT_URL")
    throughput_field = os.getenv("OMS_THROUGHPUT_FIELD", "orders_per_minute")
    policy_load_url = os.getenv("POLICY_INFERENCE_URL")
    policy_load_field = os.getenv("POLICY_INFERENCE_FIELD", "current_load")
    training_queue_url = os.getenv("TRAINING_QUEUE_URL")
    training_queue_field = os.getenv("TRAINING_QUEUE_FIELD", "pending_jobs")

    throughput_getter = (
        http_json_field_getter(
            throughput_url,
            throughput_field,
            default=0.0,
            cast=lambda value: float(value),
        )
        if throughput_url
        else _constant_async(0.0)
    )

    policy_load_getter = (
        http_json_field_getter(
            policy_load_url,
            policy_load_field,
            default=0.0,
            cast=lambda value: float(value),
        )
        if policy_load_url
        else _constant_async(0.0)
    )

    pending_job_getter = (
        http_json_field_getter(
            training_queue_url,
            training_queue_field,
            default=0,
            cast=lambda value: int(value),
        )
        if training_queue_url
        else _constant_async(0)
    )

    namespace = os.getenv("OMS_DEPLOYMENT_NAMESPACE", "aether")
    deployment_name = os.getenv("OMS_DEPLOYMENT_NAME", "oms-service")
    fallback_replicas = int(os.getenv("OMS_REPLICA_FALLBACK", "1"))
    oms_scaler = OmsDeploymentScaler(
        namespace=namespace,
        deployment=deployment_name,
        fallback_replicas=fallback_replicas,
    )

    linode_token = os.getenv("LINODE_TOKEN")
    linode_cluster_id = os.getenv("LINODE_CLUSTER_ID")
    linode_node_type = os.getenv("LINODE_GPU_NODE_TYPE")
    linode_node_count = int(os.getenv("LINODE_GPU_NODE_COUNT", "1"))
    linode_pool_label = os.getenv("LINODE_GPU_POOL_LABEL", "gpu-training")

    if linode_token and linode_cluster_id and linode_node_type:
        gpu_manager: LinodeGPUManager | NullGPUManager = LinodeGPUManager(
            token=linode_token,
            cluster_id=linode_cluster_id,
            node_type=linode_node_type,
            node_count=linode_node_count,
            pool_label=linode_pool_label,
        )
    else:
        gpu_manager = NullGPUManager(default_count=linode_node_count)
        logger.info("Linode credentials missing; using simulated GPU manager")

    oms_threshold = float(os.getenv("OMS_THROUGHPUT_THRESHOLD", "500"))
    check_interval = float(os.getenv("SCALING_CHECK_INTERVAL", "60"))
    gpu_idle_seconds = float(os.getenv("GPU_IDLE_TIMEOUT", str(60 * 60)))

    controller = ScalingController(
        throughput_getter=throughput_getter,
        policy_load_getter=policy_load_getter,
        pending_job_getter=pending_job_getter,
        oms_scaler=oms_scaler,
        gpu_manager=gpu_manager,
        oms_scale_threshold=oms_threshold,
        check_interval=check_interval,
        gpu_idle_timeout=timedelta(seconds=gpu_idle_seconds),
    )
    return controller


_controller: Optional[ScalingController] = None


def configure_scaling_controller(controller: ScalingController) -> None:
    global _controller
    _controller = controller


def get_scaling_controller() -> ScalingController:
    if _controller is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Scaling controller not configured",
        )
    return _controller


router = APIRouter(prefix="/infra/scaling", tags=["infrastructure"])


@router.get("/status", response_model=ScalingStatus)
async def scaling_status(controller: ScalingController = Depends(get_scaling_controller)) -> ScalingStatus:
    return controller.status


__all__ = [
    "ScalingController",
    "ScalingStatus",
    "build_scaling_controller_from_env",
    "configure_scaling_controller",
    "router",
]
