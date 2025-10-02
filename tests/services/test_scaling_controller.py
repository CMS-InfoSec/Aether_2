from datetime import datetime, timedelta, timezone
from typing import List

import pytest
from unittest.mock import AsyncMock

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

import scaling_controller
from scaling_controller import ScalingController


class DummyScaler:
    def __init__(self, replicas: int = 1) -> None:
        self._replica_cache = replicas
        self.scale_calls: List[int] = []

    async def get_replicas(self) -> int:
        return self._replica_cache

    async def scale_to(self, replicas: int) -> None:
        self._replica_cache = replicas
        self.scale_calls.append(replicas)


class DummyGPUManager:
    def __init__(self, nodes: List[str] | None = None) -> None:
        self.nodes = list(nodes or [])
        self.provision_calls = 0
        self.deprovision_calls = 0

    async def list_gpu_nodes(self) -> List[str]:
        return list(self.nodes)

    async def provision_gpu_pool(self) -> List[str]:
        self.provision_calls += 1
        if not self.nodes:
            self.nodes = [f"gpu-{self.provision_calls}"]
        return list(self.nodes)

    async def deprovision_gpu_pool(self) -> None:
        self.deprovision_calls += 1
        self.nodes = []


@pytest.mark.asyncio
async def test_start_stop_lifecycle_creates_background_task() -> None:
    throughput_getter = AsyncMock(return_value=0.0)
    policy_load_getter = AsyncMock(return_value=0.0)
    pending_job_getter = AsyncMock(return_value=0)
    scaler = DummyScaler()
    gpu_manager = DummyGPUManager()

    controller = ScalingController(
        throughput_getter=throughput_getter,
        policy_load_getter=policy_load_getter,
        pending_job_getter=pending_job_getter,
        oms_scaler=scaler,
        gpu_manager=gpu_manager,
        oms_scale_threshold=100.0,
        check_interval=5.0,
        gpu_idle_timeout=timedelta(minutes=10),
    )

    await controller.start()
    assert controller._task is not None  # noqa: SLF001 - verifying internal lifecycle
    assert not controller._task.done()

    await controller.stop()
    assert controller._task is None


@pytest.mark.asyncio
async def test_evaluate_scales_up_when_throughput_exceeds_threshold() -> None:
    throughput_getter = AsyncMock(return_value=600.0)
    policy_load_getter = AsyncMock(return_value=0.0)
    pending_job_getter = AsyncMock(return_value=0)
    scaler = DummyScaler(replicas=2)
    gpu_manager = DummyGPUManager()

    controller = ScalingController(
        throughput_getter=throughput_getter,
        policy_load_getter=policy_load_getter,
        pending_job_getter=pending_job_getter,
        oms_scaler=scaler,
        gpu_manager=gpu_manager,
        oms_scale_threshold=500.0,
        check_interval=5.0,
        gpu_idle_timeout=timedelta(minutes=10),
    )

    await controller.evaluate_once()

    assert scaler.scale_calls == [3]
    assert controller.status.oms_replicas == 3


@pytest.mark.asyncio
async def test_evaluate_provisions_and_deprovisions_gpu_nodes(monkeypatch: pytest.MonkeyPatch) -> None:
    throughput_getter = AsyncMock(return_value=0.0)
    policy_load_getter = AsyncMock(return_value=0.0)
    pending_job_getter = AsyncMock(side_effect=[2, 0])
    scaler = DummyScaler()
    gpu_manager = DummyGPUManager()

    controller = ScalingController(
        throughput_getter=throughput_getter,
        policy_load_getter=policy_load_getter,
        pending_job_getter=pending_job_getter,
        oms_scaler=scaler,
        gpu_manager=gpu_manager,
        oms_scale_threshold=100.0,
        check_interval=5.0,
        gpu_idle_timeout=timedelta(minutes=1),
    )

    base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    times = iter([
        base_time,
        base_time + controller._gpu_idle_timeout + timedelta(seconds=1),
    ])
    monkeypatch.setattr(scaling_controller, "_now", lambda: next(times))

    await controller.evaluate_once()
    assert gpu_manager.provision_calls == 1
    assert controller.status.gpu_nodes == 1

    await controller.evaluate_once()
    assert gpu_manager.deprovision_calls == 1
    assert controller.status.gpu_nodes == 0


@pytest.mark.asyncio
async def test_evaluate_handles_invalid_pending_job_values() -> None:
    throughput_getter = AsyncMock(return_value=0.0)
    policy_load_getter = AsyncMock(return_value=0.75)
    pending_job_getter = AsyncMock(return_value="not-a-number")
    scaler = DummyScaler()
    gpu_manager = DummyGPUManager(nodes=["gpu-preexisting"])

    controller = ScalingController(
        throughput_getter=throughput_getter,
        policy_load_getter=policy_load_getter,
        pending_job_getter=pending_job_getter,
        oms_scaler=scaler,
        gpu_manager=gpu_manager,
        oms_scale_threshold=100.0,
        check_interval=5.0,
        gpu_idle_timeout=timedelta(minutes=10),
    )

    await controller.evaluate_once()

    assert controller.status.pending_jobs == 0
    assert controller.status.gpu_nodes == 1
    policy_load_getter.assert_awaited()

