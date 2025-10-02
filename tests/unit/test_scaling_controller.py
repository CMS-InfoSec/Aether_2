from datetime import datetime, timedelta, timezone
import asyncio
import sys
from types import SimpleNamespace
from typing import List

if "httpx" not in sys.modules:
    class _StubAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

        async def request(self, *args, **kwargs):  # pragma: no cover - defensive stub
            raise RuntimeError("httpx stub cannot perform network requests")

    sys.modules["httpx"] = SimpleNamespace(AsyncClient=_StubAsyncClient, HTTPError=Exception)


if "fastapi" not in sys.modules:
    class _StubAPIRouter:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def get(self, *args, **kwargs):
            def _decorator(func):
                return func

            return _decorator

    class _StubDepends:
        def __init__(self, dependency) -> None:
            self.dependency = dependency

    class _StubHTTPException(Exception):
        def __init__(self, status_code: int, detail: str | None = None) -> None:
            self.status_code = status_code
            self.detail = detail

    sys.modules["fastapi"] = SimpleNamespace(
        APIRouter=_StubAPIRouter,
        Depends=_StubDepends,
        HTTPException=_StubHTTPException,
        status=SimpleNamespace(HTTP_503_SERVICE_UNAVAILABLE=503),
    )


if "pydantic" not in sys.modules:
    class _StubBaseModel:
        def __init__(self, **data) -> None:
            for key, value in data.items():
                setattr(self, key, value)

    def _stub_field(*args, **kwargs):
        return None

    sys.modules["pydantic"] = SimpleNamespace(BaseModel=_StubBaseModel, Field=_stub_field)


import pytest

import scaling_controller
from scaling_controller import NullGPUManager, ScalingController


class _FakeOmsScaler:
    def __init__(self, replicas: int) -> None:
        self._replica_cache = replicas
        self.calls: List[int] = []

    async def get_replicas(self) -> int:
        return self._replica_cache

    async def scale_to(self, replicas: int) -> None:
        self._replica_cache = replicas
        self.calls.append(replicas)


def _constant(value):
    async def _inner():
        return value

    return _inner


def test_scales_down_after_sustained_low_throughput(monkeypatch):
    scaler = _FakeOmsScaler(replicas=3)
    controller = ScalingController(
        throughput_getter=_constant(50.0),
        policy_load_getter=_constant(0.0),
        pending_job_getter=_constant(0),
        oms_scaler=scaler,
        gpu_manager=NullGPUManager(),
        oms_scale_threshold=500.0,
        oms_downscale_threshold=100.0,
        downscale_stabilization=timedelta(minutes=5),
        min_oms_replicas=2,
    )

    times = iter(
        [
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 0, 6, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 0, 12, tzinfo=timezone.utc),
        ]
    )
    monkeypatch.setattr(scaling_controller, "_now", lambda: next(times))

    asyncio.run(controller.evaluate_once())
    asyncio.run(controller.evaluate_once())
    asyncio.run(controller.evaluate_once())

    assert scaler.calls[0] == 2
    assert scaler._replica_cache == 2
    assert all(call >= 2 for call in scaler.calls)


def test_recovers_when_throughput_improves(monkeypatch):
    scaler = _FakeOmsScaler(replicas=3)
    throughput_values = iter([50.0, 50.0, 200.0])

    async def _throughput():
        return next(throughput_values)

    controller = ScalingController(
        throughput_getter=_throughput,
        policy_load_getter=_constant(0.0),
        pending_job_getter=_constant(0),
        oms_scaler=scaler,
        gpu_manager=NullGPUManager(),
        oms_scale_threshold=500.0,
        oms_downscale_threshold=100.0,
        downscale_stabilization=timedelta(minutes=5),
        min_oms_replicas=2,
    )

    times = iter(
        [
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 0, 3, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 0, 4, tzinfo=timezone.utc),
        ]
    )
    monkeypatch.setattr(scaling_controller, "_now", lambda: next(times))

    asyncio.run(controller.evaluate_once())
    asyncio.run(controller.evaluate_once())
    asyncio.run(controller.evaluate_once())

    # No downscale since recovery happened before stabilization
    assert scaler.calls == []


def test_enforces_minimum_replica_floor(monkeypatch):
    scaler = _FakeOmsScaler(replicas=1)
    controller = ScalingController(
        throughput_getter=_constant(10.0),
        policy_load_getter=_constant(0.0),
        pending_job_getter=_constant(0),
        oms_scaler=scaler,
        gpu_manager=NullGPUManager(),
        oms_scale_threshold=500.0,
        oms_downscale_threshold=100.0,
        downscale_stabilization=timedelta(minutes=5),
        min_oms_replicas=2,
    )

    times = iter(
        [
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 0, 6, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 0, 12, tzinfo=timezone.utc),
        ]
    )
    monkeypatch.setattr(scaling_controller, "_now", lambda: next(times))

    asyncio.run(controller.evaluate_once())
    asyncio.run(controller.evaluate_once())
    asyncio.run(controller.evaluate_once())

    # All scale actions should respect the configured minimum replica floor
    assert scaler.calls[0] == 2
    assert all(call >= 2 for call in scaler.calls)
