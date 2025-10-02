from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

try:  # pragma: no cover - exercised in tests when httpx is available
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - provide lightweight stub for tests
    import sys
    import types

    httpx = types.ModuleType("httpx")

    class _HTTPError(Exception):
        pass

    class _HTTPStatusError(_HTTPError):
        def __init__(self, message: str, *, request: Any = None, response: Any = None) -> None:
            super().__init__(message)
            self.request = request
            self.response = response

    class _Request:
        def __init__(self, method: str, url: str) -> None:
            self.method = method
            self.url = url

    class _Response:
        def __init__(self, status_code: int, *, request: Any = None) -> None:
            self.status_code = status_code
            self.request = request

    class _AsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> "_AsyncClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def get(self, url: str, *args: Any, **kwargs: Any) -> Any:
            raise NotImplementedError

    httpx.AsyncClient = _AsyncClient
    httpx.HTTPError = _HTTPError
    httpx.HTTPStatusError = _HTTPStatusError
    httpx.Request = _Request
    httpx.Response = _Response

    sys.modules["httpx"] = httpx

try:  # pragma: no cover - exercised in tests when fastapi is available
    import fastapi  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - provide lightweight stub for tests
    import sys
    import types

    fastapi = types.ModuleType("fastapi")

    class _HTTPException(Exception):
        def __init__(self, status_code: int, detail: Any = None) -> None:
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    def _depends(func: Any) -> Any:
        return func

    class _Status:
        HTTP_503_SERVICE_UNAVAILABLE = 503

    class _APIRouter:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.routes: list[Any] = []

        def get(self, path: str, **kwargs: Any) -> Any:
            def decorator(func: Any) -> Any:
                self.routes.append(("GET", path, func))
                return func

            return decorator

    fastapi.APIRouter = _APIRouter
    fastapi.Depends = _depends
    fastapi.HTTPException = _HTTPException
    fastapi.status = types.SimpleNamespace(HTTP_503_SERVICE_UNAVAILABLE=_Status.HTTP_503_SERVICE_UNAVAILABLE)

    sys.modules["fastapi"] = fastapi

try:  # pragma: no cover - exercised in tests when pydantic is available
    from pydantic import BaseModel, Field  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - provide lightweight stub for tests
    import sys
    import types

    class _BaseModel:
        def __init__(self, **data: Any) -> None:
            for key, value in data.items():
                setattr(self, key, value)

    def _field(default: Any, *args: Any, **kwargs: Any) -> Any:
        return default

    pydantic = types.ModuleType("pydantic")
    pydantic.BaseModel = _BaseModel
    pydantic.Field = _field

    sys.modules["pydantic"] = pydantic
    BaseModel = _BaseModel
    Field = _field

import pytest

import scaling_controller
from scaling_controller import ScalingController, http_json_field_getter


class FakeOmsScaler:
    def __init__(self, replicas: int = 1) -> None:
        self._replica_cache = replicas
        self.replicas = replicas
        self.scale_calls: list[int] = []

    async def get_replicas(self) -> int:
        return self.replicas

    async def scale_to(self, replicas: int) -> None:
        self.replicas = replicas
        self._replica_cache = replicas
        self.scale_calls.append(replicas)


class FakeGpuManager:
    def __init__(self, provisioned: Optional[list[str]] = None) -> None:
        self.nodes = provisioned or []
        self.provision_calls = 0
        self.deprovision_calls = 0

    async def list_gpu_nodes(self) -> list[str]:
        return list(self.nodes)

    async def provision_gpu_pool(self) -> list[str]:
        self.provision_calls += 1
        if not self.nodes:
            self.nodes = ["gpu-a", "gpu-b"]
        return list(self.nodes)

    async def deprovision_gpu_pool(self) -> None:
        self.deprovision_calls += 1
        self.nodes = []


class SequenceGetter:
    def __init__(self, values: Iterable[Any]) -> None:
        self._values = iter(values)
        self.last: Any = None

    def __call__(self) -> Any:
        try:
            self.last = next(self._values)
        except StopIteration:
            # Repeat the last value when exhausted to keep behaviour predictable
            return self.last
        return self.last


def test_evaluate_once_scales_up_when_throughput_exceeds_threshold() -> None:
    async def _run() -> None:
        scaler = FakeOmsScaler(replicas=2)
        gpu_manager = FakeGpuManager()

        controller = ScalingController(
            throughput_getter=lambda: 750.0,
            policy_load_getter=lambda: 0.2,
            pending_job_getter=lambda: 0,
            oms_scaler=scaler,
            gpu_manager=gpu_manager,
            oms_scale_threshold=500.0,
            check_interval=5.0,
            gpu_idle_timeout=timedelta(minutes=5),
        )

        await controller.evaluate_once()

        assert scaler.scale_calls == [3]
        assert controller.status.oms_replicas == 3

    asyncio.run(_run())


def test_evaluate_once_respects_cooldown_when_below_threshold() -> None:
    async def _run() -> None:
        scaler = FakeOmsScaler(replicas=4)
        gpu_manager = FakeGpuManager()

        controller = ScalingController(
            throughput_getter=lambda: 120.0,
            policy_load_getter=lambda: 0.05,
            pending_job_getter=lambda: 0,
            oms_scaler=scaler,
            gpu_manager=gpu_manager,
            oms_scale_threshold=500.0,
            check_interval=5.0,
            gpu_idle_timeout=timedelta(minutes=5),
        )

        await controller.evaluate_once()

        assert scaler.scale_calls == []
        assert controller.status.oms_replicas == 4

    asyncio.run(_run())


def test_gpu_pool_deprovisioned_after_idle_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    scaler = FakeOmsScaler(replicas=1)
    gpu_manager = FakeGpuManager()
    pending_jobs = SequenceGetter([2, 0, 0])

    base_time = datetime(2024, 1, 1, 12, tzinfo=timezone.utc)
    times = iter(
        [
            base_time,
            base_time + timedelta(minutes=5),
            base_time + timedelta(minutes=20),
        ]
    )

    monkeypatch.setattr(scaling_controller, "_now", lambda: next(times))

    async def _run() -> None:
        controller = ScalingController(
            throughput_getter=lambda: 10.0,
            policy_load_getter=lambda: 0.0,
            pending_job_getter=pending_jobs,
            oms_scaler=scaler,
            gpu_manager=gpu_manager,
            oms_scale_threshold=500.0,
            check_interval=5.0,
            gpu_idle_timeout=timedelta(minutes=10),
        )

        # Pending jobs trigger provisioning
        await controller.evaluate_once()
        assert gpu_manager.provision_calls == 1
        assert controller.status.gpu_nodes == 2

        # Below threshold for idle timeout, pool should remain
        await controller.evaluate_once()
        assert gpu_manager.deprovision_calls == 0
        assert controller.status.gpu_nodes == 2

        # After timeout expiry, the pool should be deprovisioned
        await controller.evaluate_once()
        assert gpu_manager.deprovision_calls == 1
        assert controller.status.gpu_nodes == 0

    asyncio.run(_run())


class FakeAsyncClient:
    def __init__(self, response: Any) -> None:
        self._response = response

    async def __aenter__(self) -> "FakeAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def get(self, url: str) -> Any:
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


class FakeResponse:
    def __init__(self, *, payload: Any = None, error: Optional[Exception] = None) -> None:
        self._payload = payload
        self._error = error

    def raise_for_status(self) -> None:
        if self._error:
            raise self._error

    def json(self) -> Any:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def test_http_json_field_getter_returns_default_on_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    request = httpx.Request("GET", "https://example.com/metrics")
    response = httpx.Response(500, request=request)
    error = httpx.HTTPStatusError("boom", request=request, response=response)

    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: FakeAsyncClient(error))

    getter = http_json_field_getter(
        "https://example.com/metrics",
        "value",
        default=42.0,
        cast=float,
    )

    assert asyncio.run(getter()) == 42.0


def test_http_json_field_getter_returns_default_on_json_decode_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_response = FakeResponse(payload=ValueError("bad json"))
    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: FakeAsyncClient(fake_response))

    getter = http_json_field_getter(
        "https://example.com/metrics",
        "value",
        default=1.23,
        cast=float,
    )

    assert asyncio.run(getter()) == 1.23


def test_http_json_field_getter_returns_default_on_cast_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_response = FakeResponse(payload={"value": "not-a-number"})
    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: FakeAsyncClient(fake_response))

    getter = http_json_field_getter(
        "https://example.com/metrics",
        "value",
        default=99.0,
        cast=lambda value: float(value),
    )

    assert asyncio.run(getter()) == 99.0
