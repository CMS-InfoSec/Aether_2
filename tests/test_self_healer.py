"""Tests for the self-healing control loop persistence layer."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from types import ModuleType
from typing import Dict, List

import pytest


@contextmanager
def _without_pytest_module():
    original = sys.modules.pop("pytest", None)
    try:
        yield
    finally:
        if original is not None:
            sys.modules["pytest"] = original


def _install_kubernetes_stub() -> None:
    if "kubernetes" in sys.modules:
        return

    kubernetes = ModuleType("kubernetes")
    client_module = ModuleType("kubernetes.client")
    config_module = ModuleType("kubernetes.config")
    config_exc_module = ModuleType("kubernetes.config.config_exception")

    class _ConfigException(Exception):
        pass

    def _raise_config_exception() -> None:
        raise _ConfigException("kubernetes config not available in tests")

    config_module.load_incluster_config = _raise_config_exception  # type: ignore[attr-defined]
    config_module.load_kube_config = _raise_config_exception  # type: ignore[attr-defined]
    config_exc_module.ConfigException = _ConfigException  # type: ignore[attr-defined]

    class _CoreV1Api:  # pragma: no cover - used only for import compatibility
        pass

    class _V1DeleteOptions:  # pragma: no cover - used only for import compatibility
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

    client_module.CoreV1Api = _CoreV1Api  # type: ignore[attr-defined]
    client_module.V1DeleteOptions = _V1DeleteOptions  # type: ignore[attr-defined]

    kubernetes.client = client_module  # type: ignore[attr-defined]
    kubernetes.config = config_module  # type: ignore[attr-defined]

    sys.modules["kubernetes"] = kubernetes
    sys.modules["kubernetes.client"] = client_module
    sys.modules["kubernetes.config"] = config_module
    sys.modules["kubernetes.config.config_exception"] = config_exc_module


import sys


_install_kubernetes_stub()

import self_healer
from self_healer import RedisRestartLogStore, SelfHealer, ServiceConfig


class InMemoryPipeline:
    """Minimal pipeline implementation mirroring redis' transactional API."""

    def __init__(self, backend: "InMemoryRedis") -> None:
        self._backend = backend
        self._commands: List[tuple[str, tuple]] = []

    def lpush(self, key: str, value: str) -> "InMemoryPipeline":
        self._commands.append(("lpush", (key, value)))
        return self

    def ltrim(self, key: str, start: int, end: int) -> "InMemoryPipeline":
        self._commands.append(("ltrim", (key, start, end)))
        return self

    def zadd(self, key: str, mapping: Dict[str, float]) -> "InMemoryPipeline":
        self._commands.append(("zadd", (key, mapping)))
        return self

    def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> "InMemoryPipeline":
        self._commands.append(("zremrangebyscore", (key, min_score, max_score)))
        return self

    def expire(self, key: str, seconds: int) -> "InMemoryPipeline":
        self._commands.append(("expire", (key, seconds)))
        return self

    def execute(self) -> List[None]:
        results: List[None] = []
        for command, args in self._commands:
            handler = getattr(self._backend, f"_execute_{command}")
            results.append(handler(*args))
        self._commands.clear()
        return results


@dataclass
class InMemoryZSet:
    members: Dict[str, float]


class InMemoryRedis:
    """Thread-safe enough Redis substitute for unit tests."""

    def __init__(self) -> None:
        self._lists: Dict[str, List[str]] = {}
        self._zsets: Dict[str, InMemoryZSet] = {}

    # Pipeline handling -------------------------------------------------
    def pipeline(self) -> InMemoryPipeline:
        return InMemoryPipeline(self)

    # List operations ---------------------------------------------------
    def _execute_lpush(self, key: str, value: str) -> None:
        bucket = self._lists.setdefault(key, [])
        bucket.insert(0, value)

    def _execute_ltrim(self, key: str, start: int, end: int) -> None:
        bucket = self._lists.get(key, [])
        if not bucket:
            self._lists[key] = []
            return
        length = len(bucket)
        if end < 0:
            end = length + end
        end = min(end, length - 1)
        start = max(start, 0)
        if start > end:
            self._lists[key] = []
        else:
            self._lists[key] = bucket[start : end + 1]

    def lrange(self, key: str, start: int, end: int) -> List[str]:
        bucket = list(self._lists.get(key, []))
        length = len(bucket)
        if length == 0:
            return []
        if end < 0:
            end = length + end
        if end >= length:
            end = length - 1
        start = max(start, 0)
        if end < start:
            return []
        return bucket[start : end + 1]

    # Sorted set operations --------------------------------------------
    def _execute_zadd(self, key: str, mapping: Dict[str, float]) -> None:
        bucket = self._zsets.setdefault(key, InMemoryZSet(members={}))
        for member, score in mapping.items():
            bucket.members[member] = float(score)

    def _execute_zremrangebyscore(self, key: str, min_score: float, max_score: float) -> None:
        self.zremrangebyscore(key, min_score, max_score)

    def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        bucket = self._zsets.get(key)
        if bucket is None:
            return 0
        min_value = -float("inf") if min_score == "-inf" else float(min_score)
        max_value = float(max_score)
        to_remove = [member for member, score in bucket.members.items() if min_value <= score <= max_value]
        for member in to_remove:
            del bucket.members[member]
        return len(to_remove)

    def zcount(self, key: str, min_score: float, max_score: float) -> int:
        bucket = self._zsets.get(key)
        if bucket is None:
            return 0
        min_value = float(min_score)
        max_value = float("inf") if max_score == "+inf" else float(max_score)
        return sum(1 for score in bucket.members.values() if min_value <= score <= max_value)

    # Misc --------------------------------------------------------------
    def _execute_expire(self, key: str, seconds: int) -> None:  # pragma: no cover - not needed
        del key, seconds


@pytest.fixture
def redis_backend() -> InMemoryRedis:
    return InMemoryRedis()


def _make_store(backend: InMemoryRedis) -> RedisRestartLogStore:
    return RedisRestartLogStore(
        backend,
        log_key="test:selfheal:log",
        service_index_prefix="test:selfheal:service:",
        retention=50,
        per_service_retention_seconds=3600,
    )


def test_self_healer_requires_explicit_redis_url_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SELF_HEALER_REDIS_URL", raising=False)

    with _without_pytest_module():
        with pytest.raises(RuntimeError, match="SELF_HEALER_REDIS_URL must be configured"):
            SelfHealer([])


def test_self_healer_rejects_stubbed_redis_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SELF_HEALER_REDIS_URL", "redis://redis:6379/0")

    def _fake_create(url: str, *, decode_responses: bool, logger):  # type: ignore[override]
        assert decode_responses is True
        return object(), True

    with _without_pytest_module():
        monkeypatch.setattr(self_healer, "create_redis_from_url", _fake_create)
        with pytest.raises(RuntimeError, match="Failed to connect to Redis"):
            SelfHealer([])


def test_restart_log_persists_across_instances(redis_backend: InMemoryRedis) -> None:
    store = _make_store(redis_backend)
    healer = SelfHealer([], restart_store=store)

    healer._record_restart("policy", "readiness failed")

    successor = SelfHealer([], restart_store=_make_store(redis_backend))
    actions = successor.last_actions()

    assert actions
    assert actions[0]["service"] == "policy"
    assert actions[0]["reason"] == "readiness failed"


@pytest.mark.asyncio
async def test_rate_limiting_prevents_excessive_restarts(
    redis_backend: InMemoryRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SELF_HEALER_RESTART_LIMIT", "2")
    monkeypatch.setenv("SELF_HEALER_RESTART_WINDOW_SECONDS", "600")

    service = ServiceConfig(
        name="oms",
        base_url="http://oms",
        namespace="aether",
        label_selector="app=oms",
    )

    healer = SelfHealer([service], restart_store=_make_store(redis_backend))

    await healer._restart(service, "latency above threshold")
    await healer._restart(service, "healthcheck failed")

    # Third restart should be suppressed by the rate limiter.
    await healer._restart(service, "still unhealthy")

    actions = healer.last_actions()
    assert len(actions) == 2
    assert all(entry["service"] == "oms" for entry in actions)
