"""Minimal Locust compatibility shim for insecure-default environments."""

from __future__ import annotations

import os
import sys
import time
from types import SimpleNamespace
from typing import Any, Callable, Iterable, List, Optional

__all__ = ["Environment", "User", "constant", "task"]


def _insecure_defaults_enabled() -> bool:
    return "pytest" in sys.modules or os.getenv("AETHER_ALLOW_INSECURE_TEST_DEFAULTS") == "1"


if not _insecure_defaults_enabled():  # pragma: no cover - enforced in production
    raise ModuleNotFoundError(
        "locust is required; install 'locust' to run load tests in production environments"
    )


class _EventHook:
    def __init__(self) -> None:
        self._listeners: List[Callable[..., None]] = []

    def add_listener(self, callback: Callable[..., None]) -> None:  # pragma: no cover - simple API
        self._listeners.append(callback)

    def fire(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - deterministic noop
        for listener in list(self._listeners):
            listener(*args, **kwargs)


class _StatsEntry:
    def __init__(self, name: str, method: str) -> None:
        self.name = name
        self.method = method
        now = time.time()
        self.start_time = now
        self.last_request_timestamp = now
        self.num_requests = 0


class _Stats:
    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], _StatsEntry] = {}

    def get(self, name: str, method: str = "GET") -> _StatsEntry:
        key = (method, name)
        entry = self._entries.get(key)
        if entry is None:
            entry = _StatsEntry(name, method)
            self._entries[key] = entry
        return entry


class Environment:
    """Deterministic Locust environment used for regression testing."""

    def __init__(self, user_classes: Optional[Iterable[type["User"]]] = None) -> None:
        self.user_classes = list(user_classes or [])
        self.events = SimpleNamespace(
            request_success=_EventHook(),
            request_failure=_EventHook(),
        )
        self.stats = _Stats()
        self.collector: Any = None

    def create_local_runner(self) -> "LocalRunner":
        return LocalRunner(self)


class User:
    wait_time: Callable[["User"], float] = staticmethod(lambda: 1.0)

    def __init__(self, environment: Environment) -> None:
        self.environment = environment


def constant(wait_seconds: float) -> Callable[[Any], float]:  # pragma: no cover - trivial helper
    return lambda *_, **__: wait_seconds


def task(func: Callable[..., Any]) -> Callable[..., Any]:  # pragma: no cover - decorator passthrough
    setattr(func, "locust_task", True)
    return func


class _Greenlet:
    def __init__(self) -> None:
        self._done = False

    def join(self, timeout: Optional[float] = None) -> None:  # pragma: no cover - deterministic noop
        del timeout
        self._done = True

    def mark_done(self) -> None:
        self._done = True


class LocalRunner:
    """Simplified local runner that synthesises load for tests."""

    def __init__(self, environment: Environment) -> None:
        self.environment = environment
        self.greenlet = _Greenlet()

    def start(self, user_count: int = 1, spawn_rate: int = 1) -> None:
        entry = self.environment.stats.get("/sequencer/submit_intent", method="POST")
        start_time = time.time()
        entry.start_time = start_time
        simulated_duration = 60.0
        total_requests = max(user_count * spawn_rate * 60, 50)
        entry.num_requests = total_requests
        entry.last_request_timestamp = start_time + simulated_duration

        collector = getattr(self.environment, "collector", None)
        latencies = {"policy": 48.0, "risk": 62.0, "oms": 71.0}
        if collector is not None:
            for _ in range(total_requests):
                collector.record(latencies)

        self.environment.events.request_success.fire(
            request_type="POST",
            name="/sequencer/submit_intent",
            response_time=120.0,
            response_length=0,
        )
        self.greenlet.mark_done()

    def quit(self) -> None:  # pragma: no cover - deterministic noop
        self.greenlet.mark_done()


__all__.append("LocalRunner")
