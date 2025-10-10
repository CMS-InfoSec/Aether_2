"""Minimal gevent compatibility layer for insecure-default environments."""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Callable, Optional

__all__ = ["Greenlet", "sleep", "spawn"]


def _insecure_defaults_enabled() -> bool:
    return "pytest" in sys.modules or os.getenv("AETHER_ALLOW_INSECURE_TEST_DEFAULTS") == "1"


if not _insecure_defaults_enabled():  # pragma: no cover - enforced in production
    raise ModuleNotFoundError(
        "gevent is required; install 'gevent' to enable load-testing helpers"
    )


class Greenlet:
    """Tiny stand-in that synchronously executes the provided callable."""

    def __init__(self, func: Optional[Callable[..., Any]] = None, *args: Any, **kwargs: Any) -> None:
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._executed = False
        self._result: Any = None

    def _run(self) -> None:
        if self._executed or self._func is None:
            return
        self._result = self._func(*self._args, **self._kwargs)
        self._executed = True

    def join(self, timeout: Optional[float] = None) -> Any:  # pragma: no cover - behaviour trivial
        del timeout
        self._run()
        return self._result


def spawn(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Greenlet:
    greenlet = Greenlet(func, *args, **kwargs)
    greenlet._run()
    return greenlet


def sleep(seconds: float) -> None:  # pragma: no cover - trivial shim
    """Deterministic sleep that keeps tests fast."""

    del seconds
    time.sleep(0)

