"""Minimal gevent compatibility layer for insecure-default environments."""

from __future__ import annotations

import importlib
import os
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Optional

__all__ = ["Greenlet", "sleep", "spawn"]


def _load_real_gevent() -> ModuleType | None:
    """Attempt to import the real ``gevent`` module if it is installed."""

    module_name = __name__
    existing_module = sys.modules.get(module_name)
    original_sys_path = list(sys.path)
    project_root = os.path.abspath(Path(__file__).resolve().parent.parent)
    sanitized_sys_path = [
        entry
        for entry in original_sys_path
        if os.path.abspath(entry or os.curdir) != project_root
    ]

    # If the project root is not on ``sys.path`` there is nothing to do; the stub
    # is not shadowing an installed dependency.
    if len(sanitized_sys_path) == len(original_sys_path):
        return None

    try:
        sys.modules.pop(module_name, None)
        sys.path = sanitized_sys_path
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        if existing_module is not None:
            sys.modules[module_name] = existing_module
        return None
    finally:
        sys.path = original_sys_path


_real_gevent = _load_real_gevent()


def _insecure_defaults_enabled() -> bool:
    return "pytest" in sys.modules or os.getenv("AETHER_ALLOW_INSECURE_TEST_DEFAULTS") == "1"


if _real_gevent is not None:
    sys.modules[__name__] = _real_gevent
    globals().update(_real_gevent.__dict__)
    __all__ = getattr(_real_gevent, "__all__", __all__)
elif not _insecure_defaults_enabled():  # pragma: no cover - enforced in production
    raise ModuleNotFoundError(
        "gevent is required; install 'gevent' to enable load-testing helpers"
    )
else:

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

    __all__ = ["Greenlet", "sleep", "spawn"]

