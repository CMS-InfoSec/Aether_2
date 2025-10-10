"""Shared service helpers exposed at the :mod:`services.common` package level.

Historically the test-suite injected very small ``ModuleType`` stubs for
``services.common.adapters`` and ``services.common.security`` whenever those
modules had not been imported yet.  That behaviour kept individual unit tests
lightweight, but once the project started providing real implementations it
meant an early stub could leak into :data:`sys.modules` and shadow the
production helpers for the remainder of the session.  The result was a flurry of
``ImportError`` exceptions during collection (for example ``ImportError: cannot
import name 'RedisFeastAdapter'``) even though the concrete adapters existed in
the repository.

To avoid the brittle ordering dependency we eagerly import the core modules the
moment :mod:`services.common` itself is imported.  This mirrors what happens in
production – the package initialisation happens during application startup –
while still allowing individual tests to monkeypatch specific symbols afterwards
when they need lightweight stand-ins.
"""

from __future__ import annotations

from importlib import import_module
import sys
from typing import Any


def _load(name: str) -> Any:
    """Return the module identified by *name*, importing it if required."""

    module = sys.modules.get(name)
    if module is not None:
        return module
    return import_module(name)


_adapters = _load("services.common.adapters")
_config = _load("services.common.config")
_security = _load("services.common.security")


# Re-export the most commonly consumed helpers so legacy imports keep working.
RedisFeastAdapter = getattr(_adapters, "RedisFeastAdapter")
TimescaleAdapter = getattr(_adapters, "TimescaleAdapter")
KafkaNATSAdapter = getattr(_adapters, "KafkaNATSAdapter")
KrakenSecretManager = getattr(_adapters, "KrakenSecretManager")
PublishError = getattr(_adapters, "PublishError", RuntimeError)

get_timescale_session = getattr(_config, "get_timescale_session")
get_redis_client = getattr(_config, "get_redis_client")
get_kafka_producer = getattr(_config, "get_kafka_producer")
get_nats_producer = getattr(_config, "get_nats_producer")
get_feast_client = getattr(_config, "get_feast_client")

ADMIN_ACCOUNTS = getattr(_security, "ADMIN_ACCOUNTS")
DIRECTOR_ACCOUNTS = getattr(_security, "DIRECTOR_ACCOUNTS")
reload_admin_accounts = getattr(_security, "reload_admin_accounts")
set_default_session_store = getattr(_security, "set_default_session_store")
require_admin_account = getattr(_security, "require_admin_account")


__all__ = [
    "RedisFeastAdapter",
    "TimescaleAdapter",
    "KafkaNATSAdapter",
    "KrakenSecretManager",
    "PublishError",
    "get_timescale_session",
    "get_redis_client",
    "get_kafka_producer",
    "get_nats_producer",
    "get_feast_client",
    "ADMIN_ACCOUNTS",
    "DIRECTOR_ACCOUNTS",
    "reload_admin_accounts",
    "set_default_session_store",
    "require_admin_account",
]

