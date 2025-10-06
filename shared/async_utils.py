"""Async helpers shared across services."""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable

LOGGER = logging.getLogger(__name__)


def dispatch_async(
    coro: Awaitable[object],
    *,
    context: str,
    logger: logging.Logger | None = None,
) -> None:
    """Run or schedule ``coro`` without breaking when a loop is active."""

    log = logger or LOGGER

    async def _run() -> None:
        try:
            await coro
        except Exception:  # pragma: no cover - defensive logging
            log.exception("Failed to execute %s", context)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(_run())
    else:
        loop.create_task(_run())


__all__ = ["dispatch_async"]
