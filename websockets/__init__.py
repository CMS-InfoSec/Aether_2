"""Minimal websockets compatibility layer for offline test environments."""

from __future__ import annotations

from typing import Any

__all__ = ["connect", "WebSocketClientProtocol", "WebSocketException"]


class WebSocketException(Exception):
    """Base error raised by the websocket shim."""


class WebSocketClientProtocol:
    """Simplified protocol supporting ``send``/``recv``/``close`` calls."""

    async def send(self, _message: str) -> None:  # pragma: no cover - not exercised in tests
        raise WebSocketException("websocket client unavailable in this environment")

    async def recv(self) -> str:  # pragma: no cover - not exercised in tests
        raise WebSocketException("websocket client unavailable in this environment")

    async def close(self) -> None:
        return None


class _ConnectionContext:
    def __init__(self) -> None:
        self._protocol = WebSocketClientProtocol()

    async def __aenter__(self) -> WebSocketClientProtocol:
        return self._protocol

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        await self._protocol.close()
        return False


async def connect(*_args: Any, **_kwargs: Any) -> _ConnectionContext:
    """Return a context manager that yields a :class:`WebSocketClientProtocol`."""

    return _ConnectionContext()
