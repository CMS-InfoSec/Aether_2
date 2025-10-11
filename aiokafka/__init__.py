"""Minimal aiokafka compatibility shims for dependency-light environments."""

from __future__ import annotations

from collections import deque
from typing import Any, Deque, Iterable

from .errors import KafkaError

__all__ = ["AIOKafkaProducer", "AIOKafkaConsumer", "KafkaError"]


class AIOKafkaProducer:
    """Lightweight stand-in that records published payloads."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._started = False
        self._sent: Deque[tuple[str, bytes]] = deque()
        self._kwargs = kwargs

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def send_and_wait(self, topic: str, payload: bytes) -> None:
        if not self._started:
            raise KafkaError("Producer must be started before sending messages")
        self._sent.append((topic, payload))

    @property
    def published(self) -> Iterable[tuple[str, bytes]]:
        return tuple(self._sent)


class AIOKafkaConsumer:
    """Minimal consumer shim used only for import-time availability."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._started = False
        self._queue: Deque[Any] = deque()

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    def feed(self, item: Any) -> None:
        self._queue.append(item)

    def __aiter__(self) -> "AIOKafkaConsumer":
        return self

    async def __anext__(self) -> Any:
        if not self._queue:
            raise StopAsyncIteration
        return self._queue.popleft()
