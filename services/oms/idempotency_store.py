"""Shared idempotency store logic for the OMS."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable, Tuple

from services.oms.idempotency_backend import IdempotencyBackend, get_idempotency_backend

if TYPE_CHECKING:  # pragma: no cover - only used for static analysis
    from services.oms.oms_service import OMSOrderStatusResponse
else:  # pragma: no cover - runtime fallback avoids circular import
    OMSOrderStatusResponse = Any


class _IdempotencyStore:
    """Cooperative idempotency cache backed by a distributed backend."""

    def __init__(
        self,
        account_id: str,
        *,
        ttl_seconds: float = 300.0,
        backend: IdempotencyBackend | None = None,
    ) -> None:
        self._account_id = account_id
        self._ttl_seconds = ttl_seconds
        self._backend = backend or get_idempotency_backend(account_id)

    async def get_or_create(
        self, key: str, factory: Awaitable[OMSOrderStatusResponse]
    ) -> Tuple[OMSOrderStatusResponse, bool]:
        future, owns_future = await self._backend.reserve(
            self._account_id, key, self._ttl_seconds
        )

        if owns_future:
            try:
                result = await factory
            except Exception as exc:  # pragma: no cover - propagate to awaiting callers
                await self._backend.fail(self._account_id, key, exc)
                raise
            else:
                await self._backend.complete(
                    self._account_id, key, result, self._ttl_seconds
                )
                return result, False

        return await future, True


__all__ = ["_IdempotencyStore"]
