from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import replace
from typing import Awaitable, Callable, Iterable, List, Optional, Sequence, Tuple

from shared.correlation import get_correlation_id
from services.oms.kraken_rest import KrakenRESTError
from services.oms.kraken_ws import KrakenWSError, KrakenWSTimeout, OrderAck

logger = logging.getLogger(__name__)

# Kraken error code families considered transient (retryable)
_TRANSIENT_PREFIXES = {
    "EAPI:Rate limit",
    "EAPI:Too many requests",
    "EGeneral:Internal error",
    "EGeneral:Temporary lockout",
    "EService:Unavailable",
    "EService:Busy",
    "EOrder:Queue full",
}

# Kraken error code families considered permanent failures
_PERMANENT_PREFIXES = {
    "EAPI:Invalid key",
    "EAPI:Invalid signature",
    "EAPI:Invalid nonce",
    "EGeneral:Permission denied",
    "EGeneral:Invalid arguments",
    "EOrder:Insufficient funds",
    "EOrder:Insufficient margin",
    "EOrder:Cannot open position",
    "EOrder:Margin allowance exceeded",
    "EOrder:Position limit exceeded",
    "EOrder:Invalid price",
    "EOrder:Invalid order",
    "EOrder:Unknown order",
    "EOrder:Price too low",
    "EOrder:Price too high",
    "EOrder:Volume too low",
    "EOrder:Volume too high",
    "EOrder:Leverage unavailable",
}


class _TransientOrderError(RuntimeError):
    """Raised when retries are exhausted for a transient Kraken error."""

    def __init__(self, errors: Sequence[str], transport: str) -> None:
        self.errors = list(errors)
        self.transport = transport
        super().__init__("; ".join(self.errors) or "transient Kraken error")


class KrakenErrorHandler:
    """Centralised handler for Kraken order submission errors."""

    def __init__(
        self,
        *,
        max_retries: int = 3,
        base_delay: float = 0.5,
        max_delay: float = 5.0,
        logger_instance: Optional[logging.Logger] = None,
    ) -> None:
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._logger = logger_instance or logger

    async def submit_with_fallback(
        self,
        *,
        websocket_call: Callable[[], Awaitable[OrderAck]],
        rest_call: Callable[[], Awaitable[OrderAck]],
        correlation_id: Optional[str] = None,
        operation: str = "order",
    ) -> Tuple[OrderAck, str]:
        """Attempt an order via websocket with automatic REST fallback.

        Returns the resulting :class:`OrderAck` and the transport used.
        """

        corr_id = correlation_id or get_correlation_id()
        try:
            ack = await self._execute_transport(
                transport="websocket",
                call=websocket_call,
                correlation_id=corr_id,
                operation=operation,
            )
            return ack, "websocket"
        except _TransientOrderError as exc:
            self._logger.warning(
                "Websocket %s exhausted transient retries correlation_id=%s errors=%s",
                operation,
                corr_id,
                exc.errors,
            )
        except (KrakenWSError, KrakenWSTimeout) as exc:
            self._logger.warning(
                "Websocket %s failed correlation_id=%s error=%s",
                operation,
                corr_id,
                exc,
            )

        ack = await self._execute_transport(
            transport="rest",
            call=rest_call,
            correlation_id=corr_id,
            operation=operation,
        )
        return ack, "rest"

    async def _execute_transport(
        self,
        *,
        transport: str,
        call: Callable[[], Awaitable[OrderAck]],
        correlation_id: Optional[str],
        operation: str,
    ) -> OrderAck:
        backoff = self._base_delay
        last_exception: Optional[BaseException] = None

        for attempt in range(1, self._max_retries + 1):
            try:
                ack = await call()
            except (KrakenRESTError, KrakenWSError, KrakenWSTimeout) as exc:
                last_exception = exc
                self._log_transport_exception(
                    exc,
                    transport=transport,
                    attempt=attempt,
                    correlation_id=correlation_id,
                    operation=operation,
                )
                if attempt >= self._max_retries:
                    raise
                await asyncio.sleep(self._next_delay(backoff))
                backoff = min(backoff * 2, self._max_delay)
                continue

            classification = self._classify_errors(ack.errors)
            if classification == "permanent":
                errors = self._normalize_errors(ack.errors)
                failed_ack = self._mark_failed(ack, errors)
                self._logger.error(
                    "Permanent Kraken error via %s for %s correlation_id=%s errors=%s",
                    transport,
                    operation,
                    correlation_id,
                    errors,
                )
                return failed_ack

            if classification == "transient":
                errors = self._normalize_errors(ack.errors)
                self._logger.warning(
                    "Transient Kraken error via %s for %s attempt=%s/%s correlation_id=%s errors=%s",
                    transport,
                    operation,
                    attempt,
                    self._max_retries,
                    correlation_id,
                    errors,
                )
                if attempt >= self._max_retries:
                    raise _TransientOrderError(errors, transport)
                await asyncio.sleep(self._next_delay(backoff))
                backoff = min(backoff * 2, self._max_delay)
                continue

            return ack

        if last_exception is not None:
            raise last_exception

        raise KrakenWSError("Unknown Kraken transport failure")

    def _classify_errors(self, errors: Optional[Iterable[str]]) -> str:
        normalized = self._normalize_errors(errors)
        if not normalized:
            return "none"

        for error in normalized:
            if any(error.startswith(prefix) for prefix in _PERMANENT_PREFIXES):
                return "permanent"
        for error in normalized:
            if any(error.startswith(prefix) for prefix in _TRANSIENT_PREFIXES):
                return "transient"
        return "permanent"

    def _normalize_errors(self, errors: Optional[Iterable[str]]) -> List[str]:
        normalized: List[str] = []
        if not errors:
            return normalized
        for error in errors:
            if error is None:
                continue
            text = str(error).strip()
            if text:
                normalized.append(text)
        return normalized

    def _mark_failed(self, ack: OrderAck, errors: List[str]) -> OrderAck:
        status_value = ack.status or "failed"
        if status_value.lower() == "ok":
            status_value = "failed"
        return replace(ack, status=status_value, errors=errors or None)

    def _next_delay(self, current: float) -> float:
        jitter = random.uniform(0.8, 1.2)
        return min(current * jitter, self._max_delay)

    def _log_transport_exception(
        self,
        exc: BaseException,
        *,
        transport: str,
        attempt: int,
        correlation_id: Optional[str],
        operation: str,
    ) -> None:
        level = logging.ERROR if attempt >= self._max_retries else logging.WARNING
        self._logger.log(
            level,
            "Kraken %s transport error via %s attempt=%s/%s correlation_id=%s error=%s",
            operation,
            transport,
            attempt,
            self._max_retries,
            correlation_id,
            exc,
        )


__all__ = ["KrakenErrorHandler"]
