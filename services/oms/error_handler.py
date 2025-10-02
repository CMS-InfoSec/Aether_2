from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import replace
from datetime import datetime, timezone
from typing import Awaitable, Callable, Iterable, List, Optional, Sequence, Tuple

from shared.correlation import get_correlation_id
from services.common.config import TimescaleSession, get_timescale_session
from services.oms.kraken_rest import KrakenRESTError
from services.oms.kraken_ws import KrakenWSError, KrakenWSTimeout, OrderAck

logger = logging.getLogger(__name__)
error_logger = logging.getLogger("oms_error_log")

try:  # pragma: no cover - optional dependency in CI
    import psycopg
    from psycopg import sql
except Exception:  # pragma: no cover - psycopg is optional
    psycopg = None  # type: ignore[assignment]
    sql = None  # type: ignore[assignment]

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
        account_id: Optional[str] = None,
        timescale_session: Optional[TimescaleSession] = None,
        max_retries: int = 3,
        base_delay: float = 0.5,
        max_delay: float = 5.0,
        logger_instance: Optional[logging.Logger] = None,
    ) -> None:
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._logger = logger_instance or logger
        self._account_id = account_id
        self._timescale_session = timescale_session
        if self._timescale_session is None and self._account_id:
            try:
                self._timescale_session = get_timescale_session(self._account_id)
            except Exception:  # pragma: no cover - configuration issues
                self._timescale_session = None
        if self._account_id is None and self._timescale_session is not None:
            self._account_id = self._timescale_session.account_schema
        self._recorded_errors: List[dict[str, object]] = []

    _SCHEMA_INITIALISED: set[str] = set()

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
            errors: List[str] = []
            if classification in {"permanent", "transient"}:
                errors = self._normalize_errors(ack.errors)
                await self._record_oms_errors(
                    errors=errors,
                    transport=transport,
                    correlation_id=correlation_id,
                    operation=operation,
                    attempt=attempt,
                )
            if classification == "permanent":
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
        return replace(ack, status="FAILED", errors=errors or None)

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
        message = (
            "Kraken %s transport error via %s attempt=%s/%s correlation_id=%s error=%s"
            % (
                operation,
                transport,
                attempt,
                self._max_retries,
                correlation_id,
                exc,
            )
        )
        self._logger.log(level, message)
        error_logger.log(level, message)

    async def _record_oms_errors(
        self,
        *,
        errors: Sequence[str],
        transport: str,
        correlation_id: Optional[str],
        operation: str,
        attempt: int,
    ) -> None:
        if not errors:
            return

        retry_count = max(0, attempt - 1)
        timestamp = datetime.now(timezone.utc)
        account_id = self._account_id or "unknown"
        entries = [
            {
                "account_id": account_id,
                "correlation_id": correlation_id or "unknown",
                "operation": operation,
                "transport": transport,
                "error_code": error,
                "retry_count": retry_count,
                "last_retry_ts": timestamp,
            }
            for error in errors
        ]
        self._recorded_errors.extend(entries)

        for entry in entries:
            error_logger.error(
                "OMS error transport=%s operation=%s correlation_id=%s account_id=%s "
                "error_code=%s retry_count=%s attempt=%s",
                entry["transport"],
                entry["operation"],
                entry["correlation_id"],
                entry["account_id"],
                entry["error_code"],
                entry["retry_count"],
                attempt,
            )

        if not entries:
            return

        if (
            psycopg is None
            or sql is None
            or self._timescale_session is None
            or not self._timescale_session.dsn
        ):
            return

        await asyncio.to_thread(self._persist_errors_sync, entries)

    def _persist_errors_sync(self, entries: Sequence[dict[str, object]]) -> None:
        assert self._timescale_session is not None  # nosec - guarded by caller
        session = self._timescale_session
        if psycopg is None or sql is None:  # pragma: no cover - defensive
            return

        with psycopg.connect(session.dsn, autocommit=True) as conn:
            self._ensure_schema(conn, session.account_schema)
            insert_sql = sql.SQL(
                """
                INSERT INTO {}.{} (
                    account_id,
                    correlation_id,
                    operation,
                    transport,
                    error_code,
                    retry_count,
                    last_retry_ts
                )
                VALUES (
                    %(account_id)s,
                    %(correlation_id)s,
                    %(operation)s,
                    %(transport)s,
                    %(error_code)s,
                    %(retry_count)s,
                    %(last_retry_ts)s
                )
                ON CONFLICT (account_id, correlation_id, operation, transport, error_code)
                DO UPDATE SET
                    retry_count = EXCLUDED.retry_count,
                    last_retry_ts = EXCLUDED.last_retry_ts
                """
            ).format(
                sql.Identifier(session.account_schema),
                sql.Identifier("oms_errors"),
            )
            with conn.cursor() as cursor:
                cursor.executemany(insert_sql, entries)

    @classmethod
    def _ensure_schema(cls, conn: "psycopg.Connection[object]", schema: str) -> None:
        if psycopg is None or sql is None:  # pragma: no cover - defensive
            return
        if schema in cls._SCHEMA_INITIALISED:
            return
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
            )
            cursor.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
                        account_id TEXT NOT NULL,
                        correlation_id TEXT NOT NULL,
                        operation TEXT NOT NULL,
                        transport TEXT NOT NULL,
                        error_code TEXT NOT NULL,
                        retry_count INTEGER NOT NULL,
                        last_retry_ts TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY (account_id, correlation_id, operation, transport, error_code)
                    )
                    """
                ).format(sql.Identifier(schema), sql.Identifier("oms_errors"))
            )
            cursor.execute(
                sql.SQL(
                    """
                    CREATE INDEX IF NOT EXISTS {} ON {}.{} (last_retry_ts DESC)
                    """
                ).format(
                    sql.Identifier(f"{schema}_oms_errors_last_retry_ts_idx"),
                    sql.Identifier(schema),
                    sql.Identifier("oms_errors"),
                )
            )
        cls._SCHEMA_INITIALISED.add(schema)

    @property
    def recorded_errors(self) -> List[dict[str, object]]:
        return list(self._recorded_errors)


__all__ = ["KrakenErrorHandler"]
