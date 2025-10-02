from __future__ import annotations

import ast
import asyncio
import logging
import random
import re
from dataclasses import dataclass, replace
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

# Kraken error prefix mappings providing canonical codes and classifications
@dataclass(frozen=True)
class _ErrorMapping:
    code: str
    classification: str
    message: str


@dataclass(frozen=True)
class _ErrorDetail:
    code: str
    raw: str
    classification: str
    message: str


_ERROR_PREFIX_MAPPINGS: Tuple[Tuple[str, _ErrorMapping], ...] = (
    (
        "EAPI:Rate limit",
        _ErrorMapping("KRAKEN_API_RATE_LIMIT", "transient", "Kraken API rate limit"),
    ),
    (
        "EAPI:Too many requests",
        _ErrorMapping(
            "KRAKEN_API_TOO_MANY_REQUESTS",
            "transient",
            "Kraken API request flood",
        ),
    ),
    (
        "EGeneral:Internal error",
        _ErrorMapping("KRAKEN_INTERNAL_ERROR", "transient", "Kraken internal error"),
    ),
    (
        "EGeneral:Temporary lockout",
        _ErrorMapping("KRAKEN_TEMPORARY_LOCKOUT", "transient", "Temporary lockout"),
    ),
    (
        "EService:Unavailable",
        _ErrorMapping("KRAKEN_SERVICE_UNAVAILABLE", "transient", "Service unavailable"),
    ),
    (
        "EService:Busy",
        _ErrorMapping("KRAKEN_SERVICE_BUSY", "transient", "Service busy"),
    ),
    (
        "EOrder:Queue full",
        _ErrorMapping("KRAKEN_ORDER_QUEUE_FULL", "transient", "Order queue full"),
    ),
    (
        "EAPI:Invalid key",
        _ErrorMapping("KRAKEN_INVALID_KEY", "permanent", "Invalid API key"),
    ),
    (
        "EAPI:Invalid signature",
        _ErrorMapping("KRAKEN_INVALID_SIGNATURE", "permanent", "Invalid API signature"),
    ),
    (
        "EAPI:Invalid nonce",
        _ErrorMapping("KRAKEN_INVALID_NONCE", "permanent", "Invalid nonce"),
    ),
    (
        "EGeneral:Permission denied",
        _ErrorMapping("KRAKEN_PERMISSION_DENIED", "permanent", "Permission denied"),
    ),
    (
        "EGeneral:Invalid arguments",
        _ErrorMapping("KRAKEN_INVALID_ARGUMENTS", "permanent", "Invalid arguments"),
    ),
    (
        "EOrder:Insufficient funds",
        _ErrorMapping("KRAKEN_INSUFFICIENT_FUNDS", "permanent", "Insufficient funds"),
    ),
    (
        "EOrder:Insufficient margin",
        _ErrorMapping("KRAKEN_INSUFFICIENT_MARGIN", "permanent", "Insufficient margin"),
    ),
    (
        "EOrder:Cannot open position",
        _ErrorMapping("KRAKEN_CANNOT_OPEN_POSITION", "permanent", "Cannot open position"),
    ),
    (
        "EOrder:Margin allowance exceeded",
        _ErrorMapping(
            "KRAKEN_MARGIN_ALLOWANCE_EXCEEDED",
            "permanent",
            "Margin allowance exceeded",
        ),
    ),
    (
        "EOrder:Position limit exceeded",
        _ErrorMapping(
            "KRAKEN_POSITION_LIMIT_EXCEEDED",
            "permanent",
            "Position limit exceeded",
        ),
    ),
    (
        "EOrder:Invalid price",
        _ErrorMapping("KRAKEN_INVALID_PRICE", "permanent", "Invalid order price"),
    ),
    (
        "EOrder:Invalid order",
        _ErrorMapping("KRAKEN_INVALID_ORDER", "permanent", "Invalid order"),
    ),
    (
        "EOrder:Unknown order",
        _ErrorMapping("KRAKEN_UNKNOWN_ORDER", "permanent", "Unknown order"),
    ),
    (
        "EOrder:Price too low",
        _ErrorMapping("KRAKEN_PRICE_TOO_LOW", "permanent", "Order price too low"),
    ),
    (
        "EOrder:Price too high",
        _ErrorMapping("KRAKEN_PRICE_TOO_HIGH", "permanent", "Order price too high"),
    ),
    (
        "EOrder:Volume too low",
        _ErrorMapping("KRAKEN_VOLUME_TOO_LOW", "permanent", "Order volume too low"),
    ),
    (
        "EOrder:Volume too high",
        _ErrorMapping("KRAKEN_VOLUME_TOO_HIGH", "permanent", "Order volume too high"),
    ),
    (
        "EOrder:Leverage unavailable",
        _ErrorMapping("KRAKEN_LEVERAGE_UNAVAILABLE", "permanent", "Leverage unavailable"),
    ),
)

_DEFAULT_ERROR_MAPPING = _ErrorMapping(
    "KRAKEN_UNKNOWN_ERROR", "permanent", "Unknown Kraken error"
)

_REST_SERVER_ERROR_MAPPING = _ErrorMapping(
    "KRAKEN_REST_SERVER_ERROR", "transient", "Kraken REST server error"
)

_REST_NETWORK_ERROR_MAPPING = _ErrorMapping(
    "KRAKEN_REST_NETWORK_ERROR", "transient", "Kraken REST network error"
)

_WS_TIMEOUT_MAPPING = _ErrorMapping(
    "KRAKEN_WS_TIMEOUT", "transient", "Kraken websocket timeout"
)

_WS_TRANSPORT_ERROR_MAPPING = _ErrorMapping(
    "KRAKEN_WS_TRANSPORT_ERROR", "transient", "Kraken websocket transport error"
)


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
        self._ws_failures = 0
        self._ws_failure_threshold = 3
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
        if self._ws_failures < self._ws_failure_threshold:
            try:
                ack = await self._execute_transport(
                    transport="websocket",
                    call=websocket_call,
                    correlation_id=corr_id,
                    operation=operation,
                )
                self._ws_failures = 0
                return ack, "websocket"
            except _TransientOrderError as exc:
                self._ws_failures += 1
                self._logger.warning(
                    "Websocket %s exhausted transient retries correlation_id=%s errors=%s",
                    operation,
                    corr_id,
                    exc.errors,
                )
            except (KrakenWSError, KrakenWSTimeout) as exc:
                self._ws_failures += 1
                self._logger.warning(
                    "Websocket %s failed correlation_id=%s error=%s",
                    operation,
                    corr_id,
                    exc,
                )
        else:
            self._logger.warning(
                "Bypassing websocket %s after %s consecutive failures correlation_id=%s",
                operation,
                self._ws_failures,
                corr_id,
            )

        ack = await self._execute_transport(
            transport="rest",
            call=rest_call,
            correlation_id=corr_id,
            operation=operation,
        )
        self._ws_failures = 0
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
                details = self._error_details_from_exception(exc)
                await self._record_oms_errors(
                    errors=details,
                    transport=transport,
                    correlation_id=correlation_id,
                    operation=operation,
                    attempt=attempt,
                )
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

            details = self._extract_error_details(ack.errors)
            classification = self._classify_errors(details)
            raw_errors: List[str] = [detail.raw for detail in details]
            if classification in {"permanent", "transient"}:
                await self._record_oms_errors(
                    errors=details,
                    transport=transport,
                    correlation_id=correlation_id,
                    operation=operation,
                    attempt=attempt,
                )
            if classification == "permanent":
                failed_ack = self._mark_failed(ack, raw_errors)
                self._logger.error(
                    "Permanent Kraken error via %s for %s correlation_id=%s errors=%s",
                    transport,
                    operation,
                    correlation_id,
                    raw_errors,
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
                    raw_errors,
                )
                if attempt >= self._max_retries:
                    raise _TransientOrderError(raw_errors, transport)
                await asyncio.sleep(self._next_delay(backoff))
                backoff = min(backoff * 2, self._max_delay)
                continue

            return ack

        if last_exception is not None:
            raise last_exception

        raise KrakenWSError("Unknown Kraken transport failure")

    def _classify_errors(self, errors: Iterable[_ErrorDetail]) -> str:
        details = list(errors)
        if not details:
            return "none"

        if any(detail.classification == "permanent" for detail in details):
            return "permanent"
        if any(detail.classification == "transient" for detail in details):
            return "transient"
        return "permanent"

    def _extract_error_details(self, errors: Optional[Iterable[str]]) -> List[_ErrorDetail]:
        details: List[_ErrorDetail] = []
        if not errors:
            return details
        for error in errors:
            if error is None:
                continue
            text = str(error).strip()
            if not text:
                continue
            details.append(self._map_error_detail(text))
        return details

    def _mark_failed(self, ack: OrderAck, errors: List[str]) -> OrderAck:
        return replace(ack, status="FAILED", errors=errors or None)

    def _next_delay(self, current: float) -> float:
        jitter = random.uniform(0.8, 1.2)
        return min(current * jitter, self._max_delay)

    def _map_error_detail(self, error: str) -> _ErrorDetail:
        for prefix, mapping in _ERROR_PREFIX_MAPPINGS:
            if error.startswith(prefix):
                return _ErrorDetail(
                    code=mapping.code,
                    raw=error,
                    classification=mapping.classification,
                    message=mapping.message,
                )
        return _ErrorDetail(
            code=_DEFAULT_ERROR_MAPPING.code,
            raw=error,
            classification=_DEFAULT_ERROR_MAPPING.classification,
            message=_DEFAULT_ERROR_MAPPING.message,
        )

    def _error_details_from_exception(self, exc: BaseException) -> List[_ErrorDetail]:
        if isinstance(exc, KrakenWSTimeout):
            text = str(exc) or _WS_TIMEOUT_MAPPING.message
            return [
                _ErrorDetail(
                    code=_WS_TIMEOUT_MAPPING.code,
                    raw=text,
                    classification=_WS_TIMEOUT_MAPPING.classification,
                    message=_WS_TIMEOUT_MAPPING.message,
                )
            ]

        text = str(exc) or exc.__class__.__name__
        parsed = self._parse_error_strings(text)
        if parsed:
            return [self._map_error_detail(value) for value in parsed]

        if isinstance(exc, KrakenRESTError):
            mapping = (
                _REST_SERVER_ERROR_MAPPING
                if "server error" in text.lower()
                else _REST_NETWORK_ERROR_MAPPING
            )
            return [
                _ErrorDetail(
                    code=mapping.code,
                    raw=text,
                    classification=mapping.classification,
                    message=mapping.message,
                )
            ]

        if isinstance(exc, KrakenWSError):
            return [
                _ErrorDetail(
                    code=_WS_TRANSPORT_ERROR_MAPPING.code,
                    raw=text,
                    classification=_WS_TRANSPORT_ERROR_MAPPING.classification,
                    message=_WS_TRANSPORT_ERROR_MAPPING.message,
                )
            ]

        return [
            _ErrorDetail(
                code=_DEFAULT_ERROR_MAPPING.code,
                raw=text,
                classification=_DEFAULT_ERROR_MAPPING.classification,
                message=_DEFAULT_ERROR_MAPPING.message,
            )
        ]

    def _parse_error_strings(self, text: str) -> List[str]:
        candidates: List[str] = []
        list_matches = re.findall(r"\[[^\]]+\]", text)
        for match in list_matches:
            try:
                parsed = ast.literal_eval(match)
            except (ValueError, SyntaxError):
                continue
            if isinstance(parsed, (list, tuple)):
                for item in parsed:
                    item_text = str(item).strip()
                    if item_text:
                        candidates.append(item_text)
        if candidates:
            return candidates

        stripped = text.strip()
        if stripped.startswith("E"):
            return [stripped]
        return []

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
        errors: Sequence[_ErrorDetail],
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
        db_entries: List[dict[str, object]] = []
        recorded_entries: List[dict[str, object]] = []
        for detail in errors:
            entry = {
                "account_id": account_id,
                "correlation_id": correlation_id or "unknown",
                "operation": operation,
                "transport": transport,
                "error_code": detail.code,
                "retry_count": retry_count,
                "last_retry_ts": timestamp,
            }
            db_entries.append(entry)
            recorded_entries.append(
                {
                    **entry,
                    "raw_error": detail.raw,
                    "classification": detail.classification,
                    "message": detail.message,
                }
            )
            error_logger.error(
                "OMS error transport=%s operation=%s correlation_id=%s account_id=%s "
                "error_code=%s raw_error=%s retry_count=%s attempt=%s",
                entry["transport"],
                entry["operation"],
                entry["correlation_id"],
                entry["account_id"],
                entry["error_code"],
                detail.raw,
                entry["retry_count"],
                attempt,
            )

        self._recorded_errors.extend(recorded_entries)

        if not db_entries:
            error_logger.error(
                "Failed to record OMS errors due to empty entry set transport=%s operation=%s",
                transport,
                operation,
            )
            return

        if (
            psycopg is None
            or sql is None
            or self._timescale_session is None
            or not self._timescale_session.dsn
        ):
            return

        await asyncio.to_thread(self._persist_errors_sync, db_entries)

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
