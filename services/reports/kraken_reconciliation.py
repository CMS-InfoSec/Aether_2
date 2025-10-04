"""Kraken account statement reconciliation utilities."""

from __future__ import annotations

import csv
import io
import json
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Protocol, Sequence

try:  # pragma: no cover - optional dependency for tests
    import httpx
except ImportError:  # pragma: no cover - optional dependency for tests
    httpx = None  # type: ignore[assignment]
from prometheus_client import CollectorRegistry, Gauge

try:  # pragma: no cover - optional dependency for unit tests
    from services.alert_manager import AlertManager, RiskEvent
except ModuleNotFoundError:  # pragma: no cover - used when FastAPI is not installed
    AlertManager = Any  # type: ignore

    @dataclass(slots=True)
    class RiskEvent:  # type: ignore[override]
        event_type: str
        severity: str
        description: str
        labels: Dict[str, str] = field(default_factory=dict)

from services.common.config import TimescaleSession, get_timescale_session
from services.secrets.signing import sign_kraken_request

try:  # pragma: no cover - optional dependency for unit tests
    from services.reports.report_service import (
        FILLS_QUERY,
        FILLS_QUERY_FALLBACK,
        NAV_QUERY,
    )
except ModuleNotFoundError:  # pragma: no cover - fallback when FastAPI not installed
    FILLS_QUERY = """
SELECT
    f.order_id::text AS order_id,
    o.market::text AS instrument,
    f.fill_time AS fill_time,
    f.size::numeric AS quantity,
    f.price::numeric AS price,
    COALESCE(f.fee, 0)::numeric AS fee,
    COALESCE(f.slippage_bps, 0)::numeric AS slippage_bps
FROM fills AS f
JOIN orders AS o ON o.order_id = f.order_id
WHERE o.account_id = %(account_id)s
  AND f.fill_time >= %(start)s
  AND f.fill_time < %(end)s
ORDER BY f.fill_time
"""

    FILLS_QUERY_FALLBACK = """
SELECT
    f.order_id::text AS order_id,
    o.market::text AS instrument,
    f.fill_time AS fill_time,
    f.size::numeric AS quantity,
    f.price::numeric AS price,
    COALESCE(f.fee, 0)::numeric AS fee,
    0::numeric AS slippage_bps
FROM fills AS f
JOIN orders AS o ON o.order_id = f.order_id
WHERE o.account_id = %(account_id)s
  AND f.fill_time >= %(start)s
  AND f.fill_time < %(end)s
ORDER BY f.fill_time
"""

    NAV_QUERY = """
SELECT nav
FROM pnl_curves
WHERE account_id = %(account_id)s
  AND as_of >= %(start)s
  AND as_of < %(end)s
ORDER BY as_of DESC
LIMIT 1
"""

LOGGER = logging.getLogger(__name__)


def _parse_datetime(value: Any) -> datetime:
    """Convert ``value`` into an aware ``datetime`` in UTC."""

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if value is None:
        raise ValueError("datetime value is required")

    text = str(value).strip()
    if not text:
        raise ValueError("datetime value is required")

    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _as_decimal(value: Any, default: Decimal | None = None) -> Decimal:
    """Convert numeric and decimal-like values into :class:`Decimal` instances."""

    if default is None:
        default = Decimal("0")
    if value is None or value == "":
        return default
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    try:
        return Decimal(str(value))
    except (TypeError, ValueError):  # pragma: no cover - defensive
        LOGGER.debug("Failed to parse decimal from value: %s", value)
        return default


def _quantize_decimal(value: Decimal, exponent: Decimal = Decimal("0.00000001")) -> Decimal:
    """Quantize ``value`` to a consistent precision for reporting/serialization."""

    return value.quantize(exponent, rounding=ROUND_HALF_EVEN)


@dataclass(slots=True)
class KrakenFill:
    """Fill entry extracted from a Kraken statement."""

    order_id: str
    executed_at: datetime
    quantity: Decimal
    price: Decimal
    fee: Decimal

    def as_dict(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "executed_at": self.executed_at.isoformat(),
            "quantity": float(_quantize_decimal(self.quantity)),
            "price": float(_quantize_decimal(self.price)),
            "fee": float(_quantize_decimal(self.fee)),
        }


@dataclass(slots=True)
class KrakenFeeAdjustment:
    """Fee adjustment entry supplied by Kraken."""

    amount: Decimal
    reason: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"amount": float(_quantize_decimal(self.amount))}
        if self.reason:
            payload["reason"] = self.reason
        return payload


@dataclass(slots=True)
class KrakenNavSnapshot:
    """Net asset value snapshot included in the Kraken statement."""

    as_of: datetime
    nav: Decimal

    def as_dict(self) -> Dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "nav": float(_quantize_decimal(self.nav)),
        }


@dataclass(slots=True)
class KrakenStatement:
    """Structured representation of Kraken statement payloads."""

    account_id: str
    period_start: datetime
    period_end: datetime
    fills: Sequence[KrakenFill] = field(default_factory=list)
    nav_snapshots: Sequence[KrakenNavSnapshot] = field(default_factory=list)
    fee_adjustments: Sequence[KrakenFeeAdjustment] = field(default_factory=list)

    @property
    def total_fees(self) -> Decimal:
        return sum((fill.fee for fill in self.fills), Decimal("0")) + sum(
            (adjustment.amount for adjustment in self.fee_adjustments), Decimal("0")
        )

    @property
    def latest_nav(self) -> Optional[Decimal]:
        if not self.nav_snapshots:
            return None
        latest = max(self.nav_snapshots, key=lambda snapshot: snapshot.as_of)
        return latest.nav

    def as_dict(self) -> Dict[str, Any]:
        return {
            "account_id": self.account_id,
            "period": {
                "start": self.period_start.isoformat(),
                "end": self.period_end.isoformat(),
            },
            "fills": [fill.as_dict() for fill in self.fills],
            "nav_snapshots": [snapshot.as_dict() for snapshot in self.nav_snapshots],
            "fee_adjustments": [adj.as_dict() for adj in self.fee_adjustments],
            "total_fees": float(_quantize_decimal(self.total_fees)),
        }


class KrakenStatementDownloader:
    """Download Kraken CSV and JSON statements for reconciliation."""

    def __init__(
        self,
        *,
        base_url: str,
        csv_path: str = "/statements/trades",
        json_path: str = "/statements/accounting",
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        timeout: float = 30.0,
        session: Optional[httpx.Client] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._csv_path = csv_path
        self._json_path = json_path
        self._api_key = api_key
        self._api_secret = api_secret
        self._timeout = timeout
        if session is not None:
            self._session = session
            self._owns_session = False
        else:
            if httpx is None:  # pragma: no cover - exercised in environments without httpx
                raise RuntimeError(
                    "httpx must be installed or a custom session provided to KrakenStatementDownloader"
                )
            self._session = httpx.Client(timeout=timeout)
            self._owns_session = True

    def close(self) -> None:
        if self._owns_session and hasattr(self._session, "close"):
            self._session.close()

    def _headers(self, path: str, params: Dict[str, Any]) -> Dict[str, str]:
        headers = {"User-Agent": "AetherKrakenReconciler/1.0"}
        if self._api_key and self._api_secret:
            nonce = str(int(time.time() * 1000))
            params["nonce"] = nonce
            _, signature = sign_kraken_request(path, params, self._api_secret)
            headers["API-Key"] = self._api_key
            headers["API-Sign"] = signature
        elif self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    def fetch(self, account_id: str, start: datetime, end: datetime) -> KrakenStatement:
        params = {
            "account_id": account_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }
        csv_url = f"{self._base_url}{self._csv_path}"
        json_url = f"{self._base_url}{self._json_path}"
        csv_params = dict(params)
        csv_params["format"] = "csv"
        csv_headers = self._headers(self._csv_path, csv_params)
        json_params = dict(params)
        json_params["format"] = "json"
        json_headers = self._headers(self._json_path, json_params)

        LOGGER.info(
            "Fetching Kraken statement",
            extra={
                "account_id": account_id,
                "csv_url": csv_url,
                "json_url": json_url,
                "credential_mode": (
                    "hmac"
                    if self._api_key and self._api_secret
                    else "token" if self._api_key else "public"
                ),
            },
        )

        csv_response = self._session.get(csv_url, params=csv_params, headers=csv_headers)
        csv_response.raise_for_status()
        fills = self._parse_csv(csv_response.text)

        json_response = self._session.get(json_url, params=json_params, headers=json_headers)
        json_response.raise_for_status()
        payload = self._parse_json(json_response.text)

        nav_snapshots = self._parse_nav_snapshots(payload)
        fee_adjustments = self._parse_fee_adjustments(payload)

        return KrakenStatement(
            account_id=account_id,
            period_start=start,
            period_end=end,
            fills=fills,
            nav_snapshots=nav_snapshots,
            fee_adjustments=fee_adjustments,
        )

    def _parse_csv(self, payload: str) -> Sequence[KrakenFill]:
        if not payload.strip():
            return []

        buffer = io.StringIO(payload)
        reader = csv.DictReader(buffer)
        fills: list[KrakenFill] = []
        for row in reader:
            order_id = row.get("order_id") or row.get("txid") or row.get("trade_id")
            if not order_id:
                LOGGER.debug("Skipping row without order identifier: %s", row)
                continue
            executed_at = _parse_datetime(
                row.get("executed_at")
                or row.get("fill_time")
                or row.get("time")
                or row.get("timestamp")
            )
            quantity = _as_decimal(row.get("quantity") or row.get("vol") or row.get("size"))
            price = _as_decimal(row.get("price") or row.get("avg_price") or row.get("avg"))
            fee = _as_decimal(row.get("fee") or row.get("fee_paid"))
            fills.append(
                KrakenFill(
                    order_id=order_id,
                    executed_at=executed_at,
                    quantity=quantity,
                    price=price,
                    fee=fee,
                )
            )
        return fills

    def _parse_json(self, payload: str) -> Mapping[str, Any]:
        if not payload:
            return {}
        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:  # pragma: no cover - validated via tests
            raise ValueError("Failed to decode Kraken statement JSON payload") from exc

    def _parse_nav_snapshots(self, payload: Mapping[str, Any]) -> Sequence[KrakenNavSnapshot]:
        raw_nav = payload.get("nav_snapshots") or payload.get("nav") or []
        if isinstance(raw_nav, Mapping):
            raw_entries: Iterable[Mapping[str, Any]] = [raw_nav]
        else:
            raw_entries = raw_nav  # type: ignore[assignment]

        snapshots: list[KrakenNavSnapshot] = []
        for entry in raw_entries:
            if not isinstance(entry, Mapping):
                continue
            as_of = entry.get("as_of") or entry.get("timestamp") or entry.get("time")
            nav_value = entry.get("nav") or entry.get("value")
            if as_of is None or nav_value is None:
                continue
            snapshots.append(
                KrakenNavSnapshot(as_of=_parse_datetime(as_of), nav=_as_decimal(nav_value))
            )
        return snapshots

    def _parse_fee_adjustments(self, payload: Mapping[str, Any]) -> Sequence[KrakenFeeAdjustment]:
        raw_adjustments = payload.get("fee_adjustments") or []
        adjustments: list[KrakenFeeAdjustment] = []
        for entry in raw_adjustments:
            if isinstance(entry, Mapping):
                amount = _as_decimal(entry.get("amount") or entry.get("value"))
                reason = entry.get("reason") or entry.get("description")
            else:
                amount = _as_decimal(entry)
                reason = None
            if amount == 0:
                continue
            adjustments.append(KrakenFeeAdjustment(amount=amount, reason=reason))
        return adjustments


@dataclass(slots=True)
class InternalFill:
    """Internal trade fill stored in the reporting database."""

    order_id: str
    executed_at: datetime
    quantity: Decimal
    price: Decimal
    fee: Decimal

    def as_dict(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "executed_at": self.executed_at.isoformat(),
            "quantity": float(_quantize_decimal(self.quantity)),
            "price": float(_quantize_decimal(self.price)),
            "fee": float(_quantize_decimal(self.fee)),
        }


class InternalLedger(Protocol):
    """Data access layer for internal trade and accounting records."""

    def fetch_fills(self, account_id: str, start: datetime, end: datetime) -> Sequence[InternalFill]:
        ...

    def fetch_nav(self, account_id: str, start: datetime, end: datetime) -> Optional[Decimal]:
        ...


class TimescaleInternalLedger:
    """Implementation of :class:`InternalLedger` backed by TimescaleDB."""

    def __init__(
        self,
        *,
        session_factory: Callable[[str], TimescaleSession] = get_timescale_session,
    ) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _cursor(self, account_id: str) -> Iterator[Any]:
        import psycopg2
        from psycopg2 import sql
        from psycopg2.extras import RealDictCursor

        config = self._session_factory(account_id)
        conn = psycopg2.connect(config.dsn)
        try:
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(config.account_schema)
                    )
                )
                yield cursor
        finally:
            conn.close()

    def fetch_fills(self, account_id: str, start: datetime, end: datetime) -> Sequence[InternalFill]:
        from psycopg2 import errors

        params = {"account_id": account_id, "start": start, "end": end}
        with self._cursor(account_id) as cursor:
            try:
                cursor.execute(FILLS_QUERY, params)
            except errors.UndefinedColumn:
                LOGGER.debug(
                    "slippage_bps column missing on fills table for account %s, using fallback",
                    account_id,
                )
                cursor.execute(FILLS_QUERY_FALLBACK, params)
            rows = cursor.fetchall() or []

        fills: list[InternalFill] = []
        for row in rows:
            executed_at = _parse_datetime(row.get("fill_time"))
            fills.append(
                InternalFill(
                    order_id=str(row.get("order_id")),
                    executed_at=executed_at,
                    quantity=_as_decimal(row.get("quantity")),
                    price=_as_decimal(row.get("price")),
                    fee=_as_decimal(row.get("fee")),
                )
            )
        return fills

    def fetch_nav(self, account_id: str, start: datetime, end: datetime) -> Optional[Decimal]:
        params = {"account_id": account_id, "start": start, "end": end}
        with self._cursor(account_id) as cursor:
            cursor.execute(NAV_QUERY, params)
            row = cursor.fetchone()
        if not row:
            return None
        return _as_decimal(row.get("nav"))


class KrakenReconciliationMetrics:
    """Prometheus metrics exported by the reconciliation job."""

    def __init__(self, registry: Optional[CollectorRegistry] = None) -> None:
        metric_kwargs = {"registry": registry} if registry is not None else {}
        self.registry = registry
        self.fee_difference = Gauge(
            "aether_kraken_fee_difference",
            "Absolute difference between Kraken and internal fee totals.",
            ("account_id",),
            **metric_kwargs,
        )
        self.nav_difference = Gauge(
            "aether_kraken_nav_difference",
            "Absolute difference between Kraken and internal NAV snapshots.",
            ("account_id",),
            **metric_kwargs,
        )
        self.missing_fills = Gauge(
            "aether_kraken_missing_fills",
            "Number of unmatched fills detected during Kraken reconciliation.",
            ("account_id",),
            **metric_kwargs,
        )
        self.reconciliation_success = Gauge(
            "aether_kraken_reconciliation_success",
            "Indicator metric set to 1 when reconciliation succeeds without mismatches.",
            ("account_id",),
            **metric_kwargs,
        )


_metrics_lock = threading.Lock()
_metrics: Optional[KrakenReconciliationMetrics] = None


def get_reconciliation_metrics(
    registry: Optional[CollectorRegistry] = None,
) -> KrakenReconciliationMetrics:
    global _metrics
    with _metrics_lock:
        if registry is not None or _metrics is None:
            _metrics = KrakenReconciliationMetrics(registry=registry)
        return _metrics


def configure_reconciliation_metrics(
    registry: Optional[CollectorRegistry] = None,
) -> KrakenReconciliationMetrics:
    global _metrics
    with _metrics_lock:
        _metrics = KrakenReconciliationMetrics(registry=registry)
        return _metrics


@dataclass(slots=True)
class KrakenReconciliationResult:
    """Summary of a reconciliation run for a single account."""

    account_id: str
    period_start: datetime
    period_end: datetime
    fee_difference: Decimal
    nav_difference: Decimal
    external_total_fees: Decimal
    internal_total_fees: Decimal
    external_nav: Optional[Decimal]
    internal_nav: Optional[Decimal]
    missing_external_fills: Sequence[KrakenFill]
    unexpected_internal_fills: Sequence[InternalFill]
    fee_adjustments: Sequence[KrakenFeeAdjustment]

    @property
    def missing_fill_count(self) -> int:
        return len(self.missing_external_fills) + len(self.unexpected_internal_fills)

    @property
    def ok(self) -> bool:
        return (
            self.missing_fill_count == 0
            and abs(self.fee_difference) <= Decimal("1e-9")
            and abs(self.nav_difference) <= Decimal("1e-9")
        )

    def exceeds_tolerance(self, tolerance: float) -> bool:
        if self.missing_fill_count:
            return True
        tol = Decimal(str(tolerance))
        return abs(self.fee_difference) > tol or abs(self.nav_difference) > tol

    def as_dict(self) -> Dict[str, Any]:
        status = "matched" if self.ok else "mismatch"
        return {
            "account_id": self.account_id,
            "period": {
                "start": self.period_start.isoformat(),
                "end": self.period_end.isoformat(),
            },
            "status": status,
            "fee_difference": float(_quantize_decimal(self.fee_difference)),
            "nav_difference": float(_quantize_decimal(self.nav_difference)),
            "external_total_fees": float(_quantize_decimal(self.external_total_fees)),
            "internal_total_fees": float(_quantize_decimal(self.internal_total_fees)),
            "external_nav": float(_quantize_decimal(self.external_nav)) if self.external_nav is not None else None,
            "internal_nav": float(_quantize_decimal(self.internal_nav)) if self.internal_nav is not None else None,
            "missing_external_fills": [fill.as_dict() for fill in self.missing_external_fills],
            "unexpected_internal_fills": [fill.as_dict() for fill in self.unexpected_internal_fills],
            "fee_adjustments": [adj.as_dict() for adj in self.fee_adjustments],
        }


class KrakenReconciliationService:
    """Coordinate Kraken statement downloads and reconciliation logic."""

    def __init__(
        self,
        *,
        downloader: KrakenStatementDownloader,
        ledger: InternalLedger,
        metrics: Optional[KrakenReconciliationMetrics] = None,
        alert_manager: Optional[AlertManager] = None,
        tolerance: float = 1.0,
    ) -> None:
        self._downloader = downloader
        self._ledger = ledger
        self._metrics = metrics or get_reconciliation_metrics()
        self._alert_manager = alert_manager
        self._tolerance = Decimal(str(tolerance))

    def reconcile_account(
        self, account_id: str, start: datetime, end: datetime
    ) -> KrakenReconciliationResult:
        statement = self._downloader.fetch(account_id, start, end)
        internal_fills = list(self._ledger.fetch_fills(account_id, start, end))
        internal_total_fees = sum((fill.fee for fill in internal_fills), Decimal("0"))
        external_total_fees = statement.total_fees
        fee_difference = external_total_fees - internal_total_fees

        external_nav = statement.latest_nav
        internal_nav = self._ledger.fetch_nav(account_id, start, end)
        nav_difference = (external_nav or Decimal("0")) - (internal_nav or Decimal("0"))

        internal_index = {fill.order_id: fill for fill in internal_fills}
        external_index = {fill.order_id: fill for fill in statement.fills}

        missing_external = [fill for key, fill in external_index.items() if key not in internal_index]
        unexpected_internal = [fill for key, fill in internal_index.items() if key not in external_index]

        result = KrakenReconciliationResult(
            account_id=account_id,
            period_start=start,
            period_end=end,
            fee_difference=fee_difference,
            nav_difference=nav_difference,
            external_total_fees=external_total_fees,
            internal_total_fees=internal_total_fees,
            external_nav=external_nav,
            internal_nav=internal_nav,
            missing_external_fills=missing_external,
            unexpected_internal_fills=unexpected_internal,
            fee_adjustments=statement.fee_adjustments,
        )

        self._emit_metrics(account_id, result)
        self._maybe_alert(result)
        return result

    def reconcile_accounts(
        self, accounts: Iterable[str], start: datetime, end: datetime
    ) -> Sequence[KrakenReconciliationResult]:
        return [self.reconcile_account(account_id, start, end) for account_id in accounts]

    def _emit_metrics(self, account_id: str, result: KrakenReconciliationResult) -> None:
        self._metrics.fee_difference.labels(account_id).set(
            float(_quantize_decimal(abs(result.fee_difference)))
        )
        self._metrics.nav_difference.labels(account_id).set(
            float(_quantize_decimal(abs(result.nav_difference)))
        )
        self._metrics.missing_fills.labels(account_id).set(result.missing_fill_count)
        self._metrics.reconciliation_success.labels(account_id).set(1 if result.ok else 0)

    def _maybe_alert(self, result: KrakenReconciliationResult) -> None:
        if not self._alert_manager:
            return
        if not result.exceeds_tolerance(float(self._tolerance)):
            return

        description_parts = [
            f"Fee diff={_quantize_decimal(result.fee_difference)}",
            f"NAV diff={_quantize_decimal(result.nav_difference)}",
            f"missing_fills={result.missing_fill_count}",
        ]
        description = "Kraken reconciliation mismatch: " + ", ".join(description_parts)
        labels = {
            "account_id": result.account_id,
            "missing_fills": str(result.missing_fill_count),
        }
        event = RiskEvent(
            event_type="KrakenReconciliationMismatch",
            severity="warning",
            description=description,
            labels=labels,
        )
        LOGGER.warning(description, extra={"account_id": result.account_id})
        self._alert_manager.handle_risk_event(event)


__all__ = [
    "InternalFill",
    "InternalLedger",
    "KrakenFeeAdjustment",
    "KrakenFill",
    "KrakenNavSnapshot",
    "KrakenReconciliationMetrics",
    "KrakenReconciliationResult",
    "KrakenReconciliationService",
    "KrakenStatement",
    "KrakenStatementDownloader",
    "TimescaleInternalLedger",
    "configure_reconciliation_metrics",
    "get_reconciliation_metrics",
]
