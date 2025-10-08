"""Daily trade and PnL report generation endpoints."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    TypeVar,
    cast,
)

try:  # pragma: no cover - FastAPI is optional in some unit tests
    from fastapi import APIRouter, Depends, HTTPException, Query
    from fastapi.responses import StreamingResponse
except ImportError:  # pragma: no cover - fallback when FastAPI is stubbed out
    from services.common.fastapi_stub import (  # type: ignore[misc]
        APIRouter,
        Depends,
        HTTPException,
        Query,
        StreamingResponse,
    )
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from services.common.config import TimescaleSession, get_timescale_session
from services.common.security import require_admin_account
from services.models.model_server import get_active_model

LOGGER = logging.getLogger(__name__)


TRADES_QUERY = """
SELECT
    o.order_id::text AS trade_id,
    o.market::text AS instrument,
    o.side::text AS side,
    o.size::numeric AS quantity,
    o.submitted_at AS submitted_at
FROM orders AS o
WHERE o.account_id = %(account_id)s
  AND o.submitted_at >= %(start)s
  AND o.submitted_at < %(end)s
ORDER BY o.submitted_at
"""


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


PNL_SUMMARY_QUERY = """
SELECT
    COALESCE(SUM(realized), 0) AS realized_pnl,
    COALESCE(SUM(unrealized), 0) AS unrealized_pnl
FROM pnl
WHERE account_id = %(account_id)s
  AND as_of >= %(start)s
  AND as_of < %(end)s
"""


NAV_OPEN_QUERY = """
SELECT nav
FROM pnl_curves
WHERE account_id = %(account_id)s
  AND as_of >= %(start)s
  AND as_of < %(end)s
ORDER BY as_of ASC
LIMIT 1
"""


NAV_CLOSE_QUERY = """
SELECT nav
FROM pnl_curves
WHERE account_id = %(account_id)s
  AND as_of >= %(start)s
  AND as_of < %(end)s
ORDER BY as_of DESC
LIMIT 1
"""

# Backwards compatibility for modules importing the previous constant name
NAV_QUERY = NAV_CLOSE_QUERY


FEES_SUMMARY_QUERY = """
SELECT
    COALESCE(SUM(f.fee), 0) AS fees
FROM fills AS f
JOIN orders AS o ON o.order_id = f.order_id
WHERE o.account_id = %(account_id)s
  AND f.fill_time >= %(start)s
  AND f.fill_time < %(end)s
"""


REPORTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS reports (
    account_id TEXT NOT NULL,
    date DATE NOT NULL,
    data_json JSONB NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, date)
);
"""


UPSERT_REPORT_SQL = """
INSERT INTO reports (
    account_id,
    date,
    data_json,
    ts
) VALUES (
    %(account_id)s,
    %(date)s,
    %(data_json)s::jsonb,
    %(ts)s
)
ON CONFLICT (account_id, date) DO UPDATE
SET
    data_json = EXCLUDED.data_json,
    ts = EXCLUDED.ts
"""


DELETE_EXPIRED_REPORTS_SQL = """
DELETE FROM reports
WHERE ts < %(cutoff)s
"""


DAILY_NAV_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS daily_nav (
    date DATE NOT NULL,
    account_id TEXT NOT NULL,
    open_nav NUMERIC NOT NULL,
    close_nav NUMERIC NOT NULL,
    daily_return_pct NUMERIC NOT NULL,
    PRIMARY KEY (date, account_id)
);
"""


UPSERT_DAILY_NAV_SQL = """
INSERT INTO daily_nav (
    date,
    account_id,
    open_nav,
    close_nav,
    daily_return_pct
)
VALUES (
    %(date)s,
    %(account_id)s,
    %(open_nav)s,
    %(close_nav)s,
    %(daily_return_pct)s
)
ON CONFLICT (date, account_id) DO UPDATE
SET
    open_nav = EXCLUDED.open_nav,
    close_nav = EXCLUDED.close_nav,
    daily_return_pct = EXCLUDED.daily_return_pct
"""


TRADE_CONTEXT_QUERY = """
SELECT
    COALESCE(f.fill_id::text, o.order_id::text) AS trade_id,
    COALESCE(f.order_id::text, o.order_id::text) AS order_id,
    COALESCE(f.account_id::text, o.account_id::text) AS account_id,
    COALESCE(f.market::text, f.symbol::text, o.market::text, o.symbol::text) AS instrument,
    COALESCE(f.fill_time, f.fill_ts, o.submitted_at) AS executed_at,
    f.metadata AS fill_metadata,
    o.metadata AS order_metadata
FROM fills AS f
LEFT JOIN orders AS o ON o.order_id = f.order_id
WHERE
    f.fill_id::text = %(trade_id)s
    OR f.order_id::text = %(trade_id)s
    OR o.order_id::text = %(trade_id)s
ORDER BY COALESCE(f.fill_time, f.fill_ts, o.submitted_at) DESC
LIMIT 1
"""


@dataclass(frozen=True)
class TradeRecord:
    """Normalized trade lifecycle information."""

    trade_id: str
    instrument: str
    side: str
    quantity: float
    submitted_at: datetime

    def as_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "instrument": self.instrument,
            "side": self.side,
            "quantity": self.quantity,
            "submitted_at": _isoformat(self.submitted_at),
        }


@dataclass(frozen=True)
class FillRecord:
    """Captured execution details for an order fill."""

    order_id: str
    instrument: str
    quantity: float
    price: float
    fee: float
    slippage_bps: float
    fill_time: datetime

    def as_dict(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "instrument": self.instrument,
            "quantity": self.quantity,
            "price": self.price,
            "fee": self.fee,
            "slippage_bps": self.slippage_bps,
            "fill_time": _isoformat(self.fill_time),
        }


@dataclass(frozen=True)
class DailyReport:
    """Aggregated representation of the daily activity for an account."""

    account_id: str
    report_date: date
    generated_at: datetime
    nav: float
    realized_pnl: float
    unrealized_pnl: float
    fees: float
    slippage_bps: float
    trades: Sequence[TradeRecord]
    fills: Sequence[FillRecord]

    def to_dict(self) -> Dict[str, Any]:
        net_pnl = self.realized_pnl + self.unrealized_pnl - self.fees
        return {
            "account_id": self.account_id,
            "report_date": self.report_date.isoformat(),
            "generated_at": _isoformat(self.generated_at),
            "nav": self.nav,
            "pnl": {
                "realized": self.realized_pnl,
                "unrealized": self.unrealized_pnl,
                "net": net_pnl,
            },
            "fees": self.fees,
            "slippage_bps": self.slippage_bps,
            "trades": [trade.as_dict() for trade in self.trades],
            "fills": [fill.as_dict() for fill in self.fills],
        }

    def summary_metadata(self) -> Dict[str, Any]:
        return {
            "nav": self.nav,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "fees": self.fees,
            "slippage_bps": self.slippage_bps,
            "trade_count": len(self.trades),
            "fill_count": len(self.fills),
        }


def _isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat()


class TradeContextError(RuntimeError):
    """Raised when a trade cannot be explained due to missing context."""


class DailyReportService:
    """Service responsible for building and persisting daily reports."""

    def __init__(self, *, default_account_id: str, retention_days: int = 30) -> None:
        self._default_account_id = default_account_id
        self._retention_days = retention_days

    @property
    def default_account_id(self) -> str:
        return self._default_account_id

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    @contextmanager
    def _session(self, config: TimescaleSession) -> Iterator[RealDictCursor]:
        import psycopg2

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

    def _timescale(self, account_id: str) -> TimescaleSession:
        return get_timescale_session(account_id)

    @staticmethod
    def _fetch(cursor: RealDictCursor, query: str, params: Mapping[str, Any]) -> List[Dict[str, Any]]:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _fetch_one(cursor: RealDictCursor, query: str, params: Mapping[str, Any]) -> Dict[str, Any] | None:
        cursor.execute(query, params)
        row = cursor.fetchone()
        return dict(row) if row else None

    def _fetch_fills(self, cursor: RealDictCursor, params: Mapping[str, Any]) -> List[Dict[str, Any]]:
        from psycopg2 import errors

        try:
            return self._fetch(cursor, FILLS_QUERY, params)
        except errors.UndefinedColumn:
            LOGGER.debug("slippage_bps column missing on fills table, falling back to zero values")
            return self._fetch(cursor, FILLS_QUERY_FALLBACK, params)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_daily_report(
        self,
        *,
        account_id: str | None = None,
        report_date: date | None = None,
    ) -> DailyReport:
        target_account = (account_id or self._default_account_id).strip()
        if not target_account:
            raise ValueError("account_id must be provided")
        report_day = report_date or date.today()
        start = datetime.combine(report_day, datetime.min.time(), tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        config = self._timescale(target_account)

        with self._session(config) as cursor:
            params = {"account_id": target_account, "start": start, "end": end}
            trade_rows = self._fetch(cursor, TRADES_QUERY, params)
            fill_rows = self._fetch_fills(cursor, params)
            pnl_row = self._fetch_one(cursor, PNL_SUMMARY_QUERY, params) or {}
            nav_row = self._fetch_one(cursor, NAV_CLOSE_QUERY, params) or {}
            fees_row = self._fetch_one(cursor, FEES_SUMMARY_QUERY, params) or {}

        trades = [self._build_trade(row) for row in trade_rows]
        fills = [self._build_fill(row) for row in fill_rows]
        realized = _as_float(pnl_row.get("realized_pnl", 0.0))
        unrealized = _as_float(pnl_row.get("unrealized_pnl", 0.0))
        nav = _as_float(nav_row.get("nav", 0.0))
        fees = _as_float(fees_row.get("fees", 0.0))
        slippage = self._compute_slippage(fills)
        generated_at = datetime.now(timezone.utc)

        return DailyReport(
            account_id=target_account,
            report_date=report_day,
            generated_at=generated_at,
            nav=nav,
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            fees=fees,
            slippage_bps=slippage,
            trades=trades,
            fills=fills,
        )

    def get_daily_return_summary(
        self,
        *,
        account_id: str | None = None,
        nav_date: date | None = None,
    ) -> Dict[str, Any]:
        target_account = (account_id or self._default_account_id).strip()
        if not target_account:
            raise ValueError("account_id must be provided")
        summary_date = nav_date or date.today()
        start = datetime.combine(summary_date, datetime.min.time(), tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        config = self._timescale(target_account)

        with self._session(config) as cursor:
            params = {"account_id": target_account, "start": start, "end": end}
            open_row = self._fetch_one(cursor, NAV_OPEN_QUERY, params) or {}
            close_row = self._fetch_one(cursor, NAV_CLOSE_QUERY, params) or {}
            pnl_row = self._fetch_one(cursor, PNL_SUMMARY_QUERY, params) or {}
            fees_row = self._fetch_one(cursor, FEES_SUMMARY_QUERY, params) or {}

        open_nav = _as_float(open_row.get("nav", 0.0))
        nav_now = _as_float(close_row.get("nav", 0.0))
        realized = _as_float(pnl_row.get("realized_pnl", 0.0))
        unrealized = _as_float(pnl_row.get("unrealized_pnl", 0.0))
        fees = _as_float(fees_row.get("fees", 0.0))

        daily_return_pct = 0.0
        if open_nav:
            daily_return_pct = ((nav_now - open_nav) / open_nav) * 100.0

        summary = {
            "account_id": target_account,
            "date": summary_date.isoformat(),
            "daily_return_pct": daily_return_pct,
            "nav_open": open_nav,
            "nav_now": nav_now,
            "components": {
                "realized_pnl_usd": realized,
                "unrealized_pnl_usd": unrealized,
                "fees_usd": fees,
            },
        }

        try:
            self._persist_daily_nav(
                account_id=target_account,
                nav_date=summary_date,
                open_nav=open_nav,
                close_nav=nav_now,
                daily_return_pct=daily_return_pct,
            )
        except Exception:  # pragma: no cover - best effort persistence
            LOGGER.exception("Failed to persist daily NAV for account %s", target_account)

        return summary

    def export_daily_report(self, report: DailyReport, *, fmt: str) -> tuple[bytes, str, str]:
        fmt_lower = fmt.lower()
        if fmt_lower == "json":
            data = self._serialize_json(report)
            filename = f"daily_report_{report.account_id}_{report.report_date.isoformat()}.json"
            content_type = "application/json"
        elif fmt_lower == "csv":
            data = self._serialize_csv(report)
            filename = f"daily_report_{report.account_id}_{report.report_date.isoformat()}.csv"
            content_type = "text/csv"
        elif fmt_lower == "pdf":
            data = self._serialize_pdf(report)
            filename = f"daily_report_{report.account_id}_{report.report_date.isoformat()}.pdf"
            content_type = "application/pdf"
        else:
            raise ValueError("Unsupported export format; choose 'json', 'csv', or 'pdf'")
        return data, filename, content_type

    def persist_report(self, report: DailyReport, payload: Mapping[str, Any]) -> None:
        account_id = report.account_id
        config = self._timescale(account_id)
        now = datetime.now(timezone.utc)
        serialized_payload = json.dumps(payload, separators=(",", ":"))
        params = {
            "account_id": account_id,
            "date": report.report_date,
            "data_json": serialized_payload,
            "ts": now,
        }
        cutoff = now - timedelta(days=self._retention_days)
        with self._session(config) as cursor:
            cursor.execute(REPORTS_TABLE_DDL)
            cursor.execute(UPSERT_REPORT_SQL, params)
            cursor.execute(DELETE_EXPIRED_REPORTS_SQL, {"cutoff": cutoff})

    def _persist_daily_nav(
        self,
        *,
        account_id: str,
        nav_date: date,
        open_nav: float,
        close_nav: float,
        daily_return_pct: float,
    ) -> None:
        config = self._timescale(account_id)
        params = {
            "date": nav_date,
            "account_id": account_id,
            "open_nav": open_nav,
            "close_nav": close_nav,
            "daily_return_pct": daily_return_pct,
        }
        with self._session(config) as cursor:
            cursor.execute(DAILY_NAV_TABLE_DDL)
            cursor.execute(UPSERT_DAILY_NAV_SQL, params)

    def explain_trade(self, trade_id: str) -> Dict[str, Any]:
        identifier = (trade_id or "").strip()
        if not identifier:
            raise ValueError("trade_id must be provided")

        config = self._timescale(self._default_account_id)
        with self._session(config) as cursor:
            cursor.execute(TRADE_CONTEXT_QUERY, {"trade_id": identifier})
            row = cursor.fetchone()

        if row is None:
            raise LookupError("Trade not found")

        trade_row = dict(row)
        account_id = str(trade_row.get("account_id") or "").strip()
        if not account_id:
            raise TradeContextError("Trade is missing account context")

        instrument = str(trade_row.get("instrument") or "").strip()
        if not instrument:
            raise TradeContextError("Trade is missing instrument context")

        features = _extract_feature_mapping(trade_row)

        model = get_active_model(account_id, instrument)
        try:
            raw_importance = model.explain(features)
        except Exception as exc:  # pragma: no cover - defensive safety net
            LOGGER.exception(
                "Model explanation failed",
                extra={"trade_id": identifier, "account_id": account_id},
            )
            raise TradeContextError("Unable to generate feature importances") from exc

        contributions = self._normalise_contributions(raw_importance)
        ordered = sorted(contributions, key=lambda entry: abs(entry[1]), reverse=True)

        executed_at: Optional[datetime] = None
        executed_raw = trade_row.get("executed_at")
        if executed_raw is not None:
            executed_at = _ensure_datetime(executed_raw)

        reason = _extract_reason(trade_row)
        if not reason and ordered:
            top_feature, top_value = ordered[0]
            direction = "positive" if top_value >= 0 else "negative"
            reason = (
                f"Feature '{top_feature}' had a {direction} contribution of {top_value:.4f} "
                "to the decision."
            )

        explanation = {
            "trade_id": trade_row.get("trade_id") or identifier,
            "account_id": account_id,
            "instrument": instrument,
            "executed_at": _isoformat(executed_at) if executed_at else None,
            "model_used": _resolve_model_identifier(trade_row, model),
            "regime": _extract_regime_label(trade_row),
            "reason": reason or "No explicit model reasoning recorded",
            "feature_importances": [
                {"feature": name, "importance": value} for name, value in ordered[:10]
            ],
            "feature_count": len(ordered),
        }
        return explanation

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_trade(row: Mapping[str, Any]) -> TradeRecord:
        submitted_at = _ensure_datetime(row.get("submitted_at"))
        return TradeRecord(
            trade_id=str(row.get("trade_id")),
            instrument=str(row.get("instrument", "")),
            side=str(row.get("side", "")).upper(),
            quantity=_as_float(row.get("quantity", 0.0)),
            submitted_at=submitted_at,
        )

    @staticmethod
    def _build_fill(row: Mapping[str, Any]) -> FillRecord:
        fill_time = _ensure_datetime(row.get("fill_time"))
        return FillRecord(
            order_id=str(row.get("order_id")),
            instrument=str(row.get("instrument", "")),
            quantity=_as_float(row.get("quantity", 0.0)),
            price=_as_float(row.get("price", 0.0)),
            fee=_as_float(row.get("fee", 0.0)),
            slippage_bps=_as_float(row.get("slippage_bps", 0.0)),
            fill_time=fill_time,
        )

    @staticmethod
    def _compute_slippage(fills: Sequence[FillRecord]) -> float:
        total_size = sum(abs(fill.quantity) for fill in fills)
        if total_size == 0:
            return 0.0
        weighted = sum(abs(fill.quantity) * fill.slippage_bps for fill in fills)
        return weighted / total_size

    @staticmethod
    def _serialize_json(report: DailyReport) -> bytes:
        payload = report.to_dict()
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @staticmethod
    def _serialize_csv(report: DailyReport) -> bytes:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["section", "field", "value"])
        writer.writerow(["summary", "account_id", report.account_id])
        writer.writerow(["summary", "report_date", report.report_date.isoformat()])
        writer.writerow(["summary", "generated_at", _isoformat(report.generated_at)])
        writer.writerow(["summary", "nav", f"{report.nav:.10f}"])
        writer.writerow(["summary", "realized_pnl", f"{report.realized_pnl:.10f}"])
        writer.writerow(["summary", "unrealized_pnl", f"{report.unrealized_pnl:.10f}"])
        writer.writerow(["summary", "fees", f"{report.fees:.10f}"])
        writer.writerow(["summary", "slippage_bps", f"{report.slippage_bps:.10f}"])
        for trade in report.trades:
            writer.writerow([
                "trade",
                trade.trade_id,
                json.dumps(
                    {
                        "instrument": trade.instrument,
                        "side": trade.side,
                        "quantity": trade.quantity,
                        "submitted_at": _isoformat(trade.submitted_at),
                    }
                ),
                "",
            ])
        for fill in report.fills:
            writer.writerow([
                "fill",
                fill.order_id,
                json.dumps(
                    {
                        "instrument": fill.instrument,
                        "quantity": fill.quantity,
                        "price": fill.price,
                        "fee": fill.fee,
                        "slippage_bps": fill.slippage_bps,
                        "fill_time": _isoformat(fill.fill_time),
                    }
                ),
                "",
            ])
        return buffer.getvalue().encode("utf-8")

    @staticmethod
    def _serialize_pdf(report: DailyReport) -> bytes:
        lines = [
            f"Daily report for {report.account_id}",
            f"Date: {report.report_date.isoformat()}",
            f"Generated: {_isoformat(report.generated_at)}",
            "",
            f"NAV: {report.nav:,.2f}",
            f"Realized PnL: {report.realized_pnl:,.2f}",
            f"Unrealized PnL: {report.unrealized_pnl:,.2f}",
            f"Fees: {report.fees:,.2f}",
            f"Average slippage (bps): {report.slippage_bps:,.4f}",
            "",
            f"Trades: {len(report.trades)}",
            f"Fills: {len(report.fills)}",
        ]
        preview_trades = list(report.trades)[:10]
        for trade in preview_trades:
            lines.append(
                f"Trade {trade.trade_id} {trade.side} {trade.quantity} {trade.instrument} at {trade.submitted_at.isoformat()}"
            )
        preview_fills = list(report.fills)[:10]
        for fill in preview_fills:
            lines.append(
                f"Fill {fill.order_id} {fill.quantity} @ {fill.price} fee {fill.fee}bps {fill.slippage_bps:.4f}"
            )
        return _pdf_from_lines(lines)

    @staticmethod
    def _normalise_contributions(raw: Any) -> List[tuple[str, float]]:
        if isinstance(raw, Mapping):
            pairs = []
            for name, value in raw.items():
                try:
                    pairs.append((str(name), float(value)))
                except (TypeError, ValueError) as exc:
                    raise TradeContextError(
                        f"Feature importance for '{name}' is not numeric"
                    ) from exc
            return pairs

        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            pairs = []
            for idx, value in enumerate(raw):
                try:
                    pairs.append((f"feature_{idx}", float(value)))
                except (TypeError, ValueError) as exc:
                    raise TradeContextError(
                        f"Feature importance at position {idx} is not numeric"
                    ) from exc
            return pairs

        raise TradeContextError("Model explanation returned unsupported format")


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _ensure_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return datetime.now(timezone.utc)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def _pdf_escape(text: str) -> str:
    return text.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")


def _pdf_from_lines(lines: Iterable[str]) -> bytes:
    buffer = io.BytesIO()
    buffer.write(b"%PDF-1.4\n")
    objects: List[int] = []

    def add_object(obj_id: int, body: bytes, stream: bytes | None = None) -> None:
        objects.append(buffer.tell())
        buffer.write(f"{obj_id} 0 obj\n".encode("latin-1"))
        buffer.write(body)
        if stream is not None:
            buffer.write(b"\nstream\n")
            buffer.write(stream)
            buffer.write(b"\nendstream")
        buffer.write(b"\nendobj\n")

    add_object(1, b"<< /Type /Catalog /Pages 2 0 R >>")
    add_object(2, b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    add_object(
        3,
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
    )

    content_lines = ["BT", "/F1 12 Tf"]
    y = 760
    for line in lines:
        content_lines.append(f"1 0 0 1 72 {y} Tm ({_pdf_escape(line)}) Tj")
        y -= 14
        if y < 72:
            break
    content_lines.append("ET")
    content_stream = "\n".join(content_lines).encode("latin-1", "replace")
    add_object(4, f"<< /Length {len(content_stream)} >>".encode("latin-1"), content_stream)
    add_object(5, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    xref_offset = buffer.tell()
    buffer.write(f"xref\n0 {len(objects) + 1}\n".encode("latin-1"))
    buffer.write(b"0000000000 65535 f \n")
    for offset in objects:
        buffer.write(f"{offset:010} 00000 n \n".encode("latin-1"))
    buffer.write(b"trailer\n")
    buffer.write(f"<< /Size {len(objects) + 1} /Root 1 0 R >>\n".encode("latin-1"))
    buffer.write(b"startxref\n")
    buffer.write(f"{xref_offset}\n".encode("latin-1"))
    buffer.write(b"%%EOF")
    return buffer.getvalue()


def _normalise_feature_payload(raw: Any) -> MutableMapping[str, float]:
    if isinstance(raw, Mapping):
        normalised: MutableMapping[str, float] = {}
        for key, value in raw.items():
            try:
                normalised[str(key)] = float(value)
            except (TypeError, ValueError) as exc:
                raise TradeContextError(f"Feature '{key}' is not numeric") from exc
        if normalised:
            return normalised

    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        normalised = {}
        for idx, value in enumerate(raw):
            try:
                normalised[f"feature_{idx}"] = float(value)
            except (TypeError, ValueError) as exc:
                raise TradeContextError(
                    f"Feature at position {idx} is not numeric"
                ) from exc
        if normalised:
            return normalised

    return {}


def _extract_feature_mapping(trade_row: Mapping[str, Any]) -> MutableMapping[str, float]:
    candidates = ("features", "feature_vector", "feature_values")
    for key in candidates:
        if key in trade_row:
            mapping = _normalise_feature_payload(trade_row[key])
            if mapping:
                return mapping

    metadata_candidates = (
        trade_row.get("order_metadata"),
        trade_row.get("fill_metadata"),
        trade_row.get("metadata"),
    )
    for container in metadata_candidates:
        if isinstance(container, Mapping):
            for key in candidates:
                if key in container:
                    mapping = _normalise_feature_payload(container[key])
                    if mapping:
                        return mapping

    raise TradeContextError("Trade is missing feature metadata")


def _extract_model_version(trade_row: Mapping[str, Any]) -> Optional[str]:
    model_version = trade_row.get("model_version")
    if isinstance(model_version, str) and model_version:
        return model_version

    metadata_candidates = (
        trade_row.get("order_metadata"),
        trade_row.get("fill_metadata"),
        trade_row.get("metadata"),
    )
    for container in metadata_candidates:
        if isinstance(container, Mapping):
            candidate = container.get("model_version")
            if isinstance(candidate, str) and candidate:
                return candidate
    return None


def _extract_regime_label(trade_row: Mapping[str, Any]) -> str:
    search_space: Iterable[Any] = (
        trade_row.get("regime"),
        trade_row.get("state"),
        trade_row.get("order_metadata"),
        trade_row.get("fill_metadata"),
        trade_row.get("metadata"),
    )

    for candidate in search_space:
        if isinstance(candidate, Mapping):
            regime = candidate.get("regime")
            if isinstance(regime, str) and regime:
                return regime
            state = candidate.get("state")
            if isinstance(state, Mapping):
                nested = state.get("regime")
                if isinstance(nested, str) and nested:
                    return nested
        elif isinstance(candidate, str) and candidate:
            return candidate

    return "unknown"


def _resolve_model_identifier(trade_row: Mapping[str, Any], model: Any) -> str:
    version = _extract_model_version(trade_row)
    if version:
        return version

    for attr in ("name", "model_name", "version"):
        value = getattr(model, attr, None)
        if isinstance(value, str) and value:
            return value

    return type(model).__name__


def _extract_reason(trade_row: Mapping[str, Any]) -> Optional[str]:
    direct_candidates: Iterable[Any] = (
        trade_row.get("reason"),
        trade_row.get("decision_reason"),
    )
    for candidate in direct_candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()

    metadata_candidates = (
        trade_row.get("order_metadata"),
        trade_row.get("fill_metadata"),
        trade_row.get("metadata"),
    )
    for container in metadata_candidates:
        if isinstance(container, Mapping):
            for key in ("reason", "decision_reason", "model_reason"):
                value = container.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()

    return None


router = APIRouter(prefix="/reports", tags=["reports"])

RouteFn = TypeVar("RouteFn", bound=Callable[..., Any])


def _router_get(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around :meth:`APIRouter.get` for static analysis."""

    return cast(Callable[[RouteFn], RouteFn], router.get(*args, **kwargs))


def _router_post(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around :meth:`APIRouter.post` for static analysis."""

    return cast(Callable[[RouteFn], RouteFn], router.post(*args, **kwargs))


def _build_service() -> DailyReportService:
    account_id = os.getenv("AETHER_ACCOUNT_ID", "default")
    return DailyReportService(default_account_id=account_id)


_SERVICE: DailyReportService | None = None


def get_daily_report_service() -> DailyReportService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = _build_service()
    return _SERVICE


@_router_get("/daily")
async def get_daily_report(
    account_id: str | None = Query(default=None),
    report_date: date | None = Query(default=None),
    _: str = Depends(require_admin_account),
    service: DailyReportService = Depends(get_daily_report_service),
) -> Dict[str, Any]:
    try:
        report = service.build_daily_report(account_id=account_id, report_date=report_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    payload = report.to_dict()
    try:
        service.persist_report(report, payload)
    except Exception:  # pragma: no cover - best effort persistence
        LOGGER.exception("Failed to persist daily report for account %s", report.account_id)
    return payload


@_router_get("/pnl/daily_pct")
async def get_daily_return_pct(
    account_id: str | None = Query(default=None),
    nav_date: date | None = Query(default=None, alias="date"),
    _: str = Depends(require_admin_account),
    service: DailyReportService = Depends(get_daily_report_service),
) -> Dict[str, Any]:
    try:
        return service.get_daily_return_summary(account_id=account_id, nav_date=nav_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@_router_post("/export")
async def export_daily_report(
    format: str = Query(..., pattern="^(?i)(pdf|csv|json)$"),
    account_id: str | None = Query(default=None),
    report_date: date | None = Query(default=None),
    _: str = Depends(require_admin_account),
    service: DailyReportService = Depends(get_daily_report_service),
) -> StreamingResponse:
    try:
        report = service.build_daily_report(account_id=account_id, report_date=report_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        service.persist_report(report, report.to_dict())
    except Exception:  # pragma: no cover - best effort persistence
        LOGGER.exception("Failed to persist daily report for account %s", report.account_id)
    try:
        payload, filename, content_type = service.export_daily_report(report, fmt=format)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return StreamingResponse(
        io.BytesIO(payload),
        media_type=content_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@_router_get("/explain")
async def explain_trade(
    trade_id: str = Query(..., description="Unique trade or order identifier"),
    _: str = Depends(require_admin_account),
    service: DailyReportService = Depends(get_daily_report_service),
) -> Dict[str, Any]:
    try:
        return service.explain_trade(trade_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except TradeContextError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - unexpected error surface
        LOGGER.exception("Failed to generate explanation for trade %s", trade_id)
        raise HTTPException(status_code=500, detail="Unable to generate explanation") from exc


def compute_daily_return_pct(account_id: Optional[str] = None) -> Optional[float]:
    """Return the daily net return percentage for *account_id*.

    The helper uses :class:`DailyReportService` to build the latest daily report
    and derives the net return as ``(realized + unrealized - fees) / nav``. A
    ``None`` result indicates that the computation could not be completed (for
    example due to missing NAV data or connectivity issues).
    """

    service = get_daily_report_service()
    try:
        report = service.build_daily_report(account_id=account_id)
    except ValueError:
        return None
    except Exception:  # pragma: no cover - surfaced when backend storage fails
        LOGGER.exception(
            "Failed to build daily report for return calculation",
            extra={"account_id": account_id},
        )
        return None

    nav = float(report.nav or 0.0)
    if nav <= 0.0:
        return None

    net_pnl = float(report.realized_pnl) + float(report.unrealized_pnl) - float(report.fees)
    return (net_pnl / nav) * 100.0


__all__ = [
    "DailyReport",
    "DailyReportService",
    "TradeContextError",
    "router",
    "get_daily_report_service",
    "compute_daily_return_pct",
]
