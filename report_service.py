"""FastAPI service exposing operational and explainability reports.

This module provides three HTTP endpoints that materialise analytics from
TimescaleDB and archive the resulting artifacts via :mod:`reports.storage`.

The implementation intentionally keeps the SQL and Pandas transformation layer
lightweight – the service is primarily responsible for orchestrating data flow
between the database, the in-memory analytics frame, and the object storage
layer.  Each request produces a deterministic artifact that is persisted with a
unique identifier so downstream systems (accounting, operations, data science)
can rely on immutable report references.
"""

from __future__ import annotations

import io
import json
import logging
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, Response

from reports.storage import ArtifactStorage, build_storage_from_env
from shared.timezone import format_london_time
from services.models.model_server import get_active_model

try:  # pragma: no cover - psycopg is an optional dependency in tests
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - executed on environments without psycopg
    psycopg = None  # type: ignore[assignment]
    sql = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)


DEFAULT_DSN = "postgresql://timescale:password@localhost:5432/aether"


def _database_url() -> str:
    """Resolve the TimescaleDB connection string from environment variables."""

    return (
        os.getenv("REPORT_DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or DEFAULT_DSN
    )


def _account_schema(account_id: str) -> str | None:
    """Determine the search path schema for *account_id* if configured."""

    env_key = f"AETHER_{account_id.upper()}_TIMESCALE_SCHEMA"
    if env_key in os.environ:
        return os.environ[env_key]
    return os.getenv("TIMESCALE_SCHEMA")


def _connect(account_id: str):
    """Open a psycopg connection scoped to *account_id*."""

    if psycopg is None:  # pragma: no cover - exercised when psycopg is unavailable
        raise HTTPException(
            status_code=503,
            detail="TimescaleDB driver (psycopg) is not installed in this environment.",
        )

    conn = psycopg.connect(_database_url(), row_factory=dict_row)  # type: ignore[arg-type]
    schema = _account_schema(account_id)
    if schema:
        if sql is None:  # pragma: no cover - defensive guard
            conn.close()
            raise HTTPException(status_code=500, detail="SQL helper unavailable")
        statement = sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
        conn.execute(statement)
    return conn


def _to_date(value: str | None, *, default: date) -> date:
    if not value:
        return default
    return date.fromisoformat(value)


def _query_dataframe(conn: Any, query: str, params: Mapping[str, Any]) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame()
    if isinstance(rows[0], Mapping):
        return pd.DataFrame(rows)
    return pd.DataFrame(rows)


def _maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalise_feature_payload(raw: Any) -> dict[str, float]:
    if raw is None:
        return {}
    if isinstance(raw, Mapping):
        normalised: dict[str, float] = {}
        for key, value in raw.items():
            try:
                normalised[str(key)] = float(value)
            except (TypeError, ValueError) as exc:
                raise HTTPException(
                    status_code=422,
                    detail=f"Feature '{key}' is not numeric",
                ) from exc
        return normalised
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        normalised = {}
        for idx, value in enumerate(raw):
            try:
                normalised[f"feature_{idx}"] = float(value)
            except (TypeError, ValueError) as exc:
                raise HTTPException(
                    status_code=422,
                    detail=f"Feature at position {idx} is not numeric",
                ) from exc
        return normalised
    return {}


def _extract_feature_mapping(trade_row: Mapping[str, Any]) -> dict[str, float]:
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

    raise HTTPException(status_code=422, detail="Trade is missing feature metadata")


def _extract_model_version(trade_row: Mapping[str, Any]) -> str | None:
    if isinstance(trade_row.get("model_version"), str):
        return trade_row["model_version"]

    metadata_candidates = (
        trade_row.get("order_metadata"),
        trade_row.get("fill_metadata"),
        trade_row.get("metadata"),
    )
    for container in metadata_candidates:
        if isinstance(container, Mapping):
            model_version = container.get("model_version")
            if isinstance(model_version, str):
                return model_version
    return None


def _load_trade_record(trade_id: str) -> Mapping[str, Any]:
    if psycopg is None:  # pragma: no cover - exercised when psycopg is unavailable
        raise HTTPException(
            status_code=503,
            detail="TimescaleDB driver (psycopg) is not installed in this environment.",
        )

    query = """
        SELECT
            f.fill_id,
            f.order_id,
            COALESCE(f.account_id, o.account_id) AS account_id,
            COALESCE(f.market, f.symbol, o.symbol) AS instrument,
            COALESCE(f.fill_time, f.fill_ts) AS executed_at,
            f.price,
            f.size,
            f.fee,
            f.liquidity,
            f.metadata AS fill_metadata,
            o.metadata AS order_metadata
        FROM fills AS f
        LEFT JOIN orders AS o ON o.order_id = f.order_id
        WHERE f.fill_id = %(trade_id)s
    """

    try:
        with psycopg.connect(_database_url(), row_factory=dict_row) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cursor:
                cursor.execute(query, {"trade_id": trade_id})
                row = cursor.fetchone()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive guard for DB errors
        LOGGER.exception("Failed to load trade context", extra={"trade_id": trade_id})
        raise HTTPException(status_code=500, detail="Unable to load trade context") from exc

    if row is None:
        raise HTTPException(status_code=404, detail="Trade not found")
    return dict(row)


def _first_column(frame: pd.DataFrame, candidates: Sequence[str]) -> str | None:
    for name in candidates:
        if name in frame.columns:
            return name
    return None


def _normalize_timestamp(frame: pd.DataFrame, *, report_date: date) -> pd.Series:
    time_col = _first_column(
        frame,
        (
            "fill_time",
            "submitted_ts",
            "created_at",
            "curve_ts",
            "valuation_ts",
            "ts",
            "bucket_start",
            "timestamp",
        ),
    )
    if time_col is None:
        return pd.Series([report_date] * len(frame))
    series = pd.to_datetime(frame[time_col])
    try:
        series = series.dt.tz_localize(None)
    except TypeError:
        # Already timezone-naive; nothing to strip.
        pass
    return series.dt.date


def _daily_fill_summary(
    conn: Any,
    *,
    account_id: str,
    start: datetime,
    end: datetime,
    report_date: date,
) -> pd.DataFrame:
    query = """
        SELECT
            COALESCE(f.account_id, o.account_id) AS account_id,
            f.fill_time,
            COALESCE(f.market, f.symbol, f.instrument, o.market, o.symbol) AS instrument,
            f.side,
            f.size,
            f.price,
            f.fee
        FROM fills AS f
        LEFT JOIN orders AS o ON o.order_id = f.order_id
        WHERE COALESCE(f.account_id, o.account_id) = %(account_id)s
          AND f.fill_time >= %(start)s
          AND f.fill_time < %(end)s
    """
    frame = _query_dataframe(conn, query, {"account_id": account_id, "start": start, "end": end})
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "session_date",
                "account_id",
                "instrument",
                "trade_count",
                "executed_qty",
                "gross_notional",
                "fees",
            ]
        )

    frame["session_date"] = _normalize_timestamp(frame, report_date=report_date)
    frame["notional"] = frame.get("size", 0).astype(float) * frame.get("price", 0).astype(float)
    frame["fees"] = frame.get("fee", 0).astype(float)
    grouped = (
        frame.groupby(["session_date", "account_id", "instrument"], dropna=False)
        .agg(
            trade_count=("side", "count"),
            executed_qty=("size", "sum"),
            gross_notional=("notional", "sum"),
            fees=("fees", "sum"),
        )
        .reset_index()
    )
    return grouped


def _daily_pnl_summary(
    conn: Any,
    *,
    account_id: str,
    start: datetime,
    end: datetime,
    report_date: date,
) -> pd.DataFrame:
    query = """
        SELECT *
        FROM pnl_curves
        WHERE account_id = %(account_id)s
          AND (COALESCE(curve_ts, valuation_ts, ts, created_at)) >= %(start)s
          AND (COALESCE(curve_ts, valuation_ts, ts, created_at)) < %(end)s
    """
    frame = _query_dataframe(conn, query, {"account_id": account_id, "start": start, "end": end})
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "session_date",
                "account_id",
                "realized_pnl",
                "fees",
                "net_pnl",
                "gross_exposure",
                "net_exposure",
            ]
        )

    frame = frame.copy()
    frame["session_date"] = _normalize_timestamp(frame, report_date=report_date)

    def _column(name_candidates: Sequence[str], default: float = 0.0) -> pd.Series:
        column = _first_column(frame, name_candidates)
        if column is None:
            return pd.Series([default] * len(frame), index=frame.index, dtype=float)
        return frame[column].astype(float)

    frame["realized_pnl"] = _column(("realized_pnl", "realized", "gross_pnl", "pnl"))
    frame["fees_value"] = _column(("fees", "total_fees", "fee"))
    frame["net_pnl"] = _column(("net_pnl", "net", "pnl_net"))
    frame["gross_exposure"] = _column(("gross_exposure", "gross", "total_gross_exposure"))
    frame["net_exposure"] = _column(("net_exposure", "net", "total_net_exposure"))

    grouped = (
        frame.groupby(["session_date", "account_id"], dropna=False)
        .agg(
            realized_pnl=("realized_pnl", "sum"),
            fees=("fees_value", "sum"),
            net_pnl=("net_pnl", "sum"),
            gross_exposure=("gross_exposure", "max"),
            net_exposure=("net_exposure", "max"),
        )
        .reset_index()
    )
    return grouped


def _daily_risk_summary(
    conn: Any,
    *,
    account_id: str,
    start: datetime,
    end: datetime,
    report_date: date,
) -> pd.DataFrame:
    query = """
        SELECT account_id, occurred_at, severity, event_type
        FROM risk_events
        WHERE COALESCE(account_id, %(account_id)s) = %(account_id)s
          AND occurred_at >= %(start)s
          AND occurred_at < %(end)s
    """
    frame = _query_dataframe(conn, query, {"account_id": account_id, "start": start, "end": end})
    if frame.empty:
        return pd.DataFrame(
            columns=["session_date", "account_id", "risk_breaches", "critical_breaches"]
        )

    frame = frame.copy()
    frame["session_date"] = _normalize_timestamp(frame, report_date=report_date)
    severity_col = _first_column(frame, ("severity", "level"))
    if severity_col is None:
        frame["severity_value"] = "info"
    else:
        frame["severity_value"] = frame[severity_col].astype(str)

    summary = (
        frame.groupby(["session_date", "account_id"], dropna=False)
        .agg(
            risk_breaches=("event_type", "count"),
            critical_breaches=("severity_value", lambda col: (col.str.lower() == "critical").sum()),
        )
        .reset_index()
    )
    return summary


def _merge_daily_components(
    fills: pd.DataFrame,
    pnl: pd.DataFrame,
    risk: pd.DataFrame,
) -> pd.DataFrame:
    frame = pd.merge(fills, pnl, on=["session_date", "account_id"], how="outer")
    frame = pd.merge(frame, risk, on=["session_date", "account_id"], how="left")
    for column in (
        "trade_count",
        "executed_qty",
        "gross_notional",
        "fees_x",
        "fees_y",
        "realized_pnl",
        "net_pnl",
        "gross_exposure",
        "net_exposure",
        "risk_breaches",
        "critical_breaches",
    ):
        if column in frame.columns:
            frame[column] = frame[column].fillna(0.0)

    if "fees_x" in frame.columns or "fees_y" in frame.columns:
        frame["fees"] = frame.get("fees_y", 0.0) + frame.get("fees_x", 0.0)
    elif "fees" in frame.columns:
        frame["fees"] = frame["fees"].fillna(0.0)
    else:
        frame["fees"] = 0.0

    if "gross_notional" not in frame.columns:
        frame["gross_notional"] = 0.0

    for missing in ("trade_count", "executed_qty", "realized_pnl", "net_pnl", "gross_exposure", "net_exposure"):
        if missing not in frame.columns:
            frame[missing] = 0.0

    for breach_column in ("risk_breaches", "critical_breaches"):
        if breach_column not in frame.columns:
            frame[breach_column] = 0

    if "instrument" not in frame.columns:
        frame["instrument"] = "aggregate"

    ordered = frame[
        [
            "session_date",
            "account_id",
            "instrument",
            "trade_count",
            "executed_qty",
            "gross_notional",
            "realized_pnl",
            "fees",
            "net_pnl",
            "gross_exposure",
            "net_exposure",
            "risk_breaches",
            "critical_breaches",
        ]
    ].copy()
    ordered.sort_values(["session_date", "instrument"], inplace=True)
    ordered["session_date"] = ordered["session_date"].apply(
        lambda value: value.isoformat() if isinstance(value, date) else str(value)
    )
    return ordered


def _quarter_bounds(as_of: date) -> tuple[date, date]:
    quarter = (as_of.month - 1) // 3
    start_month = quarter * 3 + 1
    start = date(as_of.year, start_month, 1)
    if start_month + 3 > 12:
        end = date(as_of.year, 12, 31)
    else:
        end = date(as_of.year, start_month + 3, 1) - timedelta(days=1)
    return start, end


def _quarterly_summary(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "account_id",
                "trading_days",
                "total_trades",
                "total_quantity",
                "gross_notional",
                "realized_pnl",
                "fees",
                "net_pnl",
                "avg_daily_pnl",
            ]
        )

    grouped = (
        frame.groupby("account_id")
        .agg(
            trading_days=("session_date", "nunique"),
            total_trades=("trade_count", "sum"),
            total_quantity=("executed_qty", "sum"),
            gross_notional=("gross_notional", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            fees=("fees", "sum"),
            net_pnl=("net_pnl", "sum"),
        )
        .reset_index()
    )
    grouped["avg_daily_pnl"] = grouped.apply(
        lambda row: row["net_pnl"] / row["trading_days"] if row["trading_days"] else 0.0,
        axis=1,
    )
    return grouped


def _shap_like_attribution(frame: pd.DataFrame) -> list[dict[str, float | str]]:
    if frame.empty:
        return []
    frame = frame.copy()
    frame["notional"] = frame.get("size", 0).astype(float) * frame.get("price", 0).astype(float)
    contributions: MutableMapping[str, float] = {
        "size": float(frame.get("size", 0).abs().sum()),
        "price": float(frame.get("price", 0).abs().sum()),
        "fees": float(frame.get("fee", 0).abs().sum()),
        "notional": float(frame["notional"].abs().sum()),
    }
    total = sum(contributions.values()) or 1.0
    return [
        {"feature": feature, "attribution": value, "weight": value / total}
        for feature, value in contributions.items()
    ]


def _regime_detection(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    frame = frame.sort_values("fill_time")
    prices = pd.to_numeric(frame.get("price", 0), errors="coerce")
    timestamps = pd.to_datetime(frame.get("fill_time"))
    returns = prices.pct_change().dropna()
    if returns.empty:
        volatility = float(0.0)
    else:
        volatility = float(returns.std())
    regime = "high_volatility" if volatility > 0.02 else "normal"
    start = ""
    end = ""
    if not timestamps.isna().all():
        start = format_london_time(timestamps.min())
        end = format_london_time(timestamps.max())

    return [
        {
            "regime": regime,
            "volatility": volatility,
            "start": start,
            "end": end,
        }
    ]


def _instrument_breakdown(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    frame = frame.copy()
    frame["notional"] = frame.get("size", 0).astype(float) * frame.get("price", 0).astype(float)
    grouped = (
        frame.groupby("instrument", dropna=False)
        .agg(
            trades=("side", "count"),
            quantity=("size", "sum"),
            notional=("notional", "sum"),
            fees=("fee", "sum"),
        )
        .reset_index()
    )
    return grouped.fillna({"instrument": "UNKNOWN"}).to_dict("records")


def _build_html_report(payload: Mapping[str, Any]) -> str:
    instruments = payload.get("instruments", [])
    shap_values = payload.get("shap_values", [])
    regimes = payload.get("regimes", [])
    trades = payload.get("trades", [])

    def _table(rows: Iterable[Mapping[str, Any]]) -> str:
        rows = list(rows)
        if not rows:
            return "<p>No data available.</p>"
        columns = rows[0].keys()
        header = "".join(f"<th>{col}</th>" for col in columns)
        body = "".join(
            "<tr>" + "".join(f"<td>{row[col]}</td>" for col in columns) + "</tr>"
            for row in rows
        )
        return f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"

    html = [
        "<html><head><title>XAI Report</title>",
        "<style>table {border-collapse: collapse;} td, th {border: 1px solid #ccc; padding: 4px;}</style>",
        "</head><body>",
        f"<h1>XAI Report for {payload.get('account_id')}</h1>",
        f"<p>Date: {payload.get('date')}</p>",
        "<h2>Summary</h2>",
        _table([payload.get("summary", {})]) if payload.get("summary") else "<p>No summary.</p>",
        "<h2>Feature Attributions</h2>",
        _table(
            sorted(
                shap_values,
                key=lambda row: abs(
                    float(row.get("importance", row.get("attribution", row.get("weight", 0.0))))
                ),
                reverse=True,
            )[:5]
        ),
        "<h2>Detected Regimes</h2>",
        _table(regimes),
        "<h2>Instrument Breakdown</h2>",
        _table(instruments),
        "<h2>Trades</h2>",
        _table(trades),
        "</body></html>",
    ]
    return "".join(html)


storage: ArtifactStorage = build_storage_from_env(os.environ)

app = FastAPI(title="Report Service")


@app.get("/reports/trade_explain")
def get_trade_explanation(
    trade_id: str = Query(..., description="Unique trade/fill identifier")
) -> JSONResponse:
    """Return the model explanation for a single trade."""

    trade = _load_trade_record(trade_id)
    account_id = trade.get("account_id")
    if not account_id:
        raise HTTPException(status_code=422, detail="Trade is missing account context")
    instrument = trade.get("instrument") or trade.get("symbol")
    if not instrument:
        raise HTTPException(status_code=422, detail="Trade is missing instrument context")

    features = _extract_feature_mapping(trade)
    model = get_active_model(str(account_id), str(instrument))
    raw_importance = model.explain(features)

    ordered = sorted(
        ((name, float(value)) for name, value in raw_importance.items()),
        key=lambda item: abs(item[1]),
        reverse=True,
    )
    feature_importance = [
        {"feature": name, "importance": value}
        for name, value in ordered
    ]
    top_features = feature_importance[:5]

    executed_at = (
        trade.get("executed_at")
        or trade.get("fill_time")
        or trade.get("fill_ts")
    )

    payload = {
        "trade_id": trade_id,
        "account_id": str(account_id),
        "instrument": str(instrument),
        "executed_at": executed_at,
        "price": _maybe_float(trade.get("price")),
        "size": _maybe_float(trade.get("size")),
        "feature_importance": feature_importance,
        "top_features": top_features,
        "model_version": _extract_model_version(trade),
    }
    return JSONResponse(content=payload)


@app.get("/reports/daily")
def get_daily_report(
    account_id: str = Query(..., description="Logical trading account identifier"),
    report_date: str | None = Query(None, description="ISO-8601 date for the report"),
) -> Response:
    """Return the daily operational report as a CSV attachment."""

    resolved_date = _to_date(report_date, default=date.today())
    start = datetime.combine(resolved_date, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    with _connect(account_id) as conn:
        fills = _daily_fill_summary(
            conn, account_id=account_id, start=start, end=end, report_date=resolved_date
        )
        pnl = _daily_pnl_summary(
            conn, account_id=account_id, start=start, end=end, report_date=resolved_date
        )
        risk = _daily_risk_summary(
            conn, account_id=account_id, start=start, end=end, report_date=resolved_date
        )
        merged = _merge_daily_components(fills, pnl, risk)

        buffer = io.StringIO()
        merged.to_csv(buffer, index=False)
        payload = buffer.getvalue().encode("utf-8")

        report_id = uuid.uuid4().hex
        object_key = f"daily/{account_id}/{resolved_date.isoformat()}-{report_id}.csv"
        metadata = {
            "report_type": "daily",
            "report_date": resolved_date.isoformat(),
            "rows": len(merged),
        }
        storage.store_artifact(
            conn,
            account_id=account_id,
            object_key=object_key,
            data=payload,
            content_type="text/csv",
            metadata=metadata,
        )
        conn.commit()
        LOGGER.info(
            "Daily report generated", extra={"account_id": account_id, "rows": len(merged)}
        )

    filename = f"daily-report-{account_id}-{resolved_date.isoformat()}.csv"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=payload, media_type="text/csv", headers=headers)


@app.get("/reports/quarterly")
def get_quarterly_report(
    account_id: str = Query(..., description="Logical trading account identifier"),
    quarter_end: str | None = Query(None, description="Quarter end date in ISO format"),
    fmt: str = Query("csv", description="Output format: csv or parquet"),
) -> Response:
    """Generate quarterly accounting summary in CSV or Parquet format."""

    resolved_end = _to_date(quarter_end, default=date.today())
    quarter_start, quarter_last = _quarter_bounds(resolved_end)
    start = datetime.combine(quarter_start, datetime.min.time(), tzinfo=timezone.utc)
    end = datetime.combine(quarter_last + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)

    with _connect(account_id) as conn:
        fills = _daily_fill_summary(
            conn, account_id=account_id, start=start, end=end, report_date=resolved_end
        )
        pnl = _daily_pnl_summary(
            conn, account_id=account_id, start=start, end=end, report_date=resolved_end
        )
        risk = _daily_risk_summary(
            conn, account_id=account_id, start=start, end=end, report_date=resolved_end
        )
        merged = _merge_daily_components(fills, pnl, risk)
        quarterly = _quarterly_summary(merged)

        fmt_normalized = fmt.lower()
        if fmt_normalized not in {"csv", "parquet"}:
            raise HTTPException(status_code=400, detail="Unsupported format. Use 'csv' or 'parquet'.")

        if fmt_normalized == "csv":
            buffer = io.StringIO()
            quarterly.to_csv(buffer, index=False)
            payload = buffer.getvalue().encode("utf-8")
            content_type = "text/csv"
            extension = "csv"
        else:
            binary = io.BytesIO()
            quarterly.to_parquet(binary, index=False)
            payload = binary.getvalue()
            content_type = "application/vnd.apache.parquet"
            extension = "parquet"

        report_id = uuid.uuid4().hex
        object_key = f"quarterly/{account_id}/{resolved_end.isoformat()}-{report_id}.{extension}"
        metadata = {
            "report_type": "quarterly",
            "quarter_start": quarter_start.isoformat(),
            "quarter_end": quarter_last.isoformat(),
            "rows": len(quarterly),
            "format": extension,
        }
        storage.store_artifact(
            conn,
            account_id=account_id,
            object_key=object_key,
            data=payload,
            content_type=content_type,
            metadata=metadata,
        )
        conn.commit()
        LOGGER.info(
            "Quarterly report generated",
            extra={
                "account_id": account_id,
                "format": extension,
                "rows": len(quarterly),
                "quarter_start": quarter_start.isoformat(),
                "quarter_end": quarter_last.isoformat(),
            },
        )

    filename = f"quarterly-report-{account_id}-{resolved_end.isoformat()}.{extension}"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=payload, media_type=content_type, headers=headers)


@app.get("/reports/xai")
def get_xai_report(
    account_id: str = Query(..., description="Logical trading account identifier"),
    report_date: str = Query(..., description="ISO-8601 date for the XAI evaluation"),
    fmt: str = Query("json", description="Output format: json or html"),
) -> Response:
    """Produce an explainability report summarising trade drivers."""

    resolved_date = _to_date(report_date, default=date.today())
    start = datetime.combine(resolved_date, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    with _connect(account_id) as conn:
        fills = _daily_fill_summary(
            conn, account_id=account_id, start=start, end=end, report_date=resolved_date
        )
        detailed_trades = _query_dataframe(
            conn,
            """
            SELECT
                f.fill_id,
                f.fill_time,
                COALESCE(f.account_id, o.account_id) AS account_id,
                COALESCE(f.market, f.symbol, f.instrument, o.market, o.symbol) AS instrument,
                f.side,
                f.size,
                f.price,
                f.fee
            FROM fills AS f
            LEFT JOIN orders AS o ON o.order_id = f.order_id
            WHERE COALESCE(f.account_id, o.account_id) = %(account_id)s
              AND f.fill_time >= %(start)s
              AND f.fill_time < %(end)s
            ORDER BY f.fill_time ASC
            """,
            {"account_id": account_id, "start": start, "end": end},
        )
        pnl = _daily_pnl_summary(
            conn, account_id=account_id, start=start, end=end, report_date=resolved_date
        )
        risk = _daily_risk_summary(
            conn, account_id=account_id, start=start, end=end, report_date=resolved_date
        )

        summary = _merge_daily_components(fills, pnl, risk)
        shap_values = _shap_like_attribution(detailed_trades)
        regimes = _regime_detection(detailed_trades)
        instruments = _instrument_breakdown(detailed_trades)

        payload_dict = {
            "report_id": uuid.uuid4().hex,
            "account_id": account_id,
            "date": resolved_date.isoformat(),
            "summary": summary.to_dict("records")[0] if not summary.empty else {},
            "shap_values": shap_values,
            "regimes": regimes,
            "instruments": instruments,
            "trades": detailed_trades.to_dict("records"),
        }

        fmt_normalized = fmt.lower()
        if fmt_normalized not in {"json", "html"}:
            raise HTTPException(status_code=400, detail="Unsupported format. Use 'json' or 'html'.")

        if fmt_normalized == "json":
            body_bytes = json.dumps(payload_dict, indent=2).encode("utf-8")
            content_type = "application/json"
            response_obj: Response = JSONResponse(content=payload_dict)
            extension = "json"
        else:
            html = _build_html_report(payload_dict)
            body_bytes = html.encode("utf-8")
            content_type = "text/html"
            response_obj = HTMLResponse(content=html)
            extension = "html"

        object_key = f"xai/{account_id}/{resolved_date.isoformat()}-{payload_dict['report_id']}.{extension}"
        metadata = {
            "report_type": "xai",
            "report_date": resolved_date.isoformat(),
            "format": extension,
            "trades": len(payload_dict.get("trades", [])),
            "features": len(payload_dict.get("shap_values", [])),
        }
        storage.store_artifact(
            conn,
            account_id=account_id,
            object_key=object_key,
            data=body_bytes,
            content_type=content_type,
            metadata=metadata,
        )
        conn.commit()
        LOGGER.info(
            "XAI report generated",
            extra={
                "account_id": account_id,
                "report_date": resolved_date.isoformat(),
                "format": extension,
                "trades": len(payload_dict.get("trades", [])),
            },
        )

    return response_obj


__all__ = ["app"]
