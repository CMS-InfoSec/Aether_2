"""FastAPI service exposing portfolio level analytics endpoints."""

from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware

try:  # pragma: no cover - shared middleware may be unavailable in minimal environments
    from shared.authz_middleware import (  # type: ignore
        BearerTokenError,
        _coerce_account_scopes as _shared_coerce_account_scopes,
        _decode_jwt as _shared_decode_jwt,
        _extract_bearer_token as _shared_extract_bearer_token,
    )
except Exception:  # pragma: no cover - fall back to header based scopes when helpers missing
    BearerTokenError = HTTPException  # type: ignore[assignment]
    _shared_coerce_account_scopes = None  # type: ignore[assignment]
    _shared_decode_jwt = None  # type: ignore[assignment]
    _shared_extract_bearer_token = None  # type: ignore[assignment]

try:  # pragma: no cover - psycopg is optional in certain environments
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - executed when psycopg is unavailable
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


DEFAULT_DSN = "postgresql://timescale:password@localhost:5432/aether"


LOGGER = logging.getLogger(__name__)


class AccountScopeMiddleware(BaseHTTPMiddleware):
    """Populate the request state with the allowed account scopes."""

    def __init__(self, app: FastAPI, header_name: str = "x-account-scopes") -> None:
        super().__init__(app)
        self._header_name = header_name

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        header_value = request.headers.get(self._header_name, "")
        scopes = _normalize_account_scopes(header_value.split(","))

        # Fall back to decoding the bearer token when explicit scope headers are missing.
        if not scopes and _shared_extract_bearer_token and _shared_decode_jwt:
            try:
                token = _shared_extract_bearer_token(request)
                payload = _shared_decode_jwt(token)
            except BearerTokenError:  # pragma: no cover - unauthenticated requests handled downstream
                payload = None
            except HTTPException:
                raise
            else:
                if payload and _shared_coerce_account_scopes:
                    token_scopes = _shared_coerce_account_scopes(payload.get("account_scopes"))
                    scopes = _normalize_account_scopes(token_scopes)

        request.state.account_scopes = scopes
        request.scope["account_scopes"] = scopes
        setattr(request, "account_scopes", scopes)
        return await call_next(request)


def _database_url() -> str:
    """Resolve the Timescale/Postgres connection string."""

    return (
        os.getenv("PORTFOLIO_DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or DEFAULT_DSN
    )


def _connect():
    if psycopg is None:  # pragma: no cover - exercised when psycopg missing
        raise HTTPException(
            status_code=503,
            detail="TimescaleDB driver (psycopg) is not installed in this environment.",
        )
    return psycopg.connect(_database_url(), row_factory=dict_row)  # type: ignore[arg-type]


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid datetime: {value}") from exc


def _query_records(
    request: Request,
    table: str,
    account_id: str,
    *,
    time_column: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    offset: int,
) -> list[Mapping[str, Any]]:
    query = f"SELECT * FROM {table} WHERE account_id = %(account_id)s"
    params: dict[str, Any] = {"account_id": account_id, "limit": limit, "offset": offset}

    if time_column and start is not None:
        query += f" AND {time_column} >= %(start)s"
        params["start"] = start
    if time_column and end is not None:
        query += f" AND {time_column} <= %(end)s"
        params["end"] = end

    if time_column:
        query += f" ORDER BY {time_column} DESC"
    else:
        query += " ORDER BY 1 DESC"

    query += " LIMIT %(limit)s OFFSET %(offset)s"
    rows = _execute_scoped_query(_scopes_from_request(request), query, params)
    return list(rows)


def _to_csv(rows: Iterable[Mapping[str, Any]]) -> io.StringIO:
    buffer = io.StringIO()
    iterator = iter(rows)
    try:
        first = next(iterator)
    except StopIteration:
        return buffer

    writer = csv.DictWriter(buffer, fieldnames=list(first.keys()))
    writer.writeheader()
    writer.writerow(first)
    for row in iterator:
        writer.writerow(row)
    buffer.seek(0)
    return buffer


async def requires_account_scope(
    request: Request,
    account_id: str = Query(..., description="Account identifier"),
) -> str:
    scopes = set(getattr(request.state, "account_scopes", ()))
    if "*" in scopes or account_id in scopes:
        return account_id
    raise HTTPException(status_code=403, detail="Account scope is not authorised")


def _build_response(
    rows: list[Mapping[str, Any]],
    *,
    fmt: str,
    filename: str,
    limit: int,
    offset: int,
) -> StreamingResponse | JSONResponse:
    if fmt == "csv":
        buffer = _to_csv(rows)
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        payload = buffer.getvalue().encode("utf-8")
        return StreamingResponse(
            iter((payload,)),
            media_type="text/csv",
            headers=headers,
        )

    payload = {
        "data": rows,
        "pagination": {"limit": limit, "offset": offset, "returned": len(rows)},
    }
    return JSONResponse(payload)


app = FastAPI(title="Portfolio Service")
app.add_middleware(AccountScopeMiddleware)


def _resolve_format(format: str | None, request: Request) -> str:
    if format:
        return format.lower()
    accept = request.headers.get("accept", "")
    if "text/csv" in accept:
        return "csv"
    return "json"


@app.get("/portfolio/positions")
async def get_positions(
    request: Request,
    account_id: str = Depends(requires_account_scope),
    limit: int = Query(100, ge=1, le=10_000),
    offset: int = Query(0, ge=0),
    format: str | None = Query(None, regex="^(json|csv)$"),
):
    rows = _query_records(
        request,
        "portfolio_positions",
        account_id,
        time_column="as_of",
        start=None,
        end=None,
        limit=limit,
        offset=offset,
    )
    response_format = _resolve_format(format, request)
    return _build_response(
        rows,
        fmt=response_format,
        filename=f"positions_{account_id}.csv",
        limit=limit,
        offset=offset,
    )


@app.get("/portfolio/pnl")
async def get_pnl(
    request: Request,
    account_id: str = Depends(requires_account_scope),
    from_ts: str | None = Query(None, alias="from"),
    to_ts: str | None = Query(None, alias="to"),
    limit: int = Query(500, ge=1, le=50_000),
    offset: int = Query(0, ge=0),
    format: str | None = Query(None, regex="^(json|csv)$"),
):
    start = _parse_datetime(from_ts)
    end = _parse_datetime(to_ts)
    rows = _query_records(
        request,
        "portfolio_pnl_curve",
        account_id,
        time_column="bucket",
        start=start,
        end=end,
        limit=limit,
        offset=offset,
    )
    response_format = _resolve_format(format, request)
    return _build_response(
        rows,
        fmt=response_format,
        filename=f"pnl_{account_id}.csv",
        limit=limit,
        offset=offset,
    )


@app.get("/portfolio/orders")
async def get_orders(
    request: Request,
    account_id: str = Depends(requires_account_scope),
    from_ts: str | None = Query(None, alias="from"),
    to_ts: str | None = Query(None, alias="to"),
    limit: int = Query(500, ge=1, le=50_000),
    offset: int = Query(0, ge=0),
    format: str | None = Query(None, regex="^(json|csv)$"),
):
    start = _parse_datetime(from_ts)
    end = _parse_datetime(to_ts)
    rows = _query_records(
        request,
        "portfolio_orders",
        account_id,
        time_column="created_at",
        start=start,
        end=end,
        limit=limit,
        offset=offset,
    )
    response_format = _resolve_format(format, request)
    return _build_response(
        rows,
        fmt=response_format,
        filename=f"orders_{account_id}.csv",
        limit=limit,
        offset=offset,
    )


@app.get("/portfolio/fills")
async def get_fills(
    request: Request,
    account_id: str = Depends(requires_account_scope),
    from_ts: str | None = Query(None, alias="from"),
    to_ts: str | None = Query(None, alias="to"),
    limit: int = Query(500, ge=1, le=50_000),
    offset: int = Query(0, ge=0),
    format: str | None = Query(None, regex="^(json|csv)$"),
):
    start = _parse_datetime(from_ts)
    end = _parse_datetime(to_ts)
    rows = _query_records(
        request,
        "portfolio_fills",
        account_id,
        time_column="fill_time",
        start=start,
        end=end,
        limit=limit,
        offset=offset,
    )
    response_format = _resolve_format(format, request)
    return _build_response(
        rows,
        fmt=response_format,
        filename=f"fills_{account_id}.csv",
        limit=limit,
        offset=offset,
    )


def _normalize_account_scopes(scopes: Iterable[str] | None) -> tuple[str, ...]:
    """Return a normalised tuple of scope identifiers."""

    normalized: list[str] = []
    seen: set[str] = set()
    if scopes is None:
        return tuple()

    for raw in scopes:
        if raw is None:
            continue
        candidate = str(raw).strip()
        if not candidate or candidate in seen:
            continue
        normalized.append(candidate)
        seen.add(candidate)

    return tuple(normalized)


def _scopes_from_request(request: Request) -> tuple[str, ...]:
    scopes = getattr(request.state, "account_scopes", ())
    normalized = _normalize_account_scopes(scopes)
    if not normalized:
        raise HTTPException(status_code=403, detail="Request is missing account scopes")
    return normalized


def _set_account_scopes(cursor: Any, scopes: Sequence[str]) -> None:
    joined = ",".join(scopes)
    cursor.execute(
        "SELECT set_config('app.account_scopes', %(scopes)s, true)",
        {"scopes": joined},
    )


def _execute_scoped_query(
    account_scopes: Sequence[str],
    query: str,
    params: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    normalized = _normalize_account_scopes(account_scopes)
    if not normalized:
        raise ValueError("account_scopes must not be empty")

    with _connect() as conn:
        with conn.cursor() as cursor:
            _set_account_scopes(cursor, normalized)
            cursor.execute(query, params)
            rows = cursor.fetchall()
    return list(rows)


def query_positions(
    *, account_scopes: Sequence[str], limit: int = 500
) -> list[Mapping[str, Any]]:
    """Return recent position rows scoped by *account_scopes*."""

    query = "SELECT * FROM positions ORDER BY as_of DESC LIMIT %(limit)s"
    return _execute_scoped_query(account_scopes, query, {"limit": limit})


def query_pnl_curves(
    *, account_scopes: Sequence[str], limit: int = 500
) -> list[Mapping[str, Any]]:
    """Return recent PnL curve rows scoped by *account_scopes*."""

    query = "SELECT * FROM pnl_curves ORDER BY curve_ts DESC LIMIT %(limit)s"
    return _execute_scoped_query(account_scopes, query, {"limit": limit})


def query_orders(
    *, account_scopes: Sequence[str], limit: int = 500
) -> list[Mapping[str, Any]]:
    """Return recent order rows scoped by *account_scopes*."""

    query = "SELECT * FROM orders ORDER BY submitted_at DESC LIMIT %(limit)s"
    return _execute_scoped_query(account_scopes, query, {"limit": limit})


def query_fills(
    *, account_scopes: Sequence[str], limit: int = 500
) -> list[Mapping[str, Any]]:
    """Return recent fill rows scoped by *account_scopes*."""

    query = "SELECT * FROM fills ORDER BY fill_time DESC LIMIT %(limit)s"
    return _execute_scoped_query(account_scopes, query, {"limit": limit})


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        return None


def _extract_nav_pair(row: Mapping[str, Any]) -> tuple[float, float] | None:
    if not isinstance(row, Mapping):
        return None

    str_keys = {str(key).lower(): key for key in row.keys() if isinstance(key, str)}
    candidates: list[tuple[float, float, tuple[int, int, str]]] = []

    for lower_key, original_key in str_keys.items():
        if "nav" not in lower_key or "open" not in lower_key:
            continue
        close_lower = lower_key.replace("open", "close", 1)
        close_key = str_keys.get(close_lower)
        if close_key is None:
            continue

        open_val = _coerce_float(row.get(original_key))
        close_val = _coerce_float(row.get(close_key))
        if open_val is None or close_val is None:
            continue

        usd_priority = 0 if "usd" in lower_key else 1
        if "mid" in lower_key:
            source_priority = 0
        elif "mark" in lower_key:
            source_priority = 1
        else:
            source_priority = 2
        priority = (usd_priority, source_priority, lower_key)
        candidates.append((open_val, close_val, priority))

    if not candidates:
        return None

    candidates.sort(key=lambda item: item[2])
    open_val, close_val, _ = candidates[0]
    return float(open_val), float(close_val)


def _nav_value_from_row(row: Mapping[str, Any]) -> float | None:
    if not isinstance(row, Mapping):
        return None

    preferred_keys = (
        "nav_usd_mid",
        "nav_usd",
        "nav",
        "net_asset_value_usd",
        "net_asset_value",
        "equity",
        "balance",
        "total_value",
    )

    lower_map = {str(key).lower(): key for key in row.keys() if isinstance(key, str)}
    for key in preferred_keys:
        actual = lower_map.get(key)
        if actual is None:
            continue
        value = _coerce_float(row.get(actual))
        if value is not None:
            return value

    for lower_key, original_key in lower_map.items():
        if "nav" in lower_key or "value" in lower_key or "equity" in lower_key:
            value = _coerce_float(row.get(original_key))
            if value is not None:
                return value
    return None


def compute_daily_return_pct(account_id: str, ts_now: datetime | None = None) -> float:
    """Return the intraday NAV change in percentage for *account_id*."""

    ts_now = _as_utc(ts_now or datetime.now(timezone.utc))
    session_start = datetime(ts_now.year, ts_now.month, ts_now.day, tzinfo=timezone.utc)

    query = """
        SELECT *
        FROM pnl_curves
        WHERE account_id = %(account_id)s
          AND COALESCE(curve_ts, valuation_ts, ts, created_at) >= %(start)s
          AND COALESCE(curve_ts, valuation_ts, ts, created_at) <= %(end)s
        ORDER BY COALESCE(curve_ts, valuation_ts, ts, created_at)
    """

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                query,
                {"account_id": account_id, "start": session_start, "end": ts_now},
            )
            rows = [dict(row) for row in cursor.fetchall()]

    nav_pair: tuple[float, float] | None = None
    nav_values: list[float] = []

    for row in rows:
        if nav_pair is None:
            nav_pair = _extract_nav_pair(row)
            if nav_pair is not None:
                break
        value = _nav_value_from_row(row)
        if value is not None:
            nav_values.append(value)

    if nav_pair is not None:
        nav_open, nav_close = nav_pair
    elif nav_values:
        nav_open = nav_values[0]
        nav_close = nav_values[-1]
    else:
        return 0.0

    if nav_open <= 0:
        LOGGER.warning(
            "Daily return computation for account %s has nav_open_usd=%s; returning 0.0%%",
            account_id,
            nav_open,
        )
        return 0.0

    return ((nav_close - nav_open) / nav_open) * 100.0

