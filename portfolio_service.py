"""FastAPI service exposing portfolio level analytics endpoints."""

from __future__ import annotations

import csv
import io
import os
from datetime import datetime
from typing import Any, Iterable, Mapping

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware

try:  # pragma: no cover - psycopg is optional in certain environments
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - executed when psycopg is unavailable
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


DEFAULT_DSN = "postgresql://timescale:password@localhost:5432/aether"


class AccountScopeMiddleware(BaseHTTPMiddleware):
    """Populate the request state with the allowed account scopes."""

    def __init__(self, app: FastAPI, header_name: str = "x-account-scopes") -> None:
        super().__init__(app)
        self._header_name = header_name

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        header_value = request.headers.get(self._header_name, "")
        scopes = {
            scope.strip()
            for scope in header_value.split(",")
            if scope and scope.strip()
        }
        request.state.account_scopes = scopes
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

    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
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
    scopes = getattr(request.state, "account_scopes", set())
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

