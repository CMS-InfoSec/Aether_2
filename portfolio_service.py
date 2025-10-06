"""FastAPI service exposing portfolio level analytics endpoints."""

from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware

from shared.spot import is_spot_symbol, normalize_spot_symbol

from services.portfolio.balance_reader import BalanceReader, BalanceRetrievalError
try:
    from services.common.security import (
        AuthenticatedPrincipal,
        _get_session_store,
        require_admin_account,
        require_authenticated_principal,
    )
except ModuleNotFoundError:  # pragma: no cover - fallback for dynamic test loaders
    ROOT = Path(__file__).resolve().parent
    if str(ROOT) not in os.sys.path:
        os.sys.path.insert(0, str(ROOT))
    import importlib.util

    services_path = ROOT / "services" / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        "services", services_path, submodule_search_locations=[str(services_path.parent)]
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise
    module = importlib.util.module_from_spec(spec)
    os.sys.modules.setdefault("services", module)
    spec.loader.exec_module(module)

    common_path = ROOT / "services" / "common" / "__init__.py"
    common_spec = importlib.util.spec_from_file_location(
        "services.common", common_path, submodule_search_locations=[str(common_path.parent)]
    )
    if common_spec is None or common_spec.loader is None:  # pragma: no cover
        raise
    common_module = importlib.util.module_from_spec(common_spec)
    os.sys.modules.setdefault("services.common", common_module)
    common_spec.loader.exec_module(common_module)

    security_path = ROOT / "services" / "common" / "security.py"
    security_spec = importlib.util.spec_from_file_location("services.common.security", security_path)
    if security_spec is None or security_spec.loader is None:  # pragma: no cover
        raise
    security_module = importlib.util.module_from_spec(security_spec)
    os.sys.modules.setdefault("services.common.security", security_module)
    security_spec.loader.exec_module(security_module)

    AuthenticatedPrincipal = security_module.AuthenticatedPrincipal  # type: ignore[assignment]
    _get_session_store = security_module._get_session_store  # type: ignore[assignment]
    require_admin_account = security_module.require_admin_account  # type: ignore[assignment]
    require_authenticated_principal = security_module.require_authenticated_principal  # type: ignore[assignment]

try:  # pragma: no cover - psycopg is optional in certain environments
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - executed when psycopg is unavailable
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


DEFAULT_DSN = "postgresql://timescale:password@localhost:5432/aether"


LOGGER = logging.getLogger(__name__)

_SPOT_KEYWORDS: tuple[str, ...] = ("symbol", "instrument", "pair", "market")


class AccountScopeMiddleware(BaseHTTPMiddleware):
    """Populate the request state with account scopes derived from the session."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        scopes: tuple[str, ...] = tuple()
        principal: AuthenticatedPrincipal | None = None

        try:
            principal = require_authenticated_principal(
                request, authorization=request.headers.get("Authorization")
            )
        except HTTPException:
            principal = None
        else:
            scopes = _account_scopes_from_session(request, principal)
            request.state.authenticated_principal = principal
            request.scope["authenticated_principal"] = principal

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
    rows = _execute_scoped_query(
        _scopes_from_request(request), query, params, table=table
    )
    return rows


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
    _: str = Depends(require_admin_account),
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


BALANCE_READER = BalanceReader()


def invalidate_balance_cache(account_id: str | None = None) -> None:
    """Expose cache invalidation for external event hooks."""

    BALANCE_READER.invalidate(account_id)


def _resolve_format(format: str | None, request: Request) -> str:
    if format:
        return format.lower()
    accept = request.headers.get("accept", "")
    if "text/csv" in accept:
        return "csv"
    return "json"


@app.get("/portfolio/balances")
async def get_balances(
    account_id: str = Depends(requires_account_scope),
    _: str = Depends(require_admin_account),
) -> Mapping[str, float]:
    try:
        return await BALANCE_READER.get_account_balances(account_id)
    except BalanceRetrievalError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.get("/portfolio/positions")
async def get_positions(
    request: Request,
    account_id: str = Depends(requires_account_scope),
    _: str = Depends(require_admin_account),
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
    _: str = Depends(require_admin_account),
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
    _: str = Depends(require_admin_account),
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
    _: str = Depends(require_admin_account),
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


def _normalize_account_scopes(scopes: Iterable[str] | str | None) -> tuple[str, ...]:
    """Return a normalised tuple of scope identifiers."""

    normalized: list[str] = []
    seen: set[str] = set()
    if scopes is None:
        return tuple()

    if isinstance(scopes, str):
        scopes = scopes.split(",")

    for raw in scopes:
        if raw is None:
            continue
        candidate = str(raw).strip()
        if not candidate or candidate in seen:
            continue
        normalized.append(candidate)
        seen.add(candidate)

    return tuple(normalized)


def _account_scopes_from_session(
    request: Request, principal: AuthenticatedPrincipal
) -> tuple[str, ...]:
    """Extract account scopes bound to the authenticated session."""

    try:
        store = _get_session_store(request)
    except HTTPException as exc:  # pragma: no cover - misconfiguration bubbled up
        LOGGER.error("Session store misconfigured for account scope enforcement", exc_info=exc)
        raise

    session = store.get(principal.token)
    if session is None:  # pragma: no cover - defensive guard, validation already occurred
        LOGGER.warning(
            "Authenticated principal %s missing session when deriving account scopes",
            principal.account_id,
        )
        return tuple()

    raw_scopes = getattr(session, "account_scopes", None)
    normalized = _normalize_account_scopes(raw_scopes)
    if normalized:
        return normalized

    fallback = principal.normalized_account
    return (fallback,) if fallback else tuple()


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
    *,
    table: str | None = None,
) -> list[Mapping[str, Any]]:
    normalized = _normalize_account_scopes(account_scopes)
    if not normalized:
        raise ValueError("account_scopes must not be empty")

    with _connect() as conn:
        with conn.cursor() as cursor:
            _set_account_scopes(cursor, normalized)
            cursor.execute(query, params)
            rows = cursor.fetchall()
    return _filter_spot_rows(rows, table=table)


def query_positions(
    *, account_scopes: Sequence[str], limit: int = 500
) -> list[Mapping[str, Any]]:
    """Return recent position rows scoped by *account_scopes*."""

    query = "SELECT * FROM positions ORDER BY as_of DESC LIMIT %(limit)s"
    return _execute_scoped_query(
        account_scopes, query, {"limit": limit}, table="positions"
    )


def query_pnl_curves(
    *, account_scopes: Sequence[str], limit: int = 500
) -> list[Mapping[str, Any]]:
    """Return recent PnL curve rows scoped by *account_scopes*."""

    query = "SELECT * FROM pnl_curves ORDER BY curve_ts DESC LIMIT %(limit)s"
    return _execute_scoped_query(
        account_scopes, query, {"limit": limit}, table="pnl_curves"
    )


def query_orders(
    *, account_scopes: Sequence[str], limit: int = 500
) -> list[Mapping[str, Any]]:
    """Return recent order rows scoped by *account_scopes*."""

    query = "SELECT * FROM orders ORDER BY submitted_at DESC LIMIT %(limit)s"
    return _execute_scoped_query(
        account_scopes, query, {"limit": limit}, table="orders"
    )


def query_fills(
    *, account_scopes: Sequence[str], limit: int = 500
) -> list[Mapping[str, Any]]:
    """Return recent fill rows scoped by *account_scopes*."""

    query = "SELECT * FROM fills ORDER BY fill_time DESC LIMIT %(limit)s"
    return _execute_scoped_query(
        account_scopes, query, {"limit": limit}, table="fills"
    )


def _filter_spot_rows(
    rows: Iterable[Mapping[str, Any]], *, table: str | None
) -> list[Mapping[str, Any]]:
    """Return rows restricted to spot instruments, normalizing canonical symbols."""

    filtered: list[Mapping[str, Any]] = []

    for row in rows:
        if not isinstance(row, Mapping):
            continue

        normalized_row: MutableMapping[str, Any] = dict(row)
        drop_row = False

        for key, value in list(normalized_row.items()):
            if not isinstance(key, str):
                continue

            lowered = key.lower()
            if not any(keyword in lowered for keyword in _SPOT_KEYWORDS):
                continue

            normalized_symbol = normalize_spot_symbol(value)
            if not normalized_symbol or not is_spot_symbol(normalized_symbol):
                LOGGER.warning(
                    "Dropping non-spot instrument '%s' from %s results",
                    value,
                    table or "portfolio",
                )
                drop_row = True
                break

            normalized_row[key] = normalized_symbol

        if drop_row:
            continue

        filtered.append(dict(normalized_row))

    return filtered


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

