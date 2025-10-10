"""Collaboration service for trade annotations and configuration proposals."""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from itertools import count
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

try:  # psycopg is optional in lightweight environments such as CI.
    import psycopg  # type: ignore[import-untyped]
    from psycopg import sql  # type: ignore[import-untyped]
    from psycopg.rows import dict_row  # type: ignore[import-untyped]
except ModuleNotFoundError:  # pragma: no cover - exercised in tests via stubs.
    psycopg = None  # type: ignore[assignment]

    class _SQLString(str):
        """Minimal stand-in that preserves chaining behaviour."""

        def format(self, *_args: object, **_kwargs: object) -> "_SQLString":
            return self

    sql = type(  # type: ignore[assignment]
        "_SQLShim",
        (),
        {
            "SQL": staticmethod(lambda value: _SQLString(value)),
            "Identifier": staticmethod(lambda value: value),
        },
    )

    def dict_row(*_args: object, **_kwargs: object) -> Callable[[Iterable[Any]], MutableMapping[str, Any]]:
        return lambda row: dict(row) if isinstance(row, Mapping) else row  # type: ignore[return-value]


from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

from services.common.config import get_timescale_session
from services.common.security import require_admin_account
from shared.audit import AuditLogStore, TimescaleAuditLogger
from shared.correlation import CorrelationIdMiddleware


LOGGER = logging.getLogger(__name__)

ACCOUNT_ID = os.getenv("AETHER_ACCOUNT_ID", "default")
TIMESCALE = get_timescale_session(ACCOUNT_ID)

AUDIT_STORE = AuditLogStore()
AUDIT_LOGGER = TimescaleAuditLogger(AUDIT_STORE)


class _InMemoryCollabStore:
    """Lightweight persistence used when psycopg/Timescale are unavailable."""

    def __init__(self) -> None:
        self._comment_rows: list[dict[str, Any]] = []
        self._proposal_rows: list[dict[str, Any]] = []
        self._proposal_ids = count(1)
        self._lock = threading.Lock()

    def ensure_ready(self) -> None:
        """Mirror the table bootstrap performed by the real backend."""

    def connect(self) -> "_InMemoryConnection":
        return _InMemoryConnection(self)

    def insert_comment(self, trade_id: str, author: str, text: str) -> dict[str, Any]:
        with self._lock:
            row = {
                "trade_id": trade_id,
                "author": author,
                "text": text,
                "ts": datetime.now(timezone.utc),
            }
            self._comment_rows.append(row)
            return dict(row)

    def list_comments(self, trade_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = [row for row in self._comment_rows if row["trade_id"] == trade_id]
        return sorted(rows, key=lambda row: row["ts"])

    def insert_proposal(self, key: str, new_value: str, reason: str) -> dict[str, Any]:
        with self._lock:
            row = {
                "id": next(self._proposal_ids),
                "key": key,
                "new_value": new_value,
                "reason": reason,
                "status": "pending",
                "ts": datetime.now(timezone.utc),
            }
            self._proposal_rows.append(row)
            return dict(row)

    def list_pending_proposals(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = [row for row in self._proposal_rows if row["status"] == "pending"]
        return sorted(rows, key=lambda row: row["ts"], reverse=True)


class _InMemoryCursor:
    """Cursor implementation that emulates the psycopg interface for tests."""

    def __init__(self, store: _InMemoryCollabStore) -> None:
        self._store = store
        self._last_row: Optional[dict[str, Any]] = None
        self._rows: list[dict[str, Any]] = []
        self.executed: list[tuple[Any, ...] | None] = []

    def __enter__(self) -> "_InMemoryCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, query: str, params: Optional[tuple[Any, ...]] = None) -> None:
        normalized = " ".join(query.split()).strip().lower()
        self.executed.append(params)

        if normalized.startswith("insert into collab_comments"):
            if not params:
                raise ValueError("collab comment insert requires parameters")
            trade_id, author, text = params
            row = self._store.insert_comment(str(trade_id), str(author), str(text))
            self._rows = [row]
            self._last_row = row
        elif normalized.startswith("select trade_id, author, text, ts from collab_comments"):
            if not params:
                raise ValueError("collab comments query requires trade identifier")
            rows = self._store.list_comments(str(params[0]))
            self._rows = rows
            self._last_row = rows[0] if rows else None
        elif normalized.startswith("insert into collab_proposals"):
            if not params:
                raise ValueError("collab proposal insert requires parameters")
            key, new_value, reason = params
            row = self._store.insert_proposal(str(key), str(new_value), str(reason))
            self._rows = [row]
            self._last_row = row
        elif normalized.startswith(
            "select id, key, new_value, reason, status, ts from collab_proposals"
        ):
            rows = self._store.list_pending_proposals()
            self._rows = rows
            self._last_row = rows[0] if rows else None
        elif normalized.startswith("create table") or normalized.startswith("create index"):
            self._rows = []
            self._last_row = None
        elif normalized.startswith("select"):
            raise RuntimeError(f"Unsupported SELECT under in-memory backend: {query}")
        else:
            raise RuntimeError(f"Unsupported statement under in-memory backend: {query}")

    def fetchone(self) -> Optional[dict[str, Any]]:
        return self._last_row

    def fetchall(self) -> List[dict[str, Any]]:
        return list(self._rows)


class _InMemoryConnection:
    """Context manager compatible connection facade used for fallbacks."""

    def __init__(self, store: _InMemoryCollabStore) -> None:
        self._store = store

    def __enter__(self) -> "_InMemoryConnection":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def cursor(self, **_kwargs: object) -> _InMemoryCursor:
        return _InMemoryCursor(self._store)


_BACKEND = _InMemoryCollabStore() if psycopg is None else None

app = FastAPI(title="Collaboration Service", version="1.0.0")
app.add_middleware(CorrelationIdMiddleware)

app.state.audit_store = AUDIT_STORE
app.state.audit_logger = AUDIT_LOGGER


class CommentCreate(BaseModel):
    """Request model for creating a trade collaboration comment."""

    trade_id: str = Field(..., min_length=1, max_length=128)
    text: str = Field(..., min_length=1, max_length=10_000)


class Comment(BaseModel):
    """Representation of a stored comment."""

    trade_id: str
    author: str
    text: str
    ts: datetime


class ProposalCreate(BaseModel):
    """Request model for submitting a configuration proposal."""

    key: str = Field(..., min_length=1, max_length=256)
    new_value: Any
    reason: str = Field(..., min_length=1, max_length=10_000)


class Proposal(BaseModel):
    """Representation of a configuration proposal."""

    id: int
    key: str
    new_value: Any
    reason: str
    status: str
    ts: datetime


def _get_conn() -> Any:
    """Create a connection to the persistence backend scoped to the account."""

    if psycopg is None:
        if _BACKEND is None:  # pragma: no cover - defensive guard.
            raise RuntimeError("In-memory backend unavailable")
        return _BACKEND.connect()

    conn = psycopg.connect(TIMESCALE.dsn)
    conn.execute(
        sql.SQL("SET search_path TO {}, public").format(
            sql.Identifier(TIMESCALE.account_schema)
        )
    )
    return conn


def _ensure_tables() -> None:
    """Create collaboration tables when the service starts."""

    if psycopg is None:
        if _BACKEND is not None:
            _BACKEND.ensure_ready()
        return

    try:
        with _get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS collab_comments (
                    id BIGSERIAL PRIMARY KEY,
                    trade_id TEXT NOT NULL,
                    author TEXT NOT NULL,
                    text TEXT NOT NULL,
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS collab_comments_trade_ts_idx
                ON collab_comments (trade_id, ts DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS collab_proposals (
                    id BIGSERIAL PRIMARY KEY,
                    key TEXT NOT NULL,
                    new_value TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS collab_proposals_status_ts_idx
                ON collab_proposals (status, ts DESC)
                """
            )
    except Exception:  # pragma: no cover - defensive logging for startup issues.
        LOGGER.exception("Failed to ensure collaboration tables exist")
        raise


@app.on_event("startup")
def startup_event() -> None:
    """FastAPI startup hook to bootstrap database structures."""

    _ensure_tables()


def _assert_account_scope(actor_account: str) -> None:
    """Ensure the authenticated caller matches the service account scope."""

    normalized_actor = actor_account.strip().lower()
    normalized_scope = ACCOUNT_ID.strip().lower()
    if normalized_scope and normalized_actor != normalized_scope:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authenticated account does not match collaboration scope.",
        )


def _record_comment_audit(row: Mapping[str, Any]) -> dict[str, Any]:
    """Persist an audit entry for a new comment."""

    row_data = dict(row)
    AUDIT_LOGGER.record(
        action="collab.comment.created",
        actor_id=row_data["author"],
        before=None,
        after={
            "trade_id": row_data["trade_id"],
            "text": row_data["text"],
            "ts": row_data["ts"].isoformat()
            if isinstance(row_data["ts"], datetime)
            else row_data["ts"],
        },
    )
    return row_data


def _record_proposal_audit(row: Mapping[str, Any]) -> dict[str, Any]:
    """Persist an audit entry for a new configuration proposal."""

    row_data = dict(row)
    AUDIT_LOGGER.record(
        action="collab.proposal.created",
        actor_id="collab-service",
        before=None,
        after={
            "proposal_id": row_data["id"],
            "key": row_data["key"],
            "new_value": row_data["new_value"],
            "reason": row_data["reason"],
            "ts": row_data["ts"].isoformat()
            if isinstance(row_data["ts"], datetime)
            else row_data["ts"],
        },
    )
    return row_data


@app.post("/collab/comment", response_model=Comment, status_code=status.HTTP_201_CREATED)
def create_comment(
    payload: CommentCreate,
    response: Response,
    actor_account: str = Depends(require_admin_account),
) -> Comment:
    """Store a new collaboration comment tied to a trade identifier."""

    _assert_account_scope(actor_account)

    try:
        with _get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    INSERT INTO collab_comments (trade_id, author, text)
                    VALUES (%s, %s, %s)
                    RETURNING trade_id, author, text, ts
                    """,
                    (payload.trade_id, actor_account, payload.text),
                )
                row = cur.fetchone()
    except Exception as exc:  # pragma: no cover - network/database errors.
        LOGGER.exception("Failed to insert collaboration comment")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to store collaboration comment",
        ) from exc

    if row is None:  # pragma: no cover - defensive.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to store collaboration comment",
        )

    row_dict = _record_comment_audit(row)

    comment = Comment(**row_dict)
    if response is None:
        payload = (
            comment.model_dump() if hasattr(comment, "model_dump") else comment.dict()
        )
        return JSONResponse(content=payload, status_code=status.HTTP_201_CREATED)

    response.status_code = status.HTTP_201_CREATED
    return comment


@app.get("/collab/comments", response_model=List[Comment])
def list_comments(
    trade_id: str = Query(..., min_length=1, max_length=128),
    actor_account: str = Depends(require_admin_account),
) -> List[Comment]:
    """Return comments for the given trade ordered by timestamp."""

    _assert_account_scope(actor_account)

    try:
        with _get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT trade_id, author, text, ts
                    FROM collab_comments
                    WHERE trade_id = %s
                    ORDER BY ts ASC
                    """,
                    (trade_id,),
                )
                rows = cur.fetchall()
    except Exception as exc:  # pragma: no cover - network/database errors.
        LOGGER.exception("Failed to fetch collaboration comments")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch collaboration comments",
        ) from exc

    return [Comment(**dict(row)) for row in rows]


def _normalize_new_value(value: Any) -> str:
    """Serialize proposal values for persistence."""

    return json.dumps(value, separators=(",", ":"))


def _decode_new_value(serialized: str) -> Any:
    """Deserialize stored proposal values."""

    try:
        return json.loads(serialized)
    except json.JSONDecodeError:  # pragma: no cover - defensive fallback.
        return serialized


@app.post("/collab/proposal", response_model=Proposal, status_code=status.HTTP_201_CREATED)
def create_proposal(
    payload: ProposalCreate,
    response: Response,
    actor_account: str = Depends(require_admin_account),
) -> Proposal:
    """Store a new configuration proposal pending review."""

    _assert_account_scope(actor_account)
    serialized_value = _normalize_new_value(payload.new_value)

    try:
        with _get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    INSERT INTO collab_proposals (key, new_value, reason)
                    VALUES (%s, %s, %s)
                    RETURNING id, key, new_value, reason, status, ts
                    """,
                    (payload.key, serialized_value, payload.reason),
                )
                row = cur.fetchone()
    except Exception as exc:  # pragma: no cover - network/database errors.
        LOGGER.exception("Failed to store configuration proposal")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to store configuration proposal",
        ) from exc

    if row is None:  # pragma: no cover - defensive.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to store configuration proposal",
        )

    row_dict = _record_proposal_audit(row)
    row_dict["new_value"] = _decode_new_value(row_dict["new_value"])
    proposal = Proposal(**row_dict)
    if response is None:
        payload = (
            proposal.model_dump() if hasattr(proposal, "model_dump") else proposal.dict()
        )
        return JSONResponse(content=payload, status_code=status.HTTP_201_CREATED)

    response.status_code = status.HTTP_201_CREATED
    return proposal


@app.get("/collab/proposals", response_model=List[Proposal])
def list_pending_proposals(
    actor_account: str = Depends(require_admin_account),
) -> List[Proposal]:
    """Return all pending configuration proposals."""

    _assert_account_scope(actor_account)

    try:
        with _get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT id, key, new_value, reason, status, ts
                    FROM collab_proposals
                    WHERE status = 'pending'
                    ORDER BY ts DESC
                    """
                )
                rows = cur.fetchall()
    except Exception as exc:  # pragma: no cover - network/database errors.
        LOGGER.exception("Failed to fetch configuration proposals")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch configuration proposals",
        ) from exc

    proposals = []
    for row in rows:
        row_dict = dict(row)
        row_dict["new_value"] = _decode_new_value(row_dict["new_value"])
        proposals.append(Proposal(**row_dict))
    return proposals


__all__ = ["app"]

