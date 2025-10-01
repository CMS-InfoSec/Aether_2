"""Collaboration service for trade annotations and configuration proposals."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, List, Mapping

import psycopg
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from psycopg import sql
from psycopg.rows import dict_row

from services.common.config import get_timescale_session
from shared.audit import AuditLogStore, TimescaleAuditLogger
from shared.correlation import CorrelationIdMiddleware


LOGGER = logging.getLogger(__name__)

ACCOUNT_ID = os.getenv("AETHER_ACCOUNT_ID", "default")
TIMESCALE = get_timescale_session(ACCOUNT_ID)

AUDIT_STORE = AuditLogStore()
AUDIT_LOGGER = TimescaleAuditLogger(AUDIT_STORE)

app = FastAPI(title="Collaboration Service", version="1.0.0")
app.add_middleware(CorrelationIdMiddleware)

app.state.audit_store = AUDIT_STORE
app.state.audit_logger = AUDIT_LOGGER


class CommentCreate(BaseModel):
    """Request model for creating a trade collaboration comment."""

    trade_id: str = Field(..., min_length=1, max_length=128)
    author: str = Field(..., min_length=1, max_length=128)
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


def _get_conn() -> psycopg.Connection:
    """Create a connection to the TimescaleDB instance scoped to the account."""

    conn = psycopg.connect(TIMESCALE.dsn)
    conn.execute(
        sql.SQL("SET search_path TO {}, public").format(
            sql.Identifier(TIMESCALE.account_schema)
        )
    )
    return conn


def _ensure_tables() -> None:
    """Create collaboration tables when the service starts."""

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
def create_comment(payload: CommentCreate) -> Comment:
    """Store a new collaboration comment tied to a trade identifier."""

    try:
        with _get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    INSERT INTO collab_comments (trade_id, author, text)
                    VALUES (%s, %s, %s)
                    RETURNING trade_id, author, text, ts
                    """,
                    (payload.trade_id, payload.author, payload.text),
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

    return Comment(**row_dict)


@app.get("/collab/comments", response_model=List[Comment])
def list_comments(trade_id: str = Query(..., min_length=1, max_length=128)) -> List[Comment]:
    """Return comments for the given trade ordered by timestamp."""

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
def create_proposal(payload: ProposalCreate) -> Proposal:
    """Store a new configuration proposal pending review."""

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
    return Proposal(**row_dict)


@app.get("/collab/proposals", response_model=List[Proposal])
def list_pending_proposals() -> List[Proposal]:
    """Return all pending configuration proposals."""

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

