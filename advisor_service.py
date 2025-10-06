"""FastAPI microservice that provides operations advisor summaries.

The service exposes a single `/advisor/query` endpoint that accepts a
question and responds with a narrative rooted in recent operational data
including logs, risk events, anomalies, and profit-and-loss performance.

If OpenAI or Anthropic API keys are supplied via environment variables the
service will call the respective GPT endpoints to generate the narrative. When
no model provider is configured a deterministic fallback summary is returned.

All queries and responses are persisted to the ``advisor_queries`` table so the
conversations can be audited later on.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field, validator
from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from auth.service import (
    InMemorySessionStore,
    SessionStoreProtocol,
    build_session_store_from_url,
)
from services.common import security
from services.common.security import require_admin_account
from shared.postgres import normalize_sqlalchemy_dsn
from shared.session_config import load_session_ttl_minutes

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

_DATABASE_URL_ENV = "ADVISOR_DATABASE_URL"
_SQLITE_FALLBACK_FLAG = "ADVISOR_ALLOW_SQLITE_FOR_TESTS"
_SSL_MODE_ENV = "ADVISOR_DB_SSLMODE"
_POOL_SIZE_ENV = "ADVISOR_DB_POOL_SIZE"
_MAX_OVERFLOW_ENV = "ADVISOR_DB_MAX_OVERFLOW"
_POOL_TIMEOUT_ENV = "ADVISOR_DB_POOL_TIMEOUT"
_POOL_RECYCLE_ENV = "ADVISOR_DB_POOL_RECYCLE"


def _allow_sqlite_fallback() -> bool:
    """Return whether sqlite DSNs are permitted (tests and explicit overrides only)."""

    if "pytest" in sys.modules:
        return True

    if os.getenv(_SQLITE_FALLBACK_FLAG) == "1":
        LOGGER.warning(
            "%s=1 allows sqlite-backed advisor persistence; never enable in production.",
            _SQLITE_FALLBACK_FLAG,
        )
        return True

    return False


def _require_database_url() -> str:
    """Return the managed Postgres/Timescale database URL for advisor history."""

    raw_url = os.getenv(_DATABASE_URL_ENV)
    if raw_url is None:
        raise RuntimeError(
            "ADVISOR_DATABASE_URL must be set and point to the shared TimescaleDB cluster."
        )

    allow_sqlite = _allow_sqlite_fallback()

    return normalize_sqlalchemy_dsn(
        raw_url,
        allow_sqlite=allow_sqlite,
        label="ADVISOR_DATABASE_URL",
    )


def _create_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine configured for pooled, managed connections."""

    connect_args: Dict[str, Any] = {}
    engine_kwargs: Dict[str, Any] = {
        "future": True,
        "pool_pre_ping": True,
    }

    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        engine_kwargs["connect_args"] = connect_args
        if ":memory:" in database_url:
            engine_kwargs["poolclass"] = StaticPool
    else:
        connect_args["sslmode"] = os.getenv(_SSL_MODE_ENV, "require")
        engine_kwargs["connect_args"] = connect_args
        engine_kwargs.update(
            pool_size=int(os.getenv(_POOL_SIZE_ENV, "10")),
            max_overflow=int(os.getenv(_MAX_OVERFLOW_ENV, "5")),
            pool_timeout=int(os.getenv(_POOL_TIMEOUT_ENV, "30")),
            pool_recycle=int(os.getenv(_POOL_RECYCLE_ENV, "1800")),
        )

    return create_engine(database_url, **engine_kwargs)


ENGINE: Engine | None = None
SessionLocal: sessionmaker | None = None
Base = declarative_base()


class AdvisorQuery(Base):
    """SQLAlchemy model backing the ``advisor_queries`` table."""

    __tablename__ = "advisor_queries"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    user_id: str = Column(String(255), nullable=False, index=True)
    question: str = Column(String(4096), nullable=False)
    answer: str = Column(String(8192), nullable=False)
    context: Dict[str, Any] = Column(JSON, nullable=False, default=dict)
    created_at: datetime = Column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )


def _init_database(application: FastAPI) -> None:
    """Initialise the shared database engine and session factory."""

    global ENGINE, SessionLocal

    if getattr(application.state, "db_engine", None) is not None:
        return

    if ENGINE is None or SessionLocal is None:
        database_url = _require_database_url()
        engine = _create_engine(database_url)
        SessionLocal = sessionmaker(
            bind=engine, autocommit=False, autoflush=False, future=True
        )
        ENGINE = engine

    application.state.db_engine = ENGINE
    application.state.db_session_factory = SessionLocal


def get_db(request: Request) -> Iterable[Session]:
    """Provide a database session dependency for FastAPI routes."""

    session_factory = getattr(request.app.state, "db_session_factory", None)
    if session_factory is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database session factory is not initialised.",
        )

    db = session_factory()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Data acquisition helpers
# ---------------------------------------------------------------------------


def _load_json_lines(path: Path, *, limit: int) -> List[Dict[str, Any]]:
    """Load a JSONL file returning the most recent ``limit`` entries."""

    if not path.exists():
        return []

    lines: List[str] = path.read_text(encoding="utf-8").strip().splitlines()
    payloads: List[Dict[str, Any]] = []
    for line in reversed(lines[-limit:]):
        try:
            payloads.append(json.loads(line))
        except json.JSONDecodeError:
            LOGGER.debug("Skipping malformed json line in %s", path)
    return list(reversed(payloads))


def fetch_recent_logs(*, limit: int = 20) -> List[Dict[str, Any]]:
    """Return recent operational logs from the configured log file."""

    log_path = Path(os.getenv("ADVISOR_LOG_PATH", "ops/event_log.jsonl"))
    return _load_json_lines(log_path, limit=limit)


def fetch_recent_risk_events(*, lookback_hours: int = 24) -> List[Dict[str, Any]]:
    """Return the most recent risk events within the lookback window."""

    risk_path = Path(os.getenv("ADVISOR_RISK_EVENTS", "ops/risk_events.jsonl"))
    events = _load_json_lines(risk_path, limit=200)
    if not events:
        return []

    cutoff = datetime.now(UTC) - timedelta(hours=lookback_hours)
    recent: List[Dict[str, Any]] = []
    for event in events:
        timestamp = event.get("timestamp") or event.get("ts")
        if not timestamp:
            continue
        if isinstance(timestamp, str):
            timestamp = timestamp.strip()
            if timestamp.lower().endswith("z"):
                timestamp = f"{timestamp[:-1]}+00:00"
        try:
            event_ts = datetime.fromisoformat(timestamp)
        except Exception:  # pragma: no cover - defensive branch
            continue
        if event_ts.tzinfo is None:
            event_ts = event_ts.replace(tzinfo=UTC)
        if event_ts >= cutoff:
            recent.append(event)
    return recent


def fetch_recent_anomalies(*, limit: int = 20) -> List[Dict[str, Any]]:
    """Load the latest anomaly events detected by the platform."""

    anomaly_path = Path(os.getenv("ADVISOR_ANOMALY_PATH", "ops/anomalies.jsonl"))
    return _load_json_lines(anomaly_path, limit=limit)


def fetch_recent_pnl(*, window_days: int = 2) -> List[Dict[str, Any]]:
    """Return rolling PnL snapshots for the requested window."""

    pnl_path = Path(os.getenv("ADVISOR_PNL_PATH", "ops/pnl.json"))
    if not pnl_path.exists():
        return []

    try:
        payload = json.loads(pnl_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("Failed to parse PnL data at %s", pnl_path)
        return []

    # Expect a list of daily snapshots sorted ascending.
    snapshots = payload if isinstance(payload, list) else payload.get("snapshots", [])
    cutoff = datetime.now(UTC).date() - timedelta(days=window_days)
    recent: List[Dict[str, Any]] = []
    for row in snapshots:
        raw_date = row.get("date") or row.get("as_of")
        if not raw_date:
            continue
        try:
            snapshot_date = datetime.fromisoformat(raw_date).date()
        except Exception:  # pragma: no cover - defensive branch
            continue
        if snapshot_date >= cutoff:
            recent.append(row)
    return recent


# ---------------------------------------------------------------------------
# LLM summarisation
# ---------------------------------------------------------------------------


class AdvisorSummarizer:
    """Bridge to either OpenAI or Anthropic GPT style APIs."""

    def __init__(
        self,
        *,
        openai_key: str | None = None,
        anthropic_key: str | None = None,
        openai_model: str = "gpt-4o-mini",
        anthropic_model: str = "claude-3-haiku-20240307",
        timeout: float = 30.0,
    ) -> None:
        self.openai_key = openai_key or os.getenv("OPENAI_API_KEY")
        self.openai_model = openai_model
        self.anthropic_key = anthropic_key or os.getenv("ANTHROPIC_API_KEY")
        self.anthropic_model = anthropic_model
        self.timeout = timeout

    async def summarize(self, question: str, context: Dict[str, Any]) -> str:
        prompt = self._build_prompt(question=question, context=context)
        if self.openai_key:
            try:
                return await self._summarize_openai(prompt)
            except Exception as exc:  # pragma: no cover - network failures
                LOGGER.warning("OpenAI summarisation failed: %s", exc, exc_info=True)

        if self.anthropic_key:
            try:
                return await self._summarize_anthropic(prompt)
            except Exception as exc:  # pragma: no cover - network failures
                LOGGER.warning("Anthropic summarisation failed: %s", exc, exc_info=True)

        LOGGER.info("Falling back to deterministic summary")
        return self._fallback_summary(question=question, context=context)

    def _build_prompt(self, *, question: str, context: Dict[str, Any]) -> str:
        """Create a prompt that guides the LLM towards actionable narratives."""

        logs_section = "\n".join(
            f"- [{entry.get('level', 'INFO')}] {entry.get('message', '')}"
            for entry in context.get("logs", [])
        ) or "- No material operational alerts in the sampling window."

        risk_section = "\n".join(
            f"- {event.get('type', 'risk_event')}: {event.get('detail', event)}"
            for event in context.get("risk_events", [])
        ) or "- No new risk limit breaches recorded."

        anomaly_section = "\n".join(
            f"- {anom.get('signal', anom.get('id', 'anomaly'))}: {anom.get('description', anom)}"
            for anom in context.get("anomalies", [])
        ) or "- No anomalies flagged by monitoring systems."

        pnl_section = "\n".join(
            f"- {row.get('date', row.get('as_of', 'recent'))}: net={row.get('net_pnl', row)}"
            for row in context.get("pnl", [])
        ) or "- No recent PnL snapshots available."

        return (
            "You are an operations advisor for an algorithmic trading desk. "
            "Explain root causes for performance changes and operational risk. "
            "Cite volatility, trade behaviour, and risk limit breaches when relevant.\n\n"
            f"Question: {question}\n\n"
            "Recent logs:\n"
            f"{logs_section}\n\n"
            "Risk events:\n"
            f"{risk_section}\n\n"
            "Anomalies:\n"
            f"{anomaly_section}\n\n"
            "PnL data:\n"
            f"{pnl_section}\n\n"
            "Respond with a concise narrative (2-3 paragraphs) followed by three bullet "
            "action items for the risk team."
        )

    async def _summarize_openai(self, prompt: str) -> str:
        headers = {
            "Authorization": f"Bearer {self.openai_key}",
        }
        payload = {
            "model": self.openai_model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a senior trading operations analyst.",
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            data = response.json()

        try:
            return data["choices"][0]["message"]["content"].strip()
        except (KeyError, IndexError) as exc:  # pragma: no cover - provider contract
            raise RuntimeError("Unexpected OpenAI response structure") from exc

    async def _summarize_anthropic(self, prompt: str) -> str:
        headers = {
            "x-api-key": self.anthropic_key,
            "anthropic-version": "2023-06-01",
        }
        payload = {
            "model": self.anthropic_model,
            "max_tokens": 800,
            "temperature": 0.2,
            "messages": [{"role": "user", "content": prompt}],
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            data = response.json()

        try:
            return data["content"][0]["text"].strip()
        except (KeyError, IndexError) as exc:  # pragma: no cover - provider contract
            raise RuntimeError("Unexpected Anthropic response structure") from exc

    def _fallback_summary(self, *, question: str, context: Dict[str, Any]) -> str:
        """Produce a deterministic summary when no LLM provider is configured."""

        pnl = context.get("pnl") or []
        if pnl:
            latest = pnl[-1]
            pnl_statement = (
                f"Latest net PnL: {latest.get('net_pnl', 'unknown')} on "
                f"{latest.get('date', latest.get('as_of', 'recent day'))}."
            )
        else:
            pnl_statement = "No PnL data available from the lookback window."

        risk_events = context.get("risk_events") or []
        if risk_events:
            risk_statement = (
                f"{len(risk_events)} risk limit breach(es) observed, including "
                f"{risk_events[-1].get('type', 'unknown breach')}."
            )
        else:
            risk_statement = "Risk systems did not record new limit breaches."

        anomaly_statement = (
            f"Monitoring surfaced {len(context.get('anomalies') or [])} anomaly signals."
        )

        log_statement = (
            f"Reviewed {len(context.get('logs') or [])} operational log lines for corroboration."
        )

        return (
            f"Question: {question}\n"
            f"{pnl_statement} {risk_statement} {anomaly_statement} {log_statement}\n"
            "Action items:\n"
            "- Reconcile trade fills versus market data to ensure losses are understood.\n"
            "- Verify hedges and limits remain aligned with stated mandates.\n"
            "- Schedule a post-mortem with quant and risk leads." 
        )


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class AdvisorQueryRequest(BaseModel):
    user_id: str = Field(..., description="Unique identifier for the requesting user")
    question: str = Field(..., description="Question posed to the advisor service")

    @validator("question")
    def _validate_question(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("question must be non-empty")
        return cleaned


class AdvisorQueryResponse(BaseModel):
    answer: str
    context: Dict[str, Any]


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


app = FastAPI(title="Advisor Service", version="1.0.0")

SESSION_STORE: SessionStoreProtocol | None = None


def _configure_session_store(application: FastAPI) -> SessionStoreProtocol:
    """Attach the shared session store so authentication can validate tokens."""

    existing = getattr(application.state, "session_store", None)
    if isinstance(existing, SessionStoreProtocol):
        store = existing
    else:
        redis_url = os.getenv("SESSION_REDIS_URL")
        if not redis_url:
            raise RuntimeError(
                "SESSION_REDIS_URL is not configured. Provide a shared session store DSN to enable advisor authentication.",
            )

        redis_url = redis_url.strip()
        if not redis_url:
            raise RuntimeError(
                "SESSION_REDIS_URL is not configured. Provide a shared session store DSN to enable advisor authentication."
            )

        ttl_minutes = load_session_ttl_minutes()
        if redis_url.lower().startswith("memory://"):
            store = InMemorySessionStore(ttl_minutes=ttl_minutes)
        else:
            store = build_session_store_from_url(redis_url, ttl_minutes=ttl_minutes)

        application.state.session_store = store

    security.set_default_session_store(store)
    return store


async def _gather_context() -> Dict[str, Any]:
    """Collect data required to answer advisor queries."""

    logs, risk_events, anomalies, pnl = await asyncio.gather(
        asyncio.to_thread(fetch_recent_logs),
        asyncio.to_thread(fetch_recent_risk_events),
        asyncio.to_thread(fetch_recent_anomalies),
        asyncio.to_thread(fetch_recent_pnl),
    )

    return {
        "logs": logs,
        "risk_events": risk_events,
        "anomalies": anomalies,
        "pnl": pnl,
    }


@app.post("/advisor/query", response_model=AdvisorQueryResponse, status_code=status.HTTP_200_OK)
async def advisor_query(
    payload: AdvisorQueryRequest,
    db: Session = Depends(get_db),
    caller_account: str = Depends(require_admin_account),
) -> AdvisorQueryResponse:
    """Respond to advisor queries with contextualised root cause analysis."""

    normalized_user = payload.user_id.strip().lower()
    if normalized_user != caller_account.strip().lower():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authenticated account is not authorized for the requested user.",
        )

    context = await _gather_context()

    summarizer = AdvisorSummarizer()
    answer = await summarizer.summarize(payload.question, context)

    record = AdvisorQuery(
        user_id=payload.user_id,
        question=payload.question,
        answer=answer,
        context=context,
    )

    try:
        db.add(record)
        db.commit()
    except SQLAlchemyError as exc:
        db.rollback()
        LOGGER.exception("Failed to persist advisor query")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to persist advisor query",
        ) from exc

    return AdvisorQueryResponse(answer=answer, context=context)


@app.on_event("startup")
async def _on_startup() -> None:
    """Configure shared dependencies once the application starts."""

    global SESSION_STORE

    SESSION_STORE = _configure_session_store(app)
    _init_database(app)


__all__ = [
    "app",
    "AdvisorSummarizer",
    "AdvisorQueryRequest",
    "AdvisorQueryResponse",
    "fetch_recent_logs",
    "fetch_recent_risk_events",
    "fetch_recent_anomalies",
    "fetch_recent_pnl",
]

