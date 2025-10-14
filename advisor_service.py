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
import re
import sys
import threading
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace, TracebackType
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, TypeVar, cast

import httpx
try:  # pragma: no cover - prefer real FastAPI when available
    from fastapi import Depends, FastAPI, HTTPException, Request, status
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field, validator

_SQLALCHEMY_AVAILABLE = True

try:  # pragma: no cover - exercised when SQLAlchemy is installed
    from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import Session, sessionmaker
    from sqlalchemy.pool import StaticPool
except Exception:  # pragma: no cover - executed in lightweight environments
    _SQLALCHEMY_AVAILABLE = False
    JSON = Column = DateTime = Integer = String = text = None  # type: ignore[assignment]
    SQLAlchemyError = RuntimeError  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]
    Session = Any  # type: ignore[assignment]
    StaticPool = type("StaticPool", (), {})  # type: ignore[assignment]

    def create_engine(*_: Any, **__: Any) -> Any:  # type: ignore[override]
        raise RuntimeError("SQLAlchemy is not available in this environment")

    def declarative_base(*_: Any, **__: Any) -> Any:  # type: ignore[override]
        return SimpleNamespace(metadata=SimpleNamespace(create_all=lambda **__: None, drop_all=lambda **__: None))

    def sessionmaker(*_: Any, **__: Any) -> Callable[[], Any]:  # type: ignore[override]
        raise RuntimeError("SQLAlchemy sessionmaker is unavailable")

_T = TypeVar("_T")

from auth.service import (
    InMemorySessionStore,
    SessionStoreProtocol,
    build_session_store_from_url,
)
from services.common import security
from services.common.security import require_admin_account
from shared.postgres import normalize_sqlalchemy_dsn
from shared.session_config import load_session_ttl_minutes
from shared import readiness
from shared.readiness import ProbeFailure, ProviderNotConfigured
from shared.readyz_router import ReadyzRouter

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


class _InMemoryAdvisorStore:
    """Minimal persistence layer used when SQLAlchemy is unavailable."""

    def __init__(self, key: str) -> None:
        self._key = key
        self._lock = threading.RLock()
        self._records: List[AdvisorQuery] = []
        self._next_id = 1
        self._path = _state_directory() / f"{key}.json"
        self._load()

    def _load(self) -> None:
        try:
            if not self._path.exists():
                return
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            LOGGER.warning(
                "Failed to load advisor fallback state from %s", self._path, exc_info=True
            )
            return

        records: List[AdvisorQuery] = []
        for entry in raw.get("records", []):
            try:
                created_at = entry.get("created_at")
                ts = (
                    datetime.fromisoformat(created_at)
                    if isinstance(created_at, str)
                    else datetime.now(UTC)
                )
                context = entry.get("context", {})
                if isinstance(context, Mapping):
                    context = dict(context)
                else:
                    context = {}
                records.append(
                    AdvisorQuery(
                        user_id=str(entry.get("user_id", "")),
                        question=str(entry.get("question", "")),
                        answer=str(entry.get("answer", "")),
                        context=context,
                        created_at=ts,
                        id=entry.get("id"),
                    )
                )
            except Exception:
                LOGGER.debug(
                    "Failed to deserialize advisor record from fallback state", exc_info=True
                )
        if records:
            self._records = records
            self._next_id = int(raw.get("next_id", len(records) + 1))

    def _persist(self) -> None:
        payload = {
            "next_id": self._next_id,
            "records": [
                {
                    "id": record.id,
                    "user_id": record.user_id,
                    "question": record.question,
                    "answer": record.answer,
                    "context": dict(record.context),
                    "created_at": record.created_at.isoformat(),
                }
                for record in self._records
            ],
        }
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            LOGGER.warning(
                "Failed to persist advisor fallback state to %s", self._path, exc_info=True
            )

    def reset(self) -> None:
        with self._lock:
            self._records.clear()
            self._next_id = 1
            try:
                if self._path.exists():
                    self._path.unlink()
            except Exception:
                LOGGER.debug(
                    "Failed to remove advisor fallback state at %s", self._path, exc_info=True
                )

    def add(self, record: "AdvisorQuery") -> None:
        with self._lock:
            if getattr(record, "id", None) in (None, 0):
                record.id = self._next_id  # type: ignore[attr-defined]
                self._next_id += 1
            self._records.append(record)
            self._persist()

    def all(self) -> List["AdvisorQuery"]:
        with self._lock:
            return [replace(record) for record in self._records]


class _InMemoryQueryResult:
    def __init__(self, rows: List["AdvisorQuery"]) -> None:
        self._rows = rows

    def all(self) -> List["AdvisorQuery"]:
        return list(self._rows)


class _InMemorySession:
    def __init__(self, store: _InMemoryAdvisorStore) -> None:
        self._store = store
        self._pending: List[AdvisorQuery] = []

    def add(self, record: "AdvisorQuery") -> None:
        self._pending.append(record)

    def commit(self) -> None:
        for record in self._pending:
            self._store.add(record)
        self._pending.clear()

    def rollback(self) -> None:
        self._pending.clear()

    def close(self) -> None:  # pragma: no cover - API parity
        self._pending.clear()

    def query(self, model: Any) -> _InMemoryQueryResult:
        if model is AdvisorQuery:
            return _InMemoryQueryResult(self._store.all())
        return _InMemoryQueryResult([])

    def __enter__(self) -> "_InMemorySession":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.close()
        return False


class _InMemoryEngine:
    def __init__(self, url: str) -> None:
        self.url = url
        self.store = _get_in_memory_store(url)

    def dispose(self) -> None:  # pragma: no cover - API parity
        return None

    def reset(self) -> None:
        self.store.reset()

    def ensure_ready(self) -> None:  # pragma: no cover - API parity
        return None


def _in_memory_sessionmaker(store: _InMemoryAdvisorStore) -> Callable[[], _InMemorySession]:
    def factory() -> _InMemorySession:
        return _InMemorySession(store)

    return factory


_IN_MEMORY_STORE_MODULE = "_advisor_service_store"
_store_module = cast(ModuleType, sys.modules.get(_IN_MEMORY_STORE_MODULE))
if _store_module is None:
    _store_module = ModuleType(_IN_MEMORY_STORE_MODULE)
    _store_module.stores = {}  # type: ignore[attr-defined]
    sys.modules[_IN_MEMORY_STORE_MODULE] = _store_module

_IN_MEMORY_STORES = cast(Dict[str, _InMemoryAdvisorStore], getattr(_store_module, "stores"))


def _normalize_store_key(url: str) -> str:
    normalized = url.strip().lower()
    return re.sub(r"[^a-z0-9._-]", "_", normalized) or "advisor"


def _state_directory() -> Path:
    root = Path(os.getenv("AETHER_STATE_DIR", ".aether_state"))
    return root / "advisor"


def _get_in_memory_store(url: str) -> _InMemoryAdvisorStore:
    normalized = _normalize_store_key(url)
    store = _IN_MEMORY_STORES.get(normalized)
    if store is None:
        store = _InMemoryAdvisorStore(normalized)
        _IN_MEMORY_STORES[normalized] = store
    return store


class _InMemoryMetadata:
    def create_all(self, **kwargs: Any) -> None:
        engine = kwargs.get("bind")
        if hasattr(engine, "ensure_ready"):
            engine.ensure_ready()

    def drop_all(self, **kwargs: Any) -> None:
        engine = kwargs.get("bind")
        if hasattr(engine, "reset"):
            engine.reset()


Base = declarative_base() if _SQLALCHEMY_AVAILABLE else SimpleNamespace(metadata=_InMemoryMetadata())


if _SQLALCHEMY_AVAILABLE:

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

else:

    @dataclass
    class AdvisorQuery:
        """Simple record persisted via the in-memory advisor store."""

        user_id: str
        question: str
        answer: str
        context: Dict[str, Any] = field(default_factory=dict)
        created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
        id: int | None = None


ENGINE: Engine | _InMemoryEngine | None = None
SessionLocal: Callable[[], Session] | Callable[[], _InMemorySession] | None = None


def _create_engine(database_url: str) -> Engine | _InMemoryEngine:
    """Create the backing persistence engine for advisor history."""

    if not _SQLALCHEMY_AVAILABLE:
        LOGGER.warning(
            "SQLAlchemy is unavailable; using in-memory advisor persistence for %s",
            database_url,
        )
        return _InMemoryEngine(database_url)

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


def _initialise_session_factory(engine: Engine | _InMemoryEngine) -> Callable[[], Any]:
    if _SQLALCHEMY_AVAILABLE:
        return sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)

    assert isinstance(engine, _InMemoryEngine)
    return _in_memory_sessionmaker(engine.store)


def _init_database(application: FastAPI) -> None:
    """Initialise the shared database engine and session factory."""

    global ENGINE, SessionLocal

    if getattr(application.state, "db_engine", None) is not None:
        return

    if ENGINE is None or SessionLocal is None:
        database_url = _require_database_url()
        engine = _create_engine(database_url)
        SessionLocal = _initialise_session_factory(engine)
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
# Readiness probes
# ---------------------------------------------------------------------------


async def _run_in_thread(func: Callable[[], _T]) -> _T:
    """Execute blocking *func* in a worker thread."""

    try:
        to_thread = asyncio.to_thread
    except AttributeError:  # pragma: no cover - fallback for Python < 3.9
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func)
    return await to_thread(func)


class _SQLAlchemyProbeClient:
    """Adapter exposing async helpers for the readiness probes."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    async def fetchval(self, query: str, *args: Any, timeout: float | None = None) -> Any:
        def _execute() -> Any:
            session = self._session_factory()
            try:
                result = session.execute(text(query))
                return result.scalar()
            finally:
                session.close()

        return await _run_in_thread(_execute)

    async def execute(self, query: str, *args: Any, timeout: float | None = None) -> Any:
        def _execute() -> Any:
            session = self._session_factory()
            try:
                session.execute(text(query))
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        return await _run_in_thread(_execute)


class _InMemoryProbeClient:
    """Minimal adapter used when SQLAlchemy is unavailable."""

    def __init__(self, session_factory: Callable[[], _InMemorySession]) -> None:
        self._session_factory = session_factory

    async def fetchval(self, query: str, *args: Any, timeout: float | None = None) -> int:
        session = self._session_factory()
        try:
            session.query(AdvisorQuery)
        finally:
            session.close()
        return 1

    async def execute(self, query: str, *args: Any, timeout: float | None = None) -> None:
        session = self._session_factory()
        try:
            session.query(AdvisorQuery)
        finally:
            session.close()


def _get_session_factory(application: FastAPI) -> Callable[[], Any]:
    session_factory = getattr(application.state, "db_session_factory", None)
    if session_factory is None:
        raise ProviderNotConfigured("Advisor database")
    return session_factory


def _using_sqlite(application: FastAPI) -> bool:
    engine = getattr(application.state, "db_engine", None)
    if not _SQLALCHEMY_AVAILABLE:
        return isinstance(engine, _InMemoryEngine)
    if engine is None:
        return False
    backend = getattr(getattr(engine, "dialect", None), "name", "")
    return str(backend).lower() == "sqlite"


def _build_postgres_probe_client(application: FastAPI) -> Any:
    session_factory = _get_session_factory(application)
    if _SQLALCHEMY_AVAILABLE:
        return _SQLAlchemyProbeClient(session_factory)  # type: ignore[arg-type]
    return _InMemoryProbeClient(session_factory)  # type: ignore[arg-type]


async def _postgres_read_probe() -> None:
    client = _build_postgres_probe_client(app)
    await readiness.postgres_read_probe(client=client)


async def _postgres_write_probe() -> None:
    client = _build_postgres_probe_client(app)
    read_only_query = "SELECT 1" if _using_sqlite(app) else "SELECT pg_is_in_recovery()"
    await readiness.postgres_write_probe(client=client, read_only_query=read_only_query)


async def _session_store_probe() -> None:
    store = getattr(app.state, "session_store", None)
    if store is None:
        raise ProviderNotConfigured("Session store")

    redis_client = getattr(store, "_redis", None)
    if redis_client is not None:
        await readiness.redis_ping_probe(client=redis_client)
        return

    getter = getattr(store, "get", None)
    if not callable(getter):
        raise ProbeFailure("Session store", "configured store does not expose a get method")

    try:
        getter("_advisor_readyz_probe_")
    except Exception as exc:  # pragma: no cover - defensive surface
        raise ProbeFailure("Session store", f"probe lookup failed with {exc!r}") from exc


_readyz_router = ReadyzRouter()
_readyz_router.register_probe("postgres_read", _postgres_read_probe)
_readyz_router.register_probe("postgres_write", _postgres_write_probe)
_readyz_router.register_probe("session_store", _session_store_probe)


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
app.include_router(_readyz_router.router)

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
    "AdvisorQuery",
    "AdvisorQueryRequest",
    "AdvisorQueryResponse",
    "fetch_recent_logs",
    "fetch_recent_risk_events",
    "fetch_recent_anomalies",
    "fetch_recent_pnl",
]

