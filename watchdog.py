"""Oversight watchdog that vetoes anomalous policy decisions.

This service listens for published :class:`IntentEvent` payloads and the
corresponding policy decision events.  When both sides of the trade context are
available the watchdog evaluates the trade with a LightGBM-based anomaly model
as well as deterministic heuristics targeted at obvious execution mistakes
(e.g. selling through the bid).  Trades that look irrational are vetoed and an
override message is dispatched so the sequencer can halt the order flow.  Every
veto is persisted for later inspection and surfaced through ``/oversight/status``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections import defaultdict
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

from fastapi import Depends, FastAPI, Query
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    create_engine,
    func,
    select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from common.schemas.contracts import IntentEvent
from services.common.security import require_admin_account
from shared.account_scope import account_id_column
from shared.dependency_alerts import notify_dependency_fallback
from shared.runtime_checks import ensure_insecure_default_flag_disabled
from shared.spot import is_spot_symbol, normalize_spot_symbol
from shared.event_bus import KafkaNATSAdapter

try:  # pragma: no cover - LightGBM is optional in many environments
    import lightgbm as lgb  # type: ignore
except Exception:  # pragma: no cover - degrade gracefully when LightGBM missing
    lgb = None  # type: ignore


LOGGER = logging.getLogger("watchdog")


DATABASE_URL_ENV_VAR = "WATCHDOG_DATABASE_URL"
_INSECURE_DEFAULTS_FLAG = "WATCHDOG_ALLOW_INSECURE_DEFAULTS"
_STATE_DIR_ENV = "WATCHDOG_STATE_DIR"
_DEFAULT_STATE_DIR = Path(".aether_state/watchdog")

ensure_insecure_default_flag_disabled(_INSECURE_DEFAULTS_FLAG)

def _insecure_defaults_enabled() -> bool:
    flag = os.getenv(_INSECURE_DEFAULTS_FLAG)
    if flag == "1":
        ensure_insecure_default_flag_disabled(_INSECURE_DEFAULTS_FLAG)
        return True
    if flag == "0":
        return False
    return "pytest" in sys.modules


def _state_dir() -> Path:
    root = Path(os.getenv(_STATE_DIR_ENV, _DEFAULT_STATE_DIR))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _sqlite_fallback_url() -> str:
    path = _state_dir() / "watchdog.sqlite"
    return f"sqlite:///{path}"


def _database_url() -> str:
    url = os.getenv(DATABASE_URL_ENV_VAR) or os.getenv("TIMESCALE_DSN")
    allow_insecure = _insecure_defaults_enabled()
    if not url:
        if allow_insecure:
            fallback = _sqlite_fallback_url()
            LOGGER.warning(
                "No WATCHDOG_DATABASE_URL configured; falling back to local SQLite store at %s",
                fallback,
            )
            return fallback
        raise RuntimeError(
            "WATCHDOG_DATABASE_URL or TIMESCALE_DSN must be set to a PostgreSQL/Timescale DSN"
        )

    normalized = url.strip()
    lowered = normalized.lower()

    if lowered.startswith("postgresql://"):
        normalized = normalized.replace("postgresql://", "postgresql+psycopg://", 1)
        lowered = normalized.lower()

    if lowered.startswith(("postgresql+psycopg://", "postgresql+psycopg2://")):
        return normalized

    if lowered.startswith("sqlite"):
        if allow_insecure:
            LOGGER.warning(
                "Permitting SQLite watchdog DSN '%s' due to %s=1", normalized, _INSECURE_DEFAULTS_FLAG
            )
            return normalized
        raise RuntimeError(
            f"SQLite watchdog DSNs require {_INSECURE_DEFAULTS_FLAG}=1 to be set explicitly"
        )

    if lowered.startswith("memory://") and allow_insecure:
        return normalized

    raise RuntimeError(
        "Watchdog requires a PostgreSQL/Timescale DSN via WATCHDOG_DATABASE_URL or TIMESCALE_DSN"
    )


# Lazily initialized database engine/session
ENGINE: Optional[Engine] = None
SessionLocal: Optional[sessionmaker] = None
Base = declarative_base()


class WatchdogLog(Base):
    """SQLAlchemy ORM table capturing trades vetoed by the watchdog."""

    __tablename__ = "watchdog_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    intent_id = Column(String, nullable=False, unique=True, index=True)
    account_id = account_id_column(index=True)
    reason = Column(Text, nullable=False)
    score = Column(Float, nullable=True)
    details_json = Column(JSON, nullable=False, default=dict)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), index=True)


def _initialise_database(app: FastAPI) -> WatchdogRepository:
    """Initialise the database engine and session factory at startup."""

    global ENGINE, SessionLocal

    if ENGINE is None or SessionLocal is None:
        database_url = _database_url()
        engine_options: Dict[str, Any] = {"future": True, "pool_pre_ping": True}
        if database_url.startswith("sqlite://"):
            engine_options.setdefault("connect_args", {"check_same_thread": False})
            if database_url.endswith(":memory:"):
                engine_options["poolclass"] = StaticPool

        try:
            ENGINE = create_engine(database_url, **engine_options)
        except ModuleNotFoundError as exc:
            if _insecure_defaults_enabled() and "psycopg" in repr(exc):
                fallback_url = _sqlite_fallback_url()
                LOGGER.warning(
                    "psycopg unavailable; watchdog falling back to SQLite store at %s", fallback_url
                )
                notify_dependency_fallback(
                    component="watchdog-service",
                    dependency="psycopg",
                    fallback="sqlite",
                    reason="psycopg import failed during watchdog database initialisation",
                    metadata={"module": __name__, "fallback_url": fallback_url},
                )
                fallback_options: Dict[str, Any] = {
                    "future": True,
                    "pool_pre_ping": True,
                    "connect_args": {"check_same_thread": False},
                }
                ENGINE = create_engine(fallback_url, **fallback_options)
            else:
                raise
        SessionLocal = sessionmaker(
            bind=ENGINE, autoflush=False, expire_on_commit=False, future=True
        )
        Base.metadata.create_all(bind=ENGINE)

    if ENGINE is None or SessionLocal is None:  # pragma: no cover - defensive
        raise RuntimeError("Watchdog database session not initialised")

    session_factory = SessionLocal
    app.state.watchdog_engine = ENGINE
    app.state.watchdog_session_factory = session_factory
    repository = WatchdogRepository(session_factory)
    app.state.watchdog_repository = repository
    return repository


# ---------------------------------------------------------------------------
# Lightweight protocol objects
# ---------------------------------------------------------------------------


class PolicyDecisionEvent(BaseModel):
    """Subset of the policy decision event published to Kafka/NATS."""

    order_id: Optional[str] = Field(None, description="Identifier linking to the originating intent")
    instrument: Optional[str] = Field(None, description="Instrument evaluated by policy")
    approved: bool = Field(..., description="Whether the policy approved the trade")
    reason: Optional[str] = Field(None, description="Optional policy rejection reason")
    edge_bps: Optional[float] = Field(None, description="Model expected edge in basis points")
    fee_adjusted_edge_bps: Optional[float] = Field(
        None, alias="fee_adjusted_edge_bps", description="Edge after fees in basis points"
    )
    confidence: Dict[str, Any] = Field(default_factory=dict, description="Confidence metrics payload")
    selected_action: Optional[str] = Field(None, description="Chosen execution template")
    action_templates: List[Dict[str, Any]] = Field(
        default_factory=list, description="All evaluated execution templates"
    )

    model_config = {"populate_by_name": True}


@dataclass(slots=True)
class IntentEnvelope:
    """Normalized view of an intent event."""

    account_id: str
    intent_id: str
    symbol: Optional[str]
    side: Optional[str]
    price: Optional[float]
    quantity: Optional[float]
    book_snapshot: Optional[Mapping[str, Any]]
    spread_bps: Optional[float]
    recorded_at: datetime
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PolicyDecisionEnvelope:
    """Normalized view of a policy decision event."""

    account_id: str
    intent_id: str
    instrument: Optional[str]
    approved: bool
    reason: Optional[str]
    edge_bps: Optional[float]
    fee_adjusted_edge_bps: Optional[float]
    confidence_overall: Optional[float]
    selected_action: Optional[str]
    action_templates: List[Mapping[str, Any]]
    recorded_at: datetime
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DetectionOutcome:
    """Result produced by the anomaly detector when vetoing a trade."""

    score: float
    reason: str
    details: Dict[str, Any]


@dataclass(slots=True)
class WatchdogVeto:
    """Persistence payload for watchdog vetoes."""

    intent_id: str
    account_id: str
    reason: str
    score: float
    details: Dict[str, Any]
    ts: datetime


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_account(value: str) -> str:
    normalized = str(value or "unknown").strip().lower()
    return normalized or "unknown"


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_present(payload: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return None


def _resolve_intent_id(payload: Mapping[str, Any]) -> Optional[str]:
    for key in ("intent_id", "order_id", "client_order_id", "orderId", "id"):
        value = payload.get(key)
        if isinstance(value, (str, int)):
            text = str(value).strip()
            if text:
                return text
    return None


def _resolve_side(payload: Mapping[str, Any]) -> Optional[str]:
    for key in ("side", "action", "direction", "order_side"):
        value = payload.get(key)
        if not isinstance(value, str):
            continue
        normalized = value.strip().lower()
        if normalized in {"buy", "sell"}:
            return normalized
        if normalized in {"long", "short"}:
            return "buy" if normalized == "long" else "sell"
    return None


def _extract_book(payload: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    candidate = payload.get("book_snapshot") or payload.get("book") or payload.get("market_state")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    return None


def _resolve_confidence(payload: Mapping[str, Any]) -> Optional[float]:
    if not isinstance(payload, Mapping):
        return None
    for key in ("overall_confidence", "overall", "score"):
        value = payload.get(key)
        confidence = _to_float(value)
        if confidence is not None:
            return max(0.0, min(1.0, confidence))
    model_conf = _to_float(payload.get("model_confidence"))
    state_conf = _to_float(payload.get("state_confidence"))
    exec_conf = _to_float(payload.get("execution_confidence"))
    values = [metric for metric in (model_conf, state_conf, exec_conf) if metric is not None]
    if values:
        return max(0.0, min(1.0, sum(values) / len(values)))
    return None


def _book_prices(book: Optional[Mapping[str, Any]]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if not isinstance(book, Mapping):
        return None, None, None
    mid = _to_float(book.get("mid_price") or book.get("mid"))
    spread_bps = _to_float(book.get("spread_bps") or book.get("spread"))
    if mid is None or spread_bps is None:
        return mid, None, None
    spread = mid * spread_bps / 10000.0
    bid = mid - spread / 2.0
    ask = mid + spread / 2.0
    return mid, bid, ask


# ---------------------------------------------------------------------------
# LightGBM scoring helpers
# ---------------------------------------------------------------------------


class LightGBMScorer:
    """Wrapper around an optional LightGBM booster used for anomaly scoring."""

    def __init__(self, model_path: Optional[str] = None) -> None:
        self._model_path = model_path or os.getenv("WATCHDOG_MODEL_PATH")
        self._booster: Any = None

        if lgb is None:
            LOGGER.warning(
                "LightGBM is not available; watchdog will rely on heuristic anomaly detection."
            )
            return

        if not self._model_path:
            LOGGER.info(
                "No LightGBM watchdog model configured; heuristics remain active without model guidance."
            )
            return

        try:  # pragma: no cover - dependent on external model availability
            self._booster = lgb.Booster(model_file=self._model_path)
            LOGGER.info("Loaded watchdog LightGBM model from %s", self._model_path)
        except Exception:  # pragma: no cover - provide graceful fallback
            LOGGER.exception("Failed to load LightGBM model from %s", self._model_path)
            self._booster = None

    def score(self, features: List[float]) -> float:
        if self._booster is None:
            return 0.0

        try:  # pragma: no cover - optional dependency in tests
            import numpy as np  # type: ignore
        except Exception:
            LOGGER.warning("NumPy unavailable; skipping LightGBM scoring")
            return 0.0

        try:
            data = np.asarray([features], dtype=float)
            prediction = self._booster.predict(data)
        except Exception:  # pragma: no cover - prediction failures should not crash the watchdog
            LOGGER.exception("LightGBM prediction failed; falling back to heuristics")
            return 0.0

        if isinstance(prediction, (list, tuple)):
            return float(prediction[0])
        try:
            return float(prediction)
        except (TypeError, ValueError):
            return 0.0


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------


class IrrationalTradeDetector:
    """Combines heuristic checks with LightGBM scoring to veto irrational trades."""

    def __init__(self, *, threshold: float = 0.7, model: Optional[LightGBMScorer] = None) -> None:
        self._threshold = threshold
        self._model = model or LightGBMScorer()

    def evaluate(
        self, intent: IntentEnvelope, decision: PolicyDecisionEnvelope
    ) -> Optional[DetectionOutcome]:
        heuristic_score, heuristic_reasons, details = self._heuristics(intent, decision)

        slippage_bps = details.get("slippage_bps")
        features = self._build_features(intent, decision, slippage_bps)
        model_score = self._model.score(features)

        details.update(
            {
                "model_score": model_score,
                "heuristic_score": heuristic_score,
                "features": features,
            }
        )

        score = max(model_score, heuristic_score)
        score = max(0.0, min(1.0, score))

        if score < self._threshold:
            return None

        if heuristic_reasons:
            reason = "; ".join(heuristic_reasons)
        else:
            reason = f"LightGBM anomaly score {score:.2f} exceeded threshold"

        return DetectionOutcome(score=score, reason=reason, details=details)

    def _build_features(
        self,
        intent: IntentEnvelope,
        decision: PolicyDecisionEnvelope,
        slippage_bps: Optional[float],
    ) -> List[float]:
        return [
            float(decision.fee_adjusted_edge_bps or 0.0),
            float(decision.edge_bps or 0.0),
            float(decision.confidence_overall or 0.0),
            float(slippage_bps or 0.0),
            1.0 if (intent.side or "").lower() == "sell" else 0.0,
            1.0 if decision.approved else 0.0,
        ]

    def _heuristics(
        self, intent: IntentEnvelope, decision: PolicyDecisionEnvelope
    ) -> Tuple[float, List[str], Dict[str, Any]]:
        reasons: List[str] = []
        details: Dict[str, Any] = {
            "fee_adjusted_edge_bps": decision.fee_adjusted_edge_bps,
            "edge_bps": decision.edge_bps,
            "overall_confidence": decision.confidence_overall,
            "spread_bps": intent.spread_bps,
            "side": intent.side,
        }

        heuristic_score = 0.0

        fee_edge = decision.fee_adjusted_edge_bps
        if decision.approved and fee_edge is not None and fee_edge < 0.0:
            heuristic_score = max(heuristic_score, 0.75)
            reasons.append(
                f"Fee-adjusted edge {fee_edge:.2f}bps is negative despite policy approval"
            )

        overall_conf = decision.confidence_overall
        if decision.approved and overall_conf is not None and overall_conf < 0.15:
            heuristic_score = max(heuristic_score, 0.6)
            reasons.append(
                f"Overall confidence {overall_conf:.2f} below watchdog floor of 0.15"
            )

        mid, bid, ask = _book_prices(intent.book_snapshot)
        details.update({"mid_price": mid, "bid_price": bid, "ask_price": ask})

        slippage_bps: Optional[float] = None
        spread_bps = intent.spread_bps
        if spread_bps is None and intent.book_snapshot is not None:
            spread_bps = _to_float(intent.book_snapshot.get("spread_bps"))

        if intent.side == "sell" and bid is not None and intent.price is not None and mid is not None:
            diff = bid - intent.price
            if diff > 0:
                slippage_bps = diff / mid * 10000.0
        elif intent.side == "buy" and ask is not None and intent.price is not None and mid is not None:
            diff = intent.price - ask
            if diff > 0:
                slippage_bps = diff / mid * 10000.0

        details["slippage_bps"] = slippage_bps

        if slippage_bps is not None and (intent.side in {"buy", "sell"}):
            limit = max(5.0, (spread_bps or 0.0) * 1.5)
            if slippage_bps > limit:
                heuristic_score = max(heuristic_score, min(1.0, 0.8 + (slippage_bps - limit) / 100.0))
                if intent.side == "sell":
                    reasons.append(
                        f"Sell price {intent.price:.6f} is {slippage_bps:.2f}bps through bid (limit {limit:.2f}bps)"
                    )
                else:
                    reasons.append(
                        f"Buy price {intent.price:.6f} is {slippage_bps:.2f}bps above ask (limit {limit:.2f}bps)"
                    )

        return heuristic_score, reasons, details


# ---------------------------------------------------------------------------
# Event subscription plumbing
# ---------------------------------------------------------------------------


class EventStream:
    """Iterates through the in-memory Kafka/NATS adapter history."""

    def __init__(self) -> None:
        self._cursor: Dict[str, int] = defaultdict(int)

    def poll(self) -> List[Tuple[str, Dict[str, Any]]]:
        records: List[Tuple[str, Dict[str, Any]]] = []
        store: Mapping[str, List[Dict[str, Any]]] = getattr(KafkaNATSAdapter, "_event_store", {})
        for account_id, events in list(store.items()):
            start = self._cursor.get(account_id, 0)
            if start >= len(events):
                continue
            new_events = events[start:]
            self._cursor[account_id] = len(events)
            for record in new_events:
                records.append((account_id, deepcopy(record)))
        return records


# ---------------------------------------------------------------------------
# Persistence layer
# ---------------------------------------------------------------------------


class WatchdogRepository:
    """Encapsulates persistence for watchdog veto events."""

    def __init__(self, session_factory: sessionmaker) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session_scope(self, commit: bool = False) -> Iterator[Session]:
        session: Session = self._session_factory()
        try:
            yield session
            if commit:
                session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def record(self, record: WatchdogVeto) -> bool:
        with self._session_scope(commit=True) as session:
            existing = (
                session.execute(select(WatchdogLog).where(WatchdogLog.intent_id == record.intent_id))
                .scalars()
                .first()
            )
            if existing is not None:
                return False

            entry = WatchdogLog(
                intent_id=record.intent_id,
                account_id=record.account_id,
                reason=record.reason,
                score=record.score,
                details_json=dict(record.details),
                ts=record.ts,
            )

            session.add(entry)
            try:
                session.flush()
            except IntegrityError:
                session.rollback()
                return False

        return True

    def summary(self, limit: int) -> Tuple[int, List[WatchdogLog], List[Tuple[str, int]]]:
        with self._session_scope(commit=False) as session:
            total = session.execute(select(func.count(WatchdogLog.id))).scalar_one() or 0

            stmt = select(WatchdogLog).order_by(WatchdogLog.ts.desc()).limit(limit)
            entries = session.execute(stmt).scalars().all()

            counts_stmt = (
                select(WatchdogLog.reason, func.count(WatchdogLog.id))
                .group_by(WatchdogLog.reason)
                .order_by(func.count(WatchdogLog.id).desc())
            )
            counts = session.execute(counts_stmt).all()

        normalized_counts = [(reason, int(count)) for reason, count in counts]
        return int(total), entries, normalized_counts


# ---------------------------------------------------------------------------
# Coordinator
# ---------------------------------------------------------------------------


class WatchdogCoordinator:
    """Background task that reconciles intent and policy events for oversight."""

    def __init__(
        self,
        *,
        detector: IrrationalTradeDetector,
        repository: WatchdogRepository,
        poll_interval: float = 0.5,
        retention: timedelta = timedelta(minutes=15),
    ) -> None:
        self._detector = detector
        self._repository = repository
        self._poll_interval = poll_interval
        self._retention = retention
        self._stream = EventStream()
        self._intents: Dict[Tuple[str, str], IntentEnvelope] = {}
        self._decisions: Dict[Tuple[str, str], PolicyDecisionEnvelope] = {}
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="watchdog-event-loop")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        await self._task
        self._task = None

    async def _run(self) -> None:
        LOGGER.info("Watchdog coordinator started")
        try:
            while not self._stop_event.is_set():
                await self._drain_events()
                self._cleanup_stale_entries()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval)
                except asyncio.TimeoutError:
                    continue
        finally:
            LOGGER.info("Watchdog coordinator stopped")

    async def _drain_events(self) -> None:
        for account_id, record in self._stream.poll():
            try:
                await self._handle_record(account_id, record)
            except Exception:  # pragma: no cover - defensive logging around event loop
                LOGGER.exception("Failed to process watchdog event for account %s", account_id)

    async def _handle_record(self, account_id: str, record: Mapping[str, Any]) -> None:
        topic = record.get("topic")
        payload = record.get("payload") or {}
        recorded_at = record.get("timestamp")
        if not isinstance(recorded_at, datetime):
            recorded_at = _now()

        if topic == "intent.events":
            envelope = self._parse_intent_event(account_id, payload, recorded_at)
            if envelope is not None:
                await self._register_intent(envelope)
        elif topic == "policy.decisions":
            envelope = self._parse_policy_event(account_id, payload, recorded_at)
            if envelope is not None:
                await self._register_policy(envelope)

    def _parse_intent_event(
        self, account_id: str, payload: Mapping[str, Any], recorded_at: datetime
    ) -> Optional[IntentEnvelope]:
        intent_payload: Mapping[str, Any] = payload
        event_ts = recorded_at
        event_account = account_id

        try:
            event = IntentEvent.model_validate(payload)
        except ValidationError:
            pass
        else:
            intent_payload = event.intent or {}
            event_ts = event.ts
            event_account = event.account_id or account_id

        intent_id = _resolve_intent_id(intent_payload) or _resolve_intent_id(payload)
        if intent_id is None:
            LOGGER.warning("Intent event missing identifier: %s", intent_payload)
            return None

        side = _resolve_side(intent_payload)
        price = _to_float(
            _first_present(intent_payload, ("price", "limit_price", "limit_px", "px"))
        )
        quantity = _to_float(_first_present(intent_payload, ("quantity", "qty", "size")))
        symbol_value = intent_payload.get("instrument") or intent_payload.get("symbol") or payload.get("symbol")
        symbol = str(symbol_value).strip() if isinstance(symbol_value, str) else None
        if symbol:
            normalized_symbol = normalize_spot_symbol(symbol)
            if not is_spot_symbol(normalized_symbol):
                LOGGER.warning(
                    "Ignoring non-spot instrument in intent event",
                    extra={"instrument": symbol, "intent_id": intent_id},
                )
                return None
            symbol = normalized_symbol
        book_snapshot = _extract_book(intent_payload)
        spread_bps = _to_float(intent_payload.get("spread_bps"))
        if spread_bps is None and book_snapshot is not None:
            spread_bps = _to_float(book_snapshot.get("spread_bps"))

        return IntentEnvelope(
            account_id=_normalize_account(event_account),
            intent_id=str(intent_id),
            symbol=symbol,
            side=side,
            price=price,
            quantity=quantity,
            book_snapshot=book_snapshot,
            spread_bps=spread_bps,
            recorded_at=event_ts,
            raw=dict(intent_payload),
        )

    def _parse_policy_event(
        self, account_id: str, payload: Mapping[str, Any], recorded_at: datetime
    ) -> Optional[PolicyDecisionEnvelope]:
        try:
            event = PolicyDecisionEvent.model_validate(payload)
        except ValidationError as exc:
            LOGGER.warning("Failed to parse policy decision payload: %s", exc)
            return None

        intent_id = event.order_id or _resolve_intent_id(payload)
        if intent_id is None:
            LOGGER.warning("Policy decision event missing order identifier: %s", payload)
            return None

        instrument = event.instrument.strip() if isinstance(event.instrument, str) else None
        if instrument:
            normalized_instrument = normalize_spot_symbol(instrument)
            if not is_spot_symbol(normalized_instrument):
                LOGGER.warning(
                    "Ignoring non-spot instrument in policy decision",
                    extra={"instrument": instrument, "intent_id": intent_id},
                )
                return None
            instrument = normalized_instrument

        return PolicyDecisionEnvelope(
            account_id=_normalize_account(account_id),
            intent_id=str(intent_id),
            instrument=instrument,
            approved=bool(event.approved),
            reason=event.reason,
            edge_bps=_to_float(event.edge_bps),
            fee_adjusted_edge_bps=_to_float(event.fee_adjusted_edge_bps),
            confidence_overall=_resolve_confidence(event.confidence),
            selected_action=event.selected_action,
            action_templates=list(event.action_templates or []),
            recorded_at=recorded_at,
            raw=dict(payload),
        )

    async def _register_intent(self, intent: IntentEnvelope) -> None:
        key = (intent.account_id, intent.intent_id)
        self._intents[key] = intent
        decision = self._decisions.pop(key, None)
        if decision is not None:
            await self._evaluate_pair(intent, decision)

    async def _register_policy(self, decision: PolicyDecisionEnvelope) -> None:
        key = (decision.account_id, decision.intent_id)
        intent = self._intents.get(key)
        if intent is None:
            self._decisions[key] = decision
            return
        await self._evaluate_pair(intent, decision)

    async def _evaluate_pair(self, intent: IntentEnvelope, decision: PolicyDecisionEnvelope) -> None:
        key = (intent.account_id, intent.intent_id)
        self._intents.pop(key, None)
        self._decisions.pop(key, None)

        outcome = self._detector.evaluate(intent, decision)
        if outcome is None:
            return

        LOGGER.info(
            "Vetoing intent %s for account %s: %s (score=%.2f)",
            intent.intent_id,
            intent.account_id,
            outcome.reason,
            outcome.score,
        )

        veto = WatchdogVeto(
            intent_id=intent.intent_id,
            account_id=intent.account_id,
            reason=outcome.reason,
            score=outcome.score,
            details={
                **outcome.details,
                "instrument": decision.instrument or intent.symbol,
                "policy_reason": decision.reason,
                "selected_action": decision.selected_action,
            },
            ts=_now(),
        )

        persisted = await asyncio.to_thread(self._repository.record, veto)
        if persisted:
            await asyncio.to_thread(self._publish_override, intent, decision, outcome)

    def _publish_override(
        self,
        intent: IntentEnvelope,
        decision: PolicyDecisionEnvelope,
        outcome: DetectionOutcome,
    ) -> None:
        adapter = KafkaNATSAdapter(account_id=intent.account_id)
        asyncio.run(
            adapter.publish(
                topic="override.queue",
                payload={
                    "intent_id": intent.intent_id,
                    "account_id": intent.account_id,
                    "instrument": decision.instrument or intent.symbol,
                    "decision": "reject",
                    "reason": outcome.reason,
                    "score": outcome.score,
                    "details": outcome.details,
                    "recorded_at": _now().isoformat(),
                    "source": "watchdog",
                },
            )
        )

    def _cleanup_stale_entries(self) -> None:
        expiry = _now() - self._retention
        for mapping in (self._intents, self._decisions):
            for key, envelope in list(mapping.items()):
                recorded_at = getattr(envelope, "recorded_at", None)
                if isinstance(recorded_at, datetime) and recorded_at < expiry:
                    mapping.pop(key, None)


# ---------------------------------------------------------------------------
# API schemas & service wiring
# ---------------------------------------------------------------------------


class ReasonCount(BaseModel):
    reason: str
    count: int


class VetoRecordResponse(BaseModel):
    intent_id: str
    account_id: str
    reason: str
    score: Optional[float] = None
    ts: datetime
    details: Dict[str, Any] = Field(default_factory=dict)


class OversightStatusResponse(BaseModel):
    total_vetoes: int
    reason_counts: List[ReasonCount]
    recent_vetoes: List[VetoRecordResponse]
    last_updated: datetime


app = FastAPI(title="Watchdog Oversight Service", version="1.0.0")


WATCHDOG_DETECTOR = IrrationalTradeDetector()


@app.on_event("startup")
async def startup_event() -> None:
    repository = _initialise_database(app)
    coordinator = WatchdogCoordinator(detector=WATCHDOG_DETECTOR, repository=repository)
    app.state.watchdog_coordinator = coordinator
    await coordinator.start()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    coordinator: Optional[WatchdogCoordinator] = getattr(app.state, "watchdog_coordinator", None)
    if coordinator is not None:
        await coordinator.stop()


def get_repository() -> WatchdogRepository:
    repository: Optional[WatchdogRepository] = getattr(app.state, "watchdog_repository", None)
    if repository is None:
        raise RuntimeError("Watchdog repository is not initialised")
    return repository


@app.get("/oversight/status", response_model=OversightStatusResponse)
async def oversight_status(
    limit: int = Query(20, ge=1, le=200, description="Maximum number of recent vetoes to return"),
    repository: WatchdogRepository = Depends(get_repository),
    _admin_account: str = Depends(require_admin_account),
) -> OversightStatusResponse:
    total, entries, counts = await asyncio.to_thread(repository.summary, limit)

    recent = [
        VetoRecordResponse(
            intent_id=entry.intent_id,
            account_id=entry.account_id,
            reason=entry.reason,
            score=entry.score,
            ts=entry.ts,
            details=dict(entry.details_json or {}),
        )
        for entry in entries
    ]

    reason_counts = [ReasonCount(reason=reason, count=count) for reason, count in counts]

    return OversightStatusResponse(
        total_vetoes=total,
        reason_counts=reason_counts,
        recent_vetoes=recent,
        last_updated=_now(),
    )


__all__ = [
    "app",
    "IrrationalTradeDetector",
    "WatchdogCoordinator",
    "WatchdogRepository",
]

