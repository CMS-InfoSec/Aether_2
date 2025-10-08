from __future__ import annotations

import asyncio
import inspect
import json
import logging
from datetime import datetime, timezone
from enum import Enum
from collections import defaultdict
from types import ModuleType
from typing import (
    Any,
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    cast,
)
from typing_extensions import ParamSpec

from fastapi import APIRouter, FastAPI, HTTPException

try:  # pragma: no cover - optional dependency during documentation builds
    import sqlalchemy as sa
    from sqlalchemy import select as _select
    from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError
    from sqlalchemy.orm import sessionmaker as _sessionmaker
except Exception:  # pragma: no cover - fall back for environments without sqlalchemy
    sa = None
    _select = None

    class _FallbackSQLAlchemyError(Exception):
        """Fallback exception when SQLAlchemy is unavailable."""


SelectCallable = Callable[..., Any]


class EngineProtocol(Protocol):
    """Subset of SQLAlchemy engine behaviour consumed by the store."""

    def connect(self) -> Any:
        """Return a connection handle."""


class MappingResultProtocol(Protocol):
    """Result contract for ``Result.mappings()`` calls."""

    def first(self) -> Optional[Mapping[str, Any]]:
        """Return the first row mapping if present."""


class ExecuteResultProtocol(Protocol):
    """Subset of SQLAlchemy result behaviour consumed by the store."""

    def mappings(self) -> MappingResultProtocol:
        """Return a mapping-aware view over the result rows."""

    def scalar_one_or_none(self) -> Optional[Any]:
        """Return the scalar value or ``None`` when no row exists."""

    def scalar(self) -> Any:
        """Return the first column of the first row or ``None``."""


class SessionProtocol(Protocol):
    """Behaviour shared by SQLAlchemy sessions and our fallbacks."""

    def execute(self, statement: Any, *args: Any, **kwargs: Any) -> ExecuteResultProtocol:
        """Execute a SQL expression and return a result handle."""

    def commit(self) -> None:
        """Commit the current transaction if supported."""

    def close(self) -> None:
        """Release any underlying connection resources."""


SessionFactory = Callable[[], SessionProtocol]

P = ParamSpec("P")
R = TypeVar("R")


def typed_router_get(
    router: APIRouter,
    path: str,
    **kwargs: Any,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Wrap ``router.get`` with typing preserved for decorated callables."""

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        wrapped = router.get(path, **kwargs)(func)
        return cast(Callable[P, Awaitable[R]], wrapped)

    return decorator


def _missing_sessionmaker(*_args: Any, **_kwargs: Any) -> SessionFactory:
    """Fallback sessionmaker that signals SQLAlchemy is required."""

    raise RuntimeError("sqlalchemy is required to use SQLStartupStateStore")


if sa is not None:
    SQLAlchemyError = _SQLAlchemyError
    sessionmaker = cast(Callable[..., SessionFactory], _sessionmaker)
    select: Optional[SelectCallable] = _select
else:
    SQLAlchemyError = _FallbackSQLAlchemyError
    sessionmaker = _missing_sessionmaker
    select = None


LOGGER = logging.getLogger(__name__)


class StartupMode(str, Enum):
    """Supported bootstrap flows for bringing the control plane online."""

    COLD = "cold"
    WARM = "warm"


class StartupStateStore(Protocol):
    """Persistence contract for recording startup progress."""

    async def read_state(self) -> Optional[Dict[str, Any]]:
        """Return the most recently persisted startup state."""

    async def write_state(
        self,
        *,
        startup_mode: str,
        last_offset: Optional[int],
        offsets: Mapping[str, int],
        updated_at: datetime,
    ) -> None:
        """Persist the latest startup state snapshot."""

    async def has_snapshot(self) -> bool:
        """Return ``True`` if a previous startup snapshot exists."""


class SQLStartupStateStore:
    """Persist startup metadata inside the ``startup_state`` table."""

    def __init__(self, engine: EngineProtocol, *, table_name: str = "startup_state") -> None:
        if sa is None or select is None:
            raise RuntimeError("sqlalchemy is required to use SQLStartupStateStore")

        assert select is not None
        assert sa is not None

        self._engine = engine
        self._session_factory: SessionFactory = sessionmaker(
            bind=engine,
            expire_on_commit=False,
            future=True,
        )
        self._select: SelectCallable = select
        self._sa: ModuleType = sa
        metadata = sa.MetaData()
        self._table = sa.Table(
            table_name,
            metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("startup_mode", sa.String(16), nullable=False),
            sa.Column("last_offset", sa.Integer, nullable=True),
            sa.Column("offsets", sa.String, nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        )
        metadata.create_all(engine, checkfirst=True)
        self._memory_state: Optional[Dict[str, Any]] = None
        self._use_memory_only = False
        try:
            session = self._session_factory()
            close = getattr(session, "close", None)
            if callable(close):
                close()
        except Exception:
            self._use_memory_only = True

    async def read_state(self) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(self._read_state)

    def _read_state(self) -> Optional[Dict[str, Any]]:
        if self._use_memory_only:
            return self._memory_state

        try:
            session = self._session_factory()
            try:
                stmt = self._select(self._table).order_by(self._table.c.updated_at.desc()).limit(1)
                result: ExecuteResultProtocol = session.execute(stmt)
                row = result.mappings().first()
            finally:
                close = getattr(session, "close", None)
                if callable(close):
                    close()
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.warning("Failed to load startup state from database", exc_info=True)
            self._use_memory_only = True
            return self._memory_state

        if row is None:
            return self._memory_state

        offsets_payload: Dict[str, int] = {}
        raw_offsets = row.get("offsets")
        if isinstance(raw_offsets, str) and raw_offsets.strip():
            try:
                decoded = json.loads(raw_offsets)
            except json.JSONDecodeError:
                decoded = {}
            if isinstance(decoded, Mapping):
                for key, value in decoded.items():
                    try:
                        offsets_payload[str(key)] = int(value)
                    except (TypeError, ValueError):
                        continue

        record = {
            "startup_mode": row.get("startup_mode"),
            "last_offset": row.get("last_offset"),
            "offsets": offsets_payload,
            "updated_at": row.get("updated_at"),
        }
        self._memory_state = record
        return record

    async def write_state(
        self,
        *,
        startup_mode: str,
        last_offset: Optional[int],
        offsets: Mapping[str, int],
        updated_at: datetime,
    ) -> None:
        await asyncio.to_thread(
            self._write_state,
            startup_mode,
            last_offset,
            offsets,
            updated_at,
        )

    def _write_state(
        self,
        startup_mode: str,
        last_offset: Optional[int],
        offsets: Mapping[str, int],
        updated_at: datetime,
    ) -> None:
        normalized_offsets = {str(k): int(v) for k, v in offsets.items()}
        payload = {
            "startup_mode": startup_mode,
            "last_offset": last_offset,
            "offsets": json.dumps(normalized_offsets, sort_keys=True),
            "updated_at": updated_at,
        }

        self._memory_state = {
            "startup_mode": startup_mode,
            "last_offset": last_offset,
            "offsets": normalized_offsets,
            "updated_at": updated_at,
        }

        if self._use_memory_only:
            return

        try:
            session = self._session_factory()
            try:
                result: ExecuteResultProtocol = session.execute(self._select(self._table.c.id))
                existing_id = result.scalar_one_or_none()
                if existing_id is None:
                    session.execute(self._table.insert().values(**payload))
                else:
                    session.execute(self._table.update().where(self._table.c.id == existing_id).values(**payload))
                commit = getattr(session, "commit", None)
                if callable(commit):
                    commit()
            finally:
                close = getattr(session, "close", None)
                if callable(close):
                    close()
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.warning("Failed to persist startup state to database", exc_info=True)
            self._use_memory_only = True

    async def has_snapshot(self) -> bool:
        return await asyncio.to_thread(self._has_snapshot)

    def _has_snapshot(self) -> bool:
        if self._use_memory_only:
            return self._memory_state is not None
        try:
            session = self._session_factory()
            try:
                stmt = self._select(self._sa.func.count()).select_from(self._table)
                result: ExecuteResultProtocol = session.execute(stmt)
                count = result.scalar()
                return bool(count)
            finally:
                close = getattr(session, "close", None)
                if callable(close):
                    close()
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.warning("Failed to check startup state snapshot", exc_info=True)
            self._use_memory_only = True
            return self._memory_state is not None


class StartupManager:
    """Coordinate restoration of state for the trading control plane."""

    def __init__(
        self,
        *,
        balance_loader: Callable[[], Awaitable[Any]] | Callable[[], Any] | None = None,
        order_loader: Callable[[], Awaitable[Any]] | Callable[[], Any] | None = None,
        position_loader: Callable[[], Awaitable[Any]] | Callable[[], Any] | None = None,
        reconcile_runner: Callable[[], Awaitable[Any]] | Callable[[], Any] | None = None,
        kafka_replay_handler: Callable[[Mapping[str, int]], Awaitable[Any]]
        | Callable[[Mapping[str, int]], Any]
        | None = None,
        state_store: StartupStateStore | None = None,
        snapshot_probe: Callable[[], Awaitable[bool]] | Callable[[], bool] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._balance_loader = self._wrap_loader(balance_loader)
        self._order_loader = self._wrap_loader(order_loader)
        self._position_loader = self._wrap_loader(position_loader)
        self._reconcile_runner = self._wrap_loader(reconcile_runner)
        self._kafka_replay_handler = kafka_replay_handler
        self._state_store = state_store
        self._snapshot_probe = self._wrap_loader(snapshot_probe)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

        self._mode: Optional[StartupMode] = None
        self._synced = False
        self._last_offset: Optional[int] = None
        self._last_error: Optional[str] = None
        self._offset_sources: MutableMapping[str, int] = {}

        self._status_lock = asyncio.Lock()
        self._run_lock = asyncio.Lock()

        self._persisted_state: Optional[Dict[str, Any]] = None
        self._seen_order_ids: set[str] = set()
        self._seen_fill_ids: set[str] = set()
        self._orders_cache: list[Any] = []
        self._fills_cache: list[Any] = []
        self._order_source_counts: DefaultDict[str, int] = defaultdict(int)
        self._fill_source_counts: DefaultDict[str, int] = defaultdict(int)

    async def start(self, mode: StartupMode | str | None = None) -> StartupMode:
        """Execute the bootstrap workflow using *mode* or auto-detect if omitted."""

        mode_value = await self._resolve_mode(mode)
        async with self._run_lock:
            LOGGER.info("Startup manager beginning %s start", mode_value.value)
            async with self._status_lock:
                self._mode = mode_value
                self._synced = False
                self._last_error = None
                self._seen_order_ids.clear()
                self._seen_fill_ids.clear()
                self._orders_cache.clear()
                self._fills_cache.clear()
                self._order_source_counts.clear()
                self._fill_source_counts.clear()

            try:
                if mode_value is StartupMode.COLD:
                    await self._run_cold_start()
                await self._replay_from_offsets()
                await self._run_reconcile()
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.exception("Startup manager failed during %s start", mode_value.value)
                async with self._status_lock:
                    self._last_error = str(exc)
                    self._synced = False
                raise
            else:
                LOGGER.info("Startup manager completed %s start", mode_value.value)
                async with self._status_lock:
                    self._synced = True
                await self._persist_state(mode_value)

        return mode_value

    async def _resolve_mode(self, mode: StartupMode | str | None) -> StartupMode:
        if mode is not None:
            return self._ensure_mode(mode)

        state = await self._load_startup_state()
        if state is None:
            LOGGER.debug("No persisted startup state found; defaulting to cold start")
            return StartupMode.COLD

        if self._snapshot_probe is not None:
            probe_result = await self._invoke_loader(self._snapshot_probe, "snapshot_probe")
            if not bool(probe_result):
                LOGGER.debug("Snapshot probe reported missing snapshot; defaulting to cold start")
                return StartupMode.COLD
        elif self._state_store is not None:
            has_snapshot = await self._state_store.has_snapshot()
            if not has_snapshot:
                LOGGER.debug("Startup store empty; defaulting to cold start")
                return StartupMode.COLD

        LOGGER.debug("Persisted startup state detected; defaulting to warm restart")
        return StartupMode.WARM

    async def status(self) -> Dict[str, Any]:
        """Return the most recent startup status snapshot."""

        if self._persisted_state is None:
            await self._load_startup_state()

        async with self._status_lock:
            persisted = self._persisted_state or {}
            mode = self._mode.value if self._mode else persisted.get("mode")
            last_offset = self._last_offset
            offsets_snapshot = dict(self._offset_sources)
            synced = self._synced
            last_error = self._last_error
            orders_seen = len(self._seen_order_ids)
            fills_seen = len(self._seen_fill_ids)
            order_sources = dict(self._order_source_counts)
            fill_sources = dict(self._fill_source_counts)

        if not offsets_snapshot:
            stored_offsets = persisted.get("offsets")
            offsets_snapshot = self._normalize_offsets(stored_offsets)

        if last_offset is None:
            stored_last = self._coerce_int(persisted.get("last_offset"))
            last_offset = stored_last if stored_last is not None else (
                max(offsets_snapshot.values()) if offsets_snapshot else None
            )

        return {
            "mode": mode,
            "synced": synced,
            "last_error": last_error,
            "offsets": {
                "last_offset": last_offset,
                "sources": offsets_snapshot,
            },
            "dedupe": {
                "orders": {
                    "total": orders_seen,
                    "sources": order_sources,
                },
                "fills": {
                    "total": fills_seen,
                    "sources": fill_sources,
                },
            },
        }

    async def _run_cold_start(self) -> None:
        await self._invoke_loader(self._balance_loader, "balances")
        orders_payload = await self._invoke_loader(self._order_loader, "open_orders")
        if orders_payload is not None:
            self._register_orders(orders_payload, source="bootstrap")
        await self._invoke_loader(self._position_loader, "positions")

    async def _run_reconcile(self) -> None:
        await self._invoke_loader(self._reconcile_runner, "reconcile")

    async def _load_startup_state(self) -> Optional[Dict[str, Any]]:
        if self._state_store is None:
            async with self._status_lock:
                self._persisted_state = None
            return None

        state = await self._state_store.read_state()
        normalized: Optional[Dict[str, Any]] = None
        if state is not None:
            offsets = self._normalize_offsets(state.get("offsets"))
            last_offset = self._coerce_int(state.get("last_offset"))
            normalized = {
                "mode": state.get("mode") or state.get("startup_mode"),
                "last_offset": last_offset,
                "offsets": offsets,
                "updated_at": state.get("updated_at"),
            }
        async with self._status_lock:
            self._persisted_state = normalized
            if normalized is not None and self._mode is None:
                if normalized.get("last_offset") is not None:
                    self._last_offset = normalized["last_offset"]
                if normalized.get("offsets"):
                    self._offset_sources = dict(normalized["offsets"])
        return normalized

    async def _persist_state(self, mode: StartupMode) -> None:
        if self._state_store is None:
            return

        ts = self._clock()
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        offsets_snapshot: Dict[str, int]
        async with self._status_lock:
            offsets_snapshot = dict(self._offset_sources)
            last_offset = self._last_offset

        try:
            await self._state_store.write_state(
                startup_mode=mode.value,
                last_offset=last_offset,
                offsets=offsets_snapshot,
                updated_at=ts,
            )
        except Exception as exc:  # pragma: no cover - IO failures should not crash startup
            LOGGER.warning("Unable to persist startup state: %s", exc)
        else:
            async with self._status_lock:
                self._persisted_state = {
                    "mode": mode.value,
                    "last_offset": last_offset,
                    "offsets": offsets_snapshot,
                    "updated_at": ts,
                }

    async def _replay_from_offsets(self) -> None:
        async with self._status_lock:
            offsets = dict(self._offset_sources)
            last_offset = self._last_offset
            persisted = self._persisted_state

        if not offsets and persisted is not None:
            offsets = self._normalize_offsets(persisted.get("offsets"))

        if last_offset is None and persisted is not None:
            last_offset = self._coerce_int(persisted.get("last_offset"))

        new_last = await self._dispatch_replay(offsets, last_offset)

        if new_last is None and offsets:
            new_last = max(offsets.values()) if offsets else None

        async with self._status_lock:
            if offsets:
                self._offset_sources = dict(offsets)
            else:
                self._offset_sources.clear()
            self._last_offset = new_last

    async def _dispatch_replay(
        self, offsets: MutableMapping[str, int], last_offset: Optional[int]
    ) -> Optional[int]:
        handler = self._kafka_replay_handler
        if handler is None:
            LOGGER.info("Kafka replay handler not configured; skipping replay")
            return last_offset

        payload = dict(offsets)
        if last_offset is not None:
            payload.setdefault("__last_offset__", last_offset)

        try:
            result = handler(payload)
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning("Kafka replay handler failed: %s", exc)
            return last_offset

        new_last = self._process_replay_result(result, offsets, last_offset)
        return new_last if new_last is not None else last_offset

        coerced = self._coerce_int(result)
        return coerced if coerced is not None else last_offset

    async def _invoke_loader(
        self,
        loader: Callable[[], Awaitable[Any]] | None,
        label: str,
    ) -> Any:
        if loader is None:
            LOGGER.debug("No %s loader configured", label)
            return None
        try:
            result = loader()
            if inspect.isawaitable(result):
                return await result
            return result
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Startup %s loader failed", label)
            raise

    def _process_replay_result(
        self,
        result: Any,
        offsets: MutableMapping[str, int],
        last_offset: Optional[int],
    ) -> Optional[int]:
        if isinstance(result, tuple) and len(result) == 2:
            # Allow handlers to return (offsets, events)
            offsets_candidate, events_candidate = result
            new_last = self._process_replay_result(offsets_candidate, offsets, last_offset)
            self._consume_event_payload(events_candidate, source="replay")
            return new_last

        if isinstance(result, Mapping):
            self._consume_event_payload(result, source="replay")
            offsets_candidate = self._extract_offsets_from_mapping(result)
            if offsets_candidate:
                offsets.update(offsets_candidate)
                return max(offsets_candidate.values())
            return last_offset

        self._consume_event_payload(result, source="replay")
        normalized = self._normalize_offsets(result)
        if normalized:
            offsets.update(normalized)
            return max(normalized.values())
        return last_offset

    def _extract_offsets_from_mapping(self, mapping: Mapping[str, Any]) -> Dict[str, int]:
        if "offsets" in mapping:
            raw_offsets = mapping.get("offsets")
            normalized = self._normalize_offsets(raw_offsets)
            if normalized:
                return normalized
        normalized_self = self._normalize_offsets(mapping)
        return normalized_self

    def _consume_event_payload(self, payload: Any, *, source: str) -> None:
        if payload is None:
            return

        if isinstance(payload, Mapping):
            orders_payload = payload.get("orders") or payload.get("open_orders")
            fills_payload = (
                payload.get("fills")
                or payload.get("executions")
                or payload.get("trades")
            )
            if orders_payload is not None:
                deduped_orders = self._register_orders(orders_payload, source=source)
                if isinstance(payload, MutableMapping) and deduped_orders is not None:
                    payload["orders"] = deduped_orders
            if fills_payload is not None:
                deduped_fills = self._register_fills(fills_payload, source=source)
                if isinstance(payload, MutableMapping) and deduped_fills is not None:
                    payload["fills"] = deduped_fills
            return

        self._register_orders(payload, source=source)
        self._register_fills(payload, source=source)

    def _register_orders(self, payload: Any, *, source: str) -> Optional[Sequence[Any]]:
        return self._register_records(
            payload,
            record_type="order",
            id_fields=("order_id", "id", "client_order_id"),
            seen=self._seen_order_ids,
            cache=self._orders_cache,
            counts=self._order_source_counts,
            source=source,
        )

    def _register_fills(self, payload: Any, *, source: str) -> Optional[Sequence[Any]]:
        return self._register_records(
            payload,
            record_type="fill",
            id_fields=("fill_id", "execution_id", "trade_id", "id"),
            seen=self._seen_fill_ids,
            cache=self._fills_cache,
            counts=self._fill_source_counts,
            source=source,
        )

    def _register_records(
        self,
        payload: Any,
        *,
        record_type: str,
        id_fields: Sequence[str],
        seen: set[str],
        cache: list[Any],
        counts: MutableMapping[str, int],
        source: str,
    ) -> Optional[Sequence[Any]]:
        sequence = self._coerce_sequence(payload)
        if sequence is None:
            return None

        unique_records: list[Any] = []
        for record in sequence:
            identifier = self._extract_identifier(record, id_fields)
            if identifier is not None:
                if identifier in seen:
                    LOGGER.debug(
                        "Skipping duplicate %s %s from %s stage",
                        record_type,
                        identifier,
                        source,
                    )
                    continue
                seen.add(identifier)
            unique_records.append(record)

        if not unique_records:
            return []

        cache.extend(unique_records)
        counts[source] += len(unique_records)
        return unique_records

    def _coerce_sequence(self, payload: Any) -> Optional[list[Any]]:
        if payload is None:
            return None
        if isinstance(payload, list):
            return payload
        if isinstance(payload, tuple):
            return list(payload)
        if isinstance(payload, set):
            return list(payload)
        if isinstance(payload, Mapping):
            return list(payload.values())
        if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes)):
            return list(payload)
        return [payload]

    def _extract_identifier(self, record: Any, id_fields: Sequence[str]) -> Optional[str]:
        if isinstance(record, Mapping):
            for field in id_fields:
                if field in record and record[field] is not None:
                    return str(record[field])
        for field in id_fields:
            value = getattr(record, field, None)
            if value is not None:
                return str(value)
        return None

    def _wrap_loader(
        self,
        loader: Callable[[], Awaitable[Any]] | Callable[[], Any] | None,
    ) -> Callable[[], Awaitable[Any]] | None:
        if loader is None:
            return None

        if not callable(loader):
            raise TypeError("Loader must be callable")

        async def _runner() -> Any:
            result = loader()
            if inspect.isawaitable(result):
                return await result
            return result

        return _runner

    def _ensure_mode(self, mode: StartupMode | str) -> StartupMode:
        if isinstance(mode, StartupMode):
            return mode
        try:
            return StartupMode(mode)
        except ValueError as exc:  # pragma: no cover - defensive
            raise ValueError(f"Unsupported startup mode: {mode}") from exc

    def _normalize_offsets(self, mapping: Any) -> Dict[str, int]:
        if not isinstance(mapping, Mapping):
            return {}
        normalized: Dict[str, int] = {}
        for key, value in mapping.items():
            coerced = self._coerce_int(value)
            if coerced is None:
                continue
            normalized[str(key)] = coerced
        return normalized

    def _coerce_int(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(str(value).strip())
            except (TypeError, ValueError):
                return None


router = APIRouter(prefix="/startup", tags=["startup"])

_MANAGER: Optional[StartupManager] = None


@typed_router_get(router, "/status")
async def startup_status() -> Dict[str, Any]:
    if _MANAGER is None:
        raise HTTPException(status_code=503, detail="Startup manager not configured")
    return await _MANAGER.status()


def register(app: FastAPI, manager: StartupManager) -> StartupManager:
    global _MANAGER
    _MANAGER = manager
    app.include_router(router)
    app.state.startup_manager = manager
    return manager


__all__ = [
    "SQLStartupStateStore",
    "StartupManager",
    "StartupMode",
    "register",
    "router",
]
