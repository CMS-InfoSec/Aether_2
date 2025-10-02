from __future__ import annotations

import asyncio
import inspect
import json
import logging
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Mapping, MutableMapping, Optional

from fastapi import APIRouter, FastAPI, HTTPException

LOGGER = logging.getLogger(__name__)


class StartupMode(str, Enum):
    """Supported bootstrap flows for bringing the control plane online."""

    COLD = "cold"
    WARM = "warm"


class StartupManager:
    """Coordinate restoration of state for the trading control plane."""

    def __init__(
        self,
        *,
        balance_loader: Callable[[], Awaitable[Any]] | Callable[[], Any] | None = None,
        position_loader: Callable[[], Awaitable[Any]] | Callable[[], Any] | None = None,
        reconcile_runner: Callable[[], Awaitable[Any]] | Callable[[], Any] | None = None,
        kafka_replay_handler: Callable[[Mapping[str, int]], Awaitable[int | None]]
        | Callable[[Mapping[str, int]], int | None]
        | None = None,
        offset_log_path: str | Path | None = None,
        startup_state_path: str | Path | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._balance_loader = self._wrap_loader(balance_loader)
        self._position_loader = self._wrap_loader(position_loader)
        self._reconcile_runner = self._wrap_loader(reconcile_runner)
        self._kafka_replay_handler = kafka_replay_handler
        self._offset_log_path = Path(offset_log_path) if offset_log_path else None
        self._startup_state_path = Path(startup_state_path) if startup_state_path else None
        self._clock = clock or (lambda: datetime.now(timezone.utc))

        self._mode: Optional[StartupMode] = None
        self._synced = False
        self._last_offset: Optional[int] = None
        self._last_error: Optional[str] = None
        self._offset_sources: MutableMapping[str, int] = {}

        self._status_lock = asyncio.Lock()
        self._run_lock = asyncio.Lock()

        self._persisted_state: Optional[Dict[str, Any]] = self._read_startup_state()
        if self._persisted_state is not None:
            persisted_offset = self._persisted_state.get("last_offset")
            if isinstance(persisted_offset, int):
                self._last_offset = persisted_offset

    async def start(self, mode: StartupMode | str | None = None) -> StartupMode:
        """Execute the bootstrap workflow using *mode* or auto-detect if omitted."""

        mode_value = await self._resolve_mode(mode)
        async with self._run_lock:
            LOGGER.info("Startup manager beginning %s start", mode_value.value)
            async with self._status_lock:
                self._mode = mode_value
                self._synced = False
                self._last_error = None

            try:
                if mode_value is StartupMode.COLD:
                    await self._run_cold_start()
                    await self._replay_from_offsets()
                else:
                    await self._replay_from_offsets()
                    await self._run_warm_start()
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

        LOGGER.debug("Persisted startup state detected; defaulting to warm restart")
        return StartupMode.WARM

    async def status(self) -> Dict[str, Any]:
        """Return the most recent startup status snapshot."""

        async with self._status_lock:
            mode = self._mode.value if self._mode else None
            offset = self._last_offset
            if mode is None and self._persisted_state:
                persisted_mode = self._persisted_state.get("mode")
                if isinstance(persisted_mode, str):
                    mode = persisted_mode
                if offset is None:
                    persisted_offset = self._persisted_state.get("last_offset")
                    if isinstance(persisted_offset, int):
                        offset = persisted_offset
            return {
                "mode": mode,
                "synced": self._synced,
                "offset": offset,
            }

    async def _run_cold_start(self) -> None:
        await self._invoke_loader(self._balance_loader, "balances")
        await self._invoke_loader(self._position_loader, "positions")

    async def _run_warm_start(self) -> None:
        if self._reconcile_runner is None:
            LOGGER.info("Warm start requested but no reconcile runner configured")
            return
        await self._invoke_loader(self._reconcile_runner, "reconcile")

    async def _load_startup_state(self) -> Optional[Dict[str, Any]]:
        if self._startup_state_path is None:
            async with self._status_lock:
                self._persisted_state = None
            return None

        state = await asyncio.to_thread(self._read_startup_state)
        async with self._status_lock:
            self._persisted_state = state
            if state and self._mode is None:
                persisted_offset = state.get("last_offset")
                if isinstance(persisted_offset, int):
                    self._last_offset = persisted_offset
        return state

    async def _persist_state(self, mode: StartupMode) -> None:
        if self._startup_state_path is None:
            return

        ts = self._clock()
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        payload = {
            "mode": mode.value,
            "last_offset": self._last_offset,
            "ts": ts.isoformat() if isinstance(ts, datetime) else str(ts),
        }

        try:
            await asyncio.to_thread(self._write_startup_state, payload)
        except Exception as exc:  # pragma: no cover - IO failures should not crash startup
            LOGGER.warning("Unable to persist startup state: %s", exc)
        async with self._status_lock:
            self._persisted_state = payload

    async def _replay_from_offsets(self) -> None:
        offsets = await self._load_offset_logs()
        if offsets:
            await self._dispatch_replay(offsets)
            self._last_offset = max(offsets.values()) if offsets else None
            self._offset_sources = offsets
        else:
            self._last_offset = None
            self._offset_sources.clear()

    def _read_startup_state(self) -> Optional[Dict[str, Any]]:
        path = self._startup_state_path
        if path is None or not path.exists():
            return None

        try:
            raw = path.read_text(encoding="utf-8").strip()
        except OSError as exc:  # pragma: no cover - defensive IO guard
            LOGGER.warning("Unable to read startup state from %s: %s", path, exc)
            return None

        if not raw:
            return None

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            LOGGER.warning("Invalid startup state payload in %s: %s", path, exc)
            return None

        if not isinstance(payload, dict):
            LOGGER.debug("Ignoring startup state from %s because payload is not an object", path)
            return None

        mode: Optional[str]
        raw_mode = payload.get("mode")
        if isinstance(raw_mode, str):
            mode = raw_mode
        elif raw_mode is None:
            mode = None
        else:
            mode = str(raw_mode)

        if mode is not None:
            try:
                StartupMode(mode)
            except ValueError:
                LOGGER.debug("Encountered unsupported startup mode %s in %s", mode, path)
                mode = None

        last_offset = self._coerce_int(payload.get("last_offset"))
        ts = payload.get("ts")
        if ts is not None and not isinstance(ts, str):
            ts = str(ts)

        return {"mode": mode, "last_offset": last_offset, "ts": ts}

    def _write_startup_state(self, payload: Mapping[str, Any]) -> None:
        path = self._startup_state_path
        if path is None:
            return

        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

        text = json.dumps(payload, sort_keys=True)
        path.write_text(text, encoding="utf-8")

    async def _dispatch_replay(self, offsets: Mapping[str, int]) -> None:
        if self._kafka_replay_handler is None:
            LOGGER.info("Kafka replay handler not configured; skipping replay")
            return

        try:
            result = self._kafka_replay_handler(offsets)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning("Kafka replay handler failed: %s", exc)

    async def _load_offset_logs(self) -> Dict[str, int]:
        if self._offset_log_path is None:
            return {}
        return await asyncio.to_thread(self._read_offset_logs)

    def _read_offset_logs(self) -> Dict[str, int]:
        path = self._offset_log_path
        assert path is not None
        if not path.exists():
            return {}

        files: list[Path] = []
        if path.is_dir():
            for pattern in ("*.json", "*.log", "*"):
                files.extend(candidate for candidate in path.glob(pattern) if candidate.is_file())
            # Deduplicate while preserving order
            seen: set[Path] = set()
            unique: list[Path] = []
            for candidate in files:
                if candidate in seen:
                    continue
                seen.add(candidate)
                unique.append(candidate)
            files = unique
        else:
            files = [path]

        offsets: Dict[str, int] = {}
        for file_path in files:
            try:
                raw_text = file_path.read_text().strip()
            except OSError as exc:  # pragma: no cover - IO failure
                LOGGER.warning("Unable to read offset log %s: %s", file_path, exc)
                continue

            if not raw_text:
                continue

            parsed: Any
            try:
                parsed = json.loads(raw_text)
            except json.JSONDecodeError:
                value = self._coerce_int(raw_text)
                if value is not None:
                    self._record_offset(offsets, file_path.stem, value)
                else:
                    LOGGER.debug("Skipping malformed offset log %s", file_path)
                continue

            extracted = self._extract_offsets(file_path.stem, parsed)
            for key, offset in extracted.items():
                self._record_offset(offsets, key, offset)

        return offsets

    def _extract_offsets(self, prefix: str, payload: Any) -> Dict[str, int]:
        if isinstance(payload, dict):
            partitions = payload.get("partitions")
            if isinstance(partitions, dict):
                return {
                    f"{prefix}:{partition}": value
                    for partition, entry in partitions.items()
                    if (value := self._coerce_entry(entry)) is not None
                }
            value = self._coerce_entry(payload)
            return {prefix: value} if value is not None else {}

        if isinstance(payload, list):
            collected: Dict[str, int] = {}
            for index, entry in enumerate(payload):
                value = self._coerce_entry(entry)
                if value is not None:
                    collected[f"{prefix}:{index}"] = value
            return collected

        value = self._coerce_entry(payload)
        return {prefix: value} if value is not None else {}

    def _record_offset(self, offsets: MutableMapping[str, int], key: str, offset: int) -> None:
        current = offsets.get(key)
        if current is None or offset > current:
            offsets[key] = offset

    async def _invoke_loader(
        self,
        loader: Callable[[], Awaitable[Any]] | None,
        label: str,
    ) -> None:
        if loader is None:
            LOGGER.debug("No %s loader configured", label)
            return
        try:
            result = loader()
            if inspect.isawaitable(result):
                await result
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Startup %s loader failed", label)
            raise

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

    def _coerce_entry(self, entry: Any) -> Optional[int]:
        if isinstance(entry, dict):
            for key in ("next_offset", "offset", "last_offset"):
                if key not in entry:
                    continue
                value = self._coerce_int(entry[key])
                if value is None:
                    continue
                if key == "last_offset":
                    return value + 1
                return value
            return None
        return self._coerce_int(entry)

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


@router.get("/status")
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


__all__ = ["StartupManager", "StartupMode", "register", "router"]
