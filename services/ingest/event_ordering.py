"""Event ordering utilities for streaming ingestion pipelines."""
from __future__ import annotations

import datetime as dt
import heapq
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, List, Mapping, MutableMapping, Optional, Tuple

from metrics import increment_late_events, set_reorder_buffer_depth

LOGGER = logging.getLogger(__name__)

_DEFAULT_KEY = "__global__"


def _ensure_timezone(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _to_milliseconds(value: dt.datetime) -> int:
    value = _ensure_timezone(value)
    return int(value.timestamp() * 1000)


@dataclass(order=True)
class _BufferedEvent:
    event_ts_ms: int
    sequence: int
    payload: Mapping[str, Any] = field(compare=False)
    event_ts: dt.datetime = field(compare=False)
    arrival_ts: dt.datetime = field(compare=False)
    key: str = field(compare=False)


@dataclass
class _StreamState:
    buffer: List[_BufferedEvent] = field(default_factory=list)
    max_event_ms: Optional[int] = None
    last_emitted_ms: Optional[int] = None
    sequence: int = 0


@dataclass(frozen=True)
class OrderedEvent:
    """Represents an event that has passed ordering checks."""

    stream: str
    payload: Mapping[str, Any]
    event_ts: dt.datetime
    arrival_ts: dt.datetime
    key: Optional[str] = None
    lateness_ms: int = 0
    is_late: bool = False


class EventOrderingBuffer:
    """Buffers and orders streaming events subject to a lateness watermark."""

    def __init__(
        self,
        *,
        stream_name: str,
        max_lateness_ms: int,
        timestamp_getter: Callable[[Mapping[str, Any]], dt.datetime],
        key_getter: Optional[Callable[[Mapping[str, Any]], Any]] = None,
        service_name: Optional[str] = None,
    ) -> None:
        if max_lateness_ms < 0:
            raise ValueError("max_lateness_ms must be non-negative")
        self.stream_name = stream_name
        self.max_lateness_ms = max_lateness_ms
        self._timestamp_getter = timestamp_getter
        self._key_getter = key_getter
        self._service_name = service_name
        self._states: MutableMapping[str, _StreamState] = {}

    def add(self, payload: Mapping[str, Any]) -> Tuple[List[OrderedEvent], List[OrderedEvent]]:
        """Register an event and return in-order and late events."""

        event_ts = _ensure_timezone(self._timestamp_getter(payload))
        arrival_ts = dt.datetime.now(tz=dt.timezone.utc)
        event_ms = _to_milliseconds(event_ts)
        key = self._normalise_key(payload)
        state = self._states.setdefault(key, _StreamState())

        if state.max_event_ms is None or event_ms > state.max_event_ms:
            state.max_event_ms = event_ms
        watermark = (state.max_event_ms or event_ms) - self.max_lateness_ms

        ready: List[OrderedEvent] = []
        late: List[OrderedEvent] = []

        if state.last_emitted_ms is not None and event_ms < state.last_emitted_ms:
            lateness = max(watermark - event_ms, 0)
            late_event = self._build_event(
                payload,
                event_ts,
                arrival_ts,
                key,
                lateness_ms=lateness,
                is_late=True,
            )
            late.append(late_event)
            increment_late_events(self.stream_name, service=self._service_name)
            self._update_depth_metric()
            return ready, late

        if event_ms < watermark:
            lateness = watermark - event_ms
            late_event = self._build_event(
                payload,
                event_ts,
                arrival_ts,
                key,
                lateness_ms=lateness,
                is_late=True,
            )
            late.append(late_event)
            increment_late_events(self.stream_name, service=self._service_name)
            self._update_depth_metric()
            return ready, late

        buffered = _BufferedEvent(
            event_ts_ms=event_ms,
            sequence=state.sequence,
            payload=payload,
            event_ts=event_ts,
            arrival_ts=arrival_ts,
            key=key,
        )
        state.sequence += 1
        heapq.heappush(state.buffer, buffered)

        ready.extend(self._drain_ready(state, watermark))
        self._update_depth_metric()
        return ready, late

    def drain(self, *, force: bool = False) -> List[OrderedEvent]:
        """Drain ready events from all buffers."""

        drained: List[OrderedEvent] = []
        for key, state in self._states.items():
            if not state.buffer:
                continue
            watermark = (state.max_event_ms or 0)
            if not force:
                watermark -= self.max_lateness_ms
            drained.extend(self._drain_ready(state, watermark, force=force))
        drained.sort(key=lambda evt: (evt.event_ts, evt.key or ""))
        self._update_depth_metric()
        return drained

    def _drain_ready(
        self, state: _StreamState, watermark: int, *, force: bool = False
    ) -> List[OrderedEvent]:
        ready: List[OrderedEvent] = []
        while state.buffer and (force or state.buffer[0].event_ts_ms <= watermark):
            buffered = heapq.heappop(state.buffer)
            state.last_emitted_ms = buffered.event_ts_ms
            lateness_ms = 0
            if state.max_event_ms is not None:
                lateness_ms = max(state.max_event_ms - buffered.event_ts_ms, 0)
            ready.append(
                self._build_event(
                    buffered.payload,
                    buffered.event_ts,
                    buffered.arrival_ts,
                    buffered.key,
                    lateness_ms=lateness_ms,
                    is_late=False,
                )
            )
        return ready

    def _build_event(
        self,
        payload: Mapping[str, Any],
        event_ts: dt.datetime,
        arrival_ts: dt.datetime,
        key: str,
        *,
        lateness_ms: int,
        is_late: bool,
    ) -> OrderedEvent:
        return OrderedEvent(
            stream=self.stream_name,
            payload=payload,
            event_ts=_ensure_timezone(event_ts),
            arrival_ts=_ensure_timezone(arrival_ts),
            key=key,
            lateness_ms=max(int(lateness_ms), 0),
            is_late=is_late,
        )

    def _normalise_key(self, payload: Mapping[str, Any]) -> str:
        if self._key_getter is None:
            return _DEFAULT_KEY
        try:
            key = self._key_getter(payload)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to derive ordering key; defaulting to global")
            return _DEFAULT_KEY
        if key is None:
            return _DEFAULT_KEY
        return str(key)

    def _update_depth_metric(self) -> None:
        depth = sum(len(state.buffer) for state in self._states.values())
        set_reorder_buffer_depth(
            self.stream_name,
            depth,
            service=self._service_name,
        )

    def __len__(self) -> int:
        return sum(len(state.buffer) for state in self._states.values())

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"EventOrderingBuffer(stream={self.stream_name!r}, "
            f"max_lateness_ms={self.max_lateness_ms}, depth={len(self)})"
        )
