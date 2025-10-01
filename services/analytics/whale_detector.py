"""Whale trade detection and toxic flow analytics service."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Deque, Dict, Iterable, Literal, Optional

from fastapi import FastAPI, Query
from pydantic import BaseModel, ConfigDict, Field


def _utcnow() -> datetime:
    """Return the current UTC timestamp."""

    return datetime.now(timezone.utc)


@dataclass(slots=True)
class WhaleEvent:
    """Structured representation of a detected whale trade."""

    symbol: str
    size: float
    side: Literal["buy", "sell"]
    impact: float
    ts: datetime
    notional: float
    aggressive: bool = False
    burst: bool = False


@dataclass(slots=True)
class ToxicFlowMetric:
    """Summarises the toxicity of aggressive whale order flow."""

    symbol: str
    toxic_score: float
    ts: datetime


class TradeObservation(BaseModel):
    """Inbound trade observation used to trigger whale detection."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(..., description="Instrument symbol for the trade")
    size: float = Field(..., gt=0, description="Executed quantity in base units")
    notional: float = Field(..., gt=0, description="Trade notional in quote currency")
    side: Literal["buy", "sell"] = Field(..., description="Trade direction")
    impact: float = Field(
        ..., description="Signed price impact in bps (positive = price up)"
    )
    aggressive: bool = Field(
        default=False,
        description="Whether the trade removed liquidity (market/aggressive)",
    )
    ts: Optional[datetime] = Field(
        default=None,
        description="Execution timestamp. Defaults to current UTC time if omitted.",
    )


class WhaleEventResponse(BaseModel):
    """API response model for recently detected whale trades."""

    model_config = ConfigDict(from_attributes=True)

    symbol: str
    size: float
    side: Literal["buy", "sell"]
    impact: float
    ts: datetime
    notional: float
    aggressive: bool
    burst: bool


class WhaleDetector:
    """Detect whale trades and evaluate associated toxic order flow."""

    def __init__(
        self,
        threshold: float = 50_000.0,
        burst_window: timedelta = timedelta(seconds=5),
        burst_min_trades: int = 3,
        max_events_per_symbol: int = 200,
        max_metrics_per_symbol: int = 100,
    ) -> None:
        self.threshold = threshold
        self.burst_window = burst_window
        self.burst_min_trades = burst_min_trades
        self.max_events_per_symbol = max_events_per_symbol
        self.max_metrics_per_symbol = max_metrics_per_symbol
        self._lock = Lock()
        self._events: Dict[str, Deque[WhaleEvent]] = {}
        self._aggressive_windows: Dict[str, Deque[WhaleEvent]] = {}
        self._metrics: Dict[str, Deque[ToxicFlowMetric]] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
    def observe_trade(self, trade: TradeObservation) -> Optional[WhaleEvent]:
        """Process a trade observation and record whale events when triggered."""

        timestamp = trade.ts or _utcnow()
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)
        if trade.notional <= self.threshold:
            return None

        event = WhaleEvent(
            symbol=trade.symbol,
            size=trade.size,
            side=trade.side,
            impact=trade.impact,
            ts=timestamp,
            notional=trade.notional,
            aggressive=trade.aggressive,
            burst=False,
        )

        with self._lock:
            events = self._get_event_queue(trade.symbol)
            events.append(event)

            if trade.aggressive:
                self._update_aggressive_flow(event)

        return event

    def recent_events(self, symbol: Optional[str] = None, limit: int = 50) -> list[WhaleEvent]:
        """Return the most recent whale events across symbols."""

        with self._lock:
            if symbol is not None:
                events = list(self._events.get(symbol, tuple()))
            else:
                events = [event for queue in self._events.values() for event in queue]

        events.sort(key=lambda evt: evt.ts, reverse=True)
        return events[:limit]

    def latest_toxic_metric(self, symbol: str) -> Optional[ToxicFlowMetric]:
        """Return the latest toxic flow metric for a symbol, if any."""

        with self._lock:
            queue = self._metrics.get(symbol)
            if not queue:
                return None
            return queue[-1]

    def iter_metrics(self, symbol: Optional[str] = None) -> Iterable[ToxicFlowMetric]:
        """Yield toxic flow metrics for a symbol or all symbols."""

        with self._lock:
            if symbol is not None:
                yield from list(self._metrics.get(symbol, tuple()))
            else:
                for metrics in self._metrics.values():
                    yield from list(metrics)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _get_event_queue(self, symbol: str) -> Deque[WhaleEvent]:
        queue = self._events.get(symbol)
        if queue is None or queue.maxlen != self.max_events_per_symbol:
            queue = deque(maxlen=self.max_events_per_symbol)
            self._events[symbol] = queue
        return queue

    def _get_metric_queue(self, symbol: str) -> Deque[ToxicFlowMetric]:
        queue = self._metrics.get(symbol)
        if queue is None or queue.maxlen != self.max_metrics_per_symbol:
            queue = deque(maxlen=self.max_metrics_per_symbol)
            self._metrics[symbol] = queue
        return queue

    def _get_aggressive_window(self, symbol: str) -> Deque[WhaleEvent]:
        window = self._aggressive_windows.get(symbol)
        if window is None:
            window = deque()
            self._aggressive_windows[symbol] = window
        return window

    def _update_aggressive_flow(self, event: WhaleEvent) -> None:
        window = self._get_aggressive_window(event.symbol)
        window.append(event)
        cutoff = event.ts - self.burst_window
        while window and window[0].ts < cutoff:
            window.popleft()

        if len(window) >= self.burst_min_trades:
            for queued_event in window:
                queued_event.burst = True
            metric = self._compute_toxic_metric(window)
            if metric is not None:
                metrics = self._get_metric_queue(event.symbol)
                metrics.append(metric)

    def _compute_toxic_metric(self, window: Iterable[WhaleEvent]) -> Optional[ToxicFlowMetric]:
        events = list(window)
        if not events:
            return None

        adverse = sum(1 for event in events if self._is_adverse(event))
        toxic_score = adverse / len(events)
        return ToxicFlowMetric(symbol=events[-1].symbol, toxic_score=toxic_score, ts=events[-1].ts)

    @staticmethod
    def _is_adverse(event: WhaleEvent) -> bool:
        if event.side == "buy":
            return event.impact < 0
        return event.impact > 0


# ---------------------------------------------------------------------------
# FastAPI wiring
# ---------------------------------------------------------------------------

detector = WhaleDetector()
app = FastAPI(title="Whale Detector Service", version="1.0.0")


@app.get("/whales/recent", response_model=list[WhaleEventResponse])
async def recent_whales(
    symbol: Optional[str] = Query(default=None, description="Filter by symbol"),
    limit: int = Query(
        default=50,
        ge=1,
        le=500,
        description="Maximum number of whale trades to return",
    ),
) -> list[WhaleEventResponse]:
    """Return recently observed whale trades, optionally filtered by symbol."""

    events = detector.recent_events(symbol=symbol, limit=limit)
    return [WhaleEventResponse.model_validate(event) for event in events]
