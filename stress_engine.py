"""Stress scenario utilities built on top of :mod:`backtest_engine`."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import pandas as pd

from backtest_engine import Backtester, Fill


@dataclass
class StressMetrics:
    """Container holding the key performance figures for a stress run."""

    pnl: float
    sharpe: float
    fees: float
    slippage: float
    win_rate: float

    def as_dict(self) -> Dict[str, float]:
        """Return a dictionary representation useful for serialisation."""

        return {
            "pnl": self.pnl,
            "sharpe": self.sharpe,
            "fees": self.fees,
            "slippage": self.slippage,
            "win_rate": self.win_rate,
        }


class StressEngine:
    """Runs targeted stress scenarios using a pre-configured backtester."""

    def __init__(self, backtester: Backtester) -> None:
        self._backtester = backtester

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run_flash_crash(self, drop_pct: float = 0.2, volume_multiplier: float = 2.0) -> StressMetrics:
        events = self._clone_events()
        self._apply_flash_crash(events, drop_pct, volume_multiplier)
        metrics = self._backtester.run_with_events(events)
        return self._build_metrics(metrics)

    def run_liquidity_halt(self, halt_length: int = 5) -> StressMetrics:
        events = self._clone_events()
        self._apply_liquidity_halt(events, halt_length)
        metrics = self._backtester.run_with_events(events)
        return self._build_metrics(metrics)

    def run_gap_open(self, gap_pct: float = 0.05) -> StressMetrics:
        events = self._clone_events()
        self._apply_gap_open(events, gap_pct)
        metrics = self._backtester.run_with_events(events)
        return self._build_metrics(metrics)

    def run_volatility_spike(self, spread_multiplier: float = 3.0, volume_multiplier: float = 3.0) -> StressMetrics:
        events = self._clone_events()
        self._apply_volatility_spike(events, spread_multiplier, volume_multiplier)
        metrics = self._backtester.run_with_events(events)
        return self._build_metrics(metrics)

    # ------------------------------------------------------------------
    # Deterministic replay support
    # ------------------------------------------------------------------
    @staticmethod
    def replay_from_ws_log(path: Path | str) -> List[Dict[str, Any]]:
        """Parse a Kraken WebSocket log into an ordered list of fills.

        The Kraken public trade feed emits messages of the form::

            [channel_id, [[price, volume, time, side, order_type, misc], ...], "trade", "pair"]

        This method parses such logs and returns a deterministic sequence of
        fill dictionaries sorted by timestamp.
        """

        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"Log file not found: {file_path}")

        fills: List[Dict[str, Any]] = []
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                payload = line.strip()
                if not payload:
                    continue
                try:
                    message = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if not isinstance(message, list) or len(message) < 3:
                    continue
                data = message[1]
                channel = message[2]
                if channel != "trade" or not isinstance(data, list):
                    continue
                for trade in data:
                    if not isinstance(trade, Sequence) or len(trade) < 5:
                        continue
                    try:
                        price = float(trade[0])
                        quantity = float(trade[1])
                        timestamp = pd.Timestamp(float(trade[2]), unit="s", tz="UTC")
                    except (TypeError, ValueError):
                        continue
                    side_token = str(trade[3]).lower()
                    side = "buy" if side_token.startswith("b") else "sell"
                    fills.append(
                        {
                            "timestamp": timestamp,
                            "side": side,
                            "price": price,
                            "quantity": quantity,
                            "order_type": str(trade[4]),
                        }
                    )
        fills.sort(key=lambda fill: fill["timestamp"])
        return fills

    # ------------------------------------------------------------------
    # Scenario builders
    # ------------------------------------------------------------------
    def _clone_events(self) -> List[Dict[str, Any]]:
        return [dict(event) for event in self._backtester.base_events]

    @staticmethod
    def _bar_indices(events: List[Dict[str, Any]]) -> List[int]:
        return [idx for idx, event in enumerate(events) if event.get("type") == "bar"]

    @staticmethod
    def _book_indices(events: List[Dict[str, Any]]) -> List[int]:
        return [idx for idx, event in enumerate(events) if event.get("type") == "book"]

    def _apply_flash_crash(
        self, events: List[Dict[str, Any]], drop_pct: float, volume_multiplier: float
    ) -> None:
        book_indices = self._book_indices(events)
        bar_indices = self._bar_indices(events)
        if not book_indices or not bar_indices:
            return

        centre = len(book_indices) // 2
        window = book_indices[max(0, centre - 2) : centre + 3]
        crash_timestamps = {events[idx]["timestamp"] for idx in window}

        for idx in window:
            event = events[idx]
            bid = event.get("bid")
            ask = event.get("ask")
            if bid is not None:
                event["bid"] = bid * (1.0 - drop_pct)
            if ask is not None:
                event["ask"] = ask * (1.0 - drop_pct * 1.05)
            event["bid_size"] = float(event.get("bid_size", 1.0)) * 0.4
            event["ask_size"] = float(event.get("ask_size", 1.0)) * 0.4

        for idx in bar_indices:
            event = events[idx]
            if event["timestamp"] not in crash_timestamps:
                continue
            self._scale_bar(event, 1.0 - drop_pct, volume_multiplier)

    def _apply_liquidity_halt(self, events: List[Dict[str, Any]], halt_length: int) -> None:
        if halt_length <= 0:
            return
        book_indices = self._book_indices(events)
        bar_indices = self._bar_indices(events)
        if not book_indices:
            return

        start = len(book_indices) // 3
        halt_window = book_indices[start : start + halt_length]
        halt_timestamps = {events[idx]["timestamp"] for idx in halt_window}

        for idx in halt_window:
            event = events[idx]
            event["bid_size"] = 0.0
            event["ask_size"] = 0.0
            event["halted"] = True

        for idx in book_indices:
            event = events[idx]
            if event["timestamp"] in halt_timestamps:
                event["halted"] = True

        for idx in bar_indices:
            bar = events[idx]
            if bar["timestamp"] in halt_timestamps:
                if "volume" in bar:
                    bar["volume"] = 0.0

    def _apply_gap_open(self, events: List[Dict[str, Any]], gap_pct: float) -> None:
        bar_indices = self._bar_indices(events)
        book_indices = self._book_indices(events)
        if not bar_indices:
            return
        first_bar = events[bar_indices[0]]
        factor = 1.0 + gap_pct
        reference_price = first_bar.get("open") or first_bar.get("close")
        if reference_price is None:
            return
        for key in ("open", "high", "low", "close"):
            if key in first_bar and first_bar[key] is not None:
                first_bar[key] = float(first_bar[key]) * factor
        if "volume" in first_bar:
            first_bar["volume"] = float(first_bar.get("volume", 0.0)) * 1.2

        first_timestamp = first_bar["timestamp"]
        for idx in book_indices:
            book = events[idx]
            if book["timestamp"] > first_timestamp:
                break
            bid = book.get("bid")
            ask = book.get("ask")
            if bid is not None:
                book["bid"] = bid * factor
            if ask is not None:
                book["ask"] = ask * factor

    def _apply_volatility_spike(
        self, events: List[Dict[str, Any]], spread_multiplier: float, volume_multiplier: float
    ) -> None:
        book_indices = self._book_indices(events)
        bar_indices = self._bar_indices(events)
        if not bar_indices:
            return
        start = max(0, len(bar_indices) // 2 - 1)
        vol_window = bar_indices[start : start + 4]
        vol_timestamps = {events[idx]["timestamp"] for idx in vol_window}

        for idx in vol_window:
            bar = events[idx]
            close = bar.get("close")
            high = bar.get("high")
            low = bar.get("low")
            if close is not None and high is not None:
                bar["high"] = close + abs(high - close) * spread_multiplier
            if close is not None and low is not None:
                bar["low"] = close - abs(close - low) * spread_multiplier
            if "volume" in bar:
                bar["volume"] = float(bar.get("volume", 0.0)) * volume_multiplier

        for idx in book_indices:
            book = events[idx]
            if book["timestamp"] not in vol_timestamps:
                continue
            bid = book.get("bid")
            ask = book.get("ask")
            if bid is not None and ask is not None and ask > bid:
                mid = (bid + ask) / 2.0
                spread = (ask - bid) * spread_multiplier
                book["bid"] = max(0.0, mid - spread / 2.0)
                book["ask"] = mid + spread / 2.0
            book["bid_size"] = float(book.get("bid_size", 1.0)) / max(volume_multiplier, 1e-6)
            book["ask_size"] = float(book.get("ask_size", 1.0)) / max(volume_multiplier, 1e-6)

    @staticmethod
    def _scale_bar(event: Dict[str, Any], price_factor: float, volume_multiplier: float) -> None:
        for key in ("open", "high", "low", "close"):
            if key in event and event[key] is not None:
                event[key] = float(event[key]) * price_factor
        if "volume" in event:
            event["volume"] = float(event.get("volume", 0.0)) * volume_multiplier

    # ------------------------------------------------------------------
    # Metrics helpers
    # ------------------------------------------------------------------
    def _build_metrics(self, metrics: Dict[str, Any]) -> StressMetrics:
        state = self._backtester.last_state
        if state is None:
            raise RuntimeError("Backtester state is unavailable; did you run a scenario?")
        pnl = float(metrics.get("net_pnl", 0.0))
        sharpe = float(metrics.get("sharpe", 0.0))
        fees = float(state.total_fee)
        slippage = float(metrics.get("slippage_attrib", 0.0))
        win_rate = self._win_rate_from_fills(state.fills)
        return StressMetrics(pnl=pnl, sharpe=sharpe, fees=fees, slippage=slippage, win_rate=win_rate)

    def _win_rate_from_fills(self, fills: Iterable[Fill]) -> float:
        realized = self._realized_trade_pnls(fills)
        if not realized:
            return 0.0
        wins = sum(1 for pnl in realized if pnl > 0)
        losses = sum(1 for pnl in realized if pnl < 0)
        total = wins + losses
        if total == 0:
            return 0.0
        return wins / total

    @staticmethod
    def _realized_trade_pnls(fills: Iterable[Fill]) -> List[float]:
        position = 0.0
        avg_entry = 0.0
        realized: List[float] = []

        for fill in sorted(fills, key=lambda item: item.timestamp):
            qty = float(fill.quantity)
            if qty <= 0:
                continue
            fee_per_unit = float(fill.fee) / qty if qty else 0.0
            if fill.side == "buy":
                net_price = float(fill.price) + fee_per_unit
                if position >= 0:
                    new_pos = position + qty
                    if new_pos != 0:
                        avg_entry = (avg_entry * position + net_price * qty) / new_pos
                    else:
                        avg_entry = 0.0
                    position = new_pos
                else:
                    closing = min(qty, -position)
                    if closing > 0:
                        realized.append(closing * (avg_entry - net_price))
                    position += closing
                    remaining = qty - closing
                    if position == 0:
                        avg_entry = 0.0
                    if remaining > 0:
                        position = remaining
                        avg_entry = net_price
            else:  # sell
                net_price = float(fill.price) - fee_per_unit
                if position <= 0:
                    new_pos = position - qty
                    abs_new_pos = abs(new_pos)
                    if abs_new_pos != 0:
                        avg_entry = (avg_entry * abs(position) + net_price * qty) / abs_new_pos
                    else:
                        avg_entry = 0.0
                    position = new_pos
                else:
                    closing = min(qty, position)
                    if closing > 0:
                        realized.append(closing * (net_price - avg_entry))
                    position -= closing
                    remaining = qty - closing
                    if position == 0:
                        avg_entry = 0.0
                    if remaining > 0:
                        position = -remaining
                        avg_entry = net_price

        return realized


def replay_from_ws_log(path: Path | str) -> List[Dict[str, Any]]:
    """Module level helper to align with historical usage patterns."""

    return StressEngine.replay_from_ws_log(path)

