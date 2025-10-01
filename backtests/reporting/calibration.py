"""Calibration utilities for backtest KPI generation."""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable, Dict, Iterable, List, Optional


@dataclass
class MetricResult:
    name: str
    value: float
    timestamp: datetime


def compute_sharpe(returns: Iterable[float], risk_free_rate: float = 0.0) -> float:
    series = list(returns)
    if not series:
        return 0.0
    excess = [r - risk_free_rate for r in series]
    mean = sum(excess) / len(excess)
    variance = sum((r - mean) ** 2 for r in excess) / len(excess)
    std = math.sqrt(variance) if variance > 0 else 0.0
    if std == 0:
        return 0.0
    return mean / std * math.sqrt(252)


def compute_sortino(returns: Iterable[float], risk_free_rate: float = 0.0) -> float:
    series = list(returns)
    if not series:
        return 0.0
    downside = [min(0.0, r - risk_free_rate) for r in series]
    downside_var = sum(d ** 2 for d in downside) / len(series)
    downside_std = math.sqrt(downside_var) if downside_var > 0 else 0.0
    if downside_std == 0:
        return 0.0
    mean_excess = sum(series) / len(series) - risk_free_rate
    return mean_excess / downside_std * math.sqrt(252)


def compute_cvar(returns: Iterable[float], alpha: float = 0.95) -> float:
    series = sorted(returns)
    if not series:
        return 0.0
    cutoff = int((1 - alpha) * len(series))
    cutoff = max(1, cutoff)
    tail = series[:cutoff]
    return sum(tail) / len(tail)


def compute_max_drawdown(equity_curve: Iterable[float]) -> float:
    curve = list(equity_curve)
    if not curve:
        return 0.0
    peak = curve[0]
    max_dd = 0.0
    for value in curve:
        peak = max(peak, value)
        drawdown = (value - peak) / peak if peak else 0.0
        max_dd = min(max_dd, drawdown)
    return max_dd


def compute_fee_attribution(fills: Iterable[Dict[str, float]]) -> Dict[str, float]:
    fees = {"maker": 0.0, "taker": 0.0}
    for fill in fills:
        side = fill.get("liquidity_flag", "taker")
        fees[side] = fees.get(side, 0.0) + fill.get("fee", 0.0)
    return fees


class TimescaleReporter:
    """Very small TimescaleDB writer used for KPI persistence."""

    def __init__(self, connection_factory: Callable[[], object]) -> None:
        self.connection_factory = connection_factory

    def persist(self, metrics: List[MetricResult], table: str = "backtest_metrics") -> None:
        conn = self.connection_factory()
        cursor = conn.cursor()
        try:
            for metric in metrics:
                cursor.execute(
                    """
                    INSERT INTO {table}(timestamp, name, value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (timestamp, name) DO UPDATE SET value = EXCLUDED.value
                    """.format(table=table),
                    (metric.timestamp, metric.name, metric.value),
                )
            conn.commit()
        finally:
            cursor.close()
            conn.close()


def generate_kpis(
    returns: Iterable[float],
    equity_curve: Iterable[float],
    fills: Iterable[Dict[str, float]],
    risk_free_rate: float = 0.0,
) -> Dict[str, float]:
    """Return dictionary of KPIs computed from a backtest run."""

    sharpe = compute_sharpe(returns, risk_free_rate)
    sortino = compute_sortino(returns, risk_free_rate)
    cvar = compute_cvar(returns)
    drawdown = compute_max_drawdown(equity_curve)
    fee_attr = compute_fee_attribution(fills)
    metrics = {
        "sharpe": sharpe,
        "sortino": sortino,
        "cvar": cvar,
        "max_drawdown": drawdown,
    }
    metrics.update({f"fee_{k}": v for k, v in fee_attr.items()})
    return metrics


def persist_kpis(
    reporter: TimescaleReporter,
    metrics: Dict[str, float],
    timestamp: Optional[datetime] = None,
) -> None:
    now = timestamp or datetime.now(UTC)
    payload = [MetricResult(name=key, value=value, timestamp=now) for key, value in metrics.items()]
    reporter.persist(payload)
