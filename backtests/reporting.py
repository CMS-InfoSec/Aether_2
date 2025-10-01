"""Backtest reporting utilities for KPI generation and persistence."""
from __future__ import annotations

import io
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, Dict, Iterable, List, Optional


@dataclass
class MetricResult:
    """Container for KPI persistence in TimescaleDB."""

    name: str
    value: float
    timestamp: datetime


def _to_series(values: Iterable[float]) -> List[float]:
    series = [float(v) for v in values]
    return series


def compute_sharpe(returns: Iterable[float], risk_free_rate: float = 0.0) -> float:
    series = _to_series(returns)
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
    series = _to_series(returns)
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
    series = sorted(_to_series(returns))
    if not series:
        return 0.0
    cutoff = int((1 - alpha) * len(series))
    cutoff = max(1, cutoff)
    tail = series[:cutoff]
    return sum(tail) / len(tail)


def compute_max_drawdown(equity_curve: Iterable[float]) -> float:
    curve = _to_series(equity_curve)
    if not curve:
        return 0.0
    peak = curve[0]
    max_dd = 0.0
    for value in curve:
        peak = max(peak, value)
        drawdown = (value - peak) / peak if peak else 0.0
        max_dd = min(max_dd, drawdown)
    return max_dd


def compute_turnover(fills: Iterable[Dict[str, float]]) -> float:
    turnover = 0.0
    for fill in fills:
        turnover += abs(fill.get("price", 0.0) * fill.get("quantity", 0.0))
    return turnover


def compute_fee_attribution(fills: Iterable[Dict[str, float]]) -> Dict[str, float]:
    fees: Dict[str, float] = {"maker": 0.0, "taker": 0.0}
    for fill in fills:
        side = fill.get("liquidity", fill.get("liquidity_flag", "taker"))
        fees[side] = fees.get(side, 0.0) + fill.get("fee", 0.0)
    return fees


def generate_kpis(
    returns: Iterable[float],
    equity_curve: Iterable[float],
    fills: Iterable[Dict[str, float]],
    risk_free_rate: float = 0.0,
) -> Dict[str, float]:
    """Aggregate KPI dictionary from backtest outputs."""

    fills_list = list(fills)
    metrics = {
        "sharpe": compute_sharpe(returns, risk_free_rate),
        "sortino": compute_sortino(returns, risk_free_rate),
        "cvar": compute_cvar(returns),
        "max_drawdown": compute_max_drawdown(equity_curve),
        "turnover": compute_turnover(fills_list),
    }
    metrics.update({f"fee_{k}": v for k, v in compute_fee_attribution(fills_list).items()})
    return metrics


class TimescaleReporter:
    """Simple TimescaleDB reporter for KPI metrics."""

    def __init__(self, connection_factory: Callable[[], Any]) -> None:
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


class ObjectStorageReporter:
    """Adapter for persisting payloads to object storage."""

    def __init__(self, client_factory: Callable[[], Any], serializer: Optional[Callable[[Dict[str, Any]], bytes]] = None) -> None:
        self.client_factory = client_factory
        self.serializer = serializer or (lambda payload: json.dumps(payload, default=str).encode("utf-8"))

    def persist(self, bucket: str, key: str, payload: Dict[str, Any]) -> None:
        body = self.serializer(payload)
        client = self.client_factory()
        if hasattr(client, "put_object"):
            client.put_object(Bucket=bucket, Key=key, Body=body)
        elif hasattr(client, "upload_fileobj"):
            with io.BytesIO(body) as buffer:
                client.upload_fileobj(buffer, bucket, key)
        else:
            raise AttributeError("Object storage client must support put_object or upload_fileobj.")


def persist_metrics(
    reporter: TimescaleReporter,
    metrics: Dict[str, float],
    table: str = "backtest_metrics",
    timestamp: Optional[datetime] = None,
) -> None:
    snapshot_time = timestamp or datetime.now(UTC)
    payload = [MetricResult(name=key, value=value, timestamp=snapshot_time) for key, value in metrics.items()]
    reporter.persist(payload, table=table)


def persist_report(
    metrics: Dict[str, float],
    trade_log: Iterable[Dict[str, Any]],
    timescale_reporter: Optional[TimescaleReporter] = None,
    object_reporter: Optional[ObjectStorageReporter] = None,
    object_bucket: Optional[str] = None,
    object_key: Optional[str] = None,
    table: str = "backtest_metrics",
    timestamp: Optional[datetime] = None,
) -> None:
    """Persist metrics and trade logs to configured sinks."""

    if timescale_reporter is not None:
        persist_metrics(timescale_reporter, metrics, table=table, timestamp=timestamp)
    if object_reporter is not None:
        if not object_bucket or not object_key:
            raise ValueError("Object storage bucket and key must be provided when persisting trade logs.")
        payload = {
            "timestamp": (timestamp or datetime.now(UTC)),
            "metrics": metrics,
            "trades": list(trade_log),
        }
        object_reporter.persist(object_bucket, object_key, payload)


__all__ = [
    "MetricResult",
    "TimescaleReporter",
    "ObjectStorageReporter",
    "compute_sharpe",
    "compute_sortino",
    "compute_cvar",
    "compute_max_drawdown",
    "compute_turnover",
    "compute_fee_attribution",
    "generate_kpis",
    "persist_metrics",
    "persist_report",
]
