"""Advanced market microstructure and risk signal service.

This FastAPI application bundles together a selection of advanced
microstructure analytics that we rely on for monitoring derivatives and
spot venues.  The handlers source market data directly from the
authoritative TimescaleDB-backed market-data store via pluggable adapters
so results reflect the latest production context while remaining fully
testable using recorded fixtures.
"""

from __future__ import annotations

import logging
import math
import os
import statistics
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Mapping, Sequence

import numpy as np
from fastapi import Depends, FastAPI, HTTPException, Query
from prometheus_client import Gauge
from pydantic import BaseModel

from services.analytics.market_data_store import (
    MarketDataAdapter,
    MarketDataUnavailable,
    TimescaleMarketDataAdapter,
    Trade,
)
from services.common import security
from services.common.security import require_admin_account


logger = logging.getLogger(__name__)


DATA_STALENESS_GAUGE = Gauge(
    "signal_service_data_age_seconds",
    "Age of the market data powering signal service computations.",
    ["symbol", "feed"],
)


# ---------------------------------------------------------------------------
# Helper computations
# ---------------------------------------------------------------------------


def _coerce_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _ensure_fresh(symbol: str, feed: str, observed_at: datetime | None, max_age: timedelta) -> None:
    if observed_at is None:
        detail = f"{feed} feed unavailable for {symbol.upper()}"
        logger.warning(detail)
        raise HTTPException(status_code=503, detail=detail)

    observed = observed_at.astimezone(timezone.utc)
    age_seconds = max(0.0, (datetime.now(timezone.utc) - observed).total_seconds())
    DATA_STALENESS_GAUGE.labels(symbol=symbol.upper(), feed=feed).set(age_seconds)

    if age_seconds > max_age.total_seconds():
        detail = f"{feed} feed stale for {symbol.upper()} ({int(age_seconds)}s old)"
        logger.warning(detail)
        raise HTTPException(status_code=503, detail=detail)


def _order_flow_metrics(trades: Sequence[Trade]) -> Dict[str, float]:
    if not trades:
        raise HTTPException(status_code=422, detail="No trades available for the requested window")

    buy_volume = sum(t.volume for t in trades if t.side == "buy")
    sell_volume = sum(t.volume for t in trades if t.side == "sell")
    total_volume = buy_volume + sell_volume
    imbalance = 0.0 if total_volume == 0 else (buy_volume - sell_volume) / total_volume

    volumes = [t.volume for t in trades]
    mean_volume = statistics.fmean(volumes)
    std_volume = statistics.pstdev(volumes) if len(volumes) > 1 else 0.0
    whale_threshold = mean_volume + 3 * std_volume if std_volume else mean_volume * 2.5
    whale_ratio = sum(1 for t in trades if t.volume >= whale_threshold) / len(trades)

    return {
        "buy_volume": round(buy_volume, 6),
        "sell_volume": round(sell_volume, 6),
        "imbalance": round(imbalance, 6),
        "whale_threshold": round(whale_threshold, 6),
        "whale_ratio": round(whale_ratio, 6),
    }


def _queue_depth_anomalies(order_book: Mapping[str, Sequence[Sequence[float]]]) -> Dict[str, object]:
    bids = list(order_book.get("bids", []))
    asks = list(order_book.get("asks", []))
    if not bids or not asks:
        raise HTTPException(status_code=422, detail="Order book missing bids or asks")

    top_depth_bid = sum(level[1] for level in bids[:3])
    top_depth_ask = sum(level[1] for level in asks[:3])
    depth_imbalance = 0.0
    if top_depth_bid + top_depth_ask:
        depth_imbalance = (top_depth_bid - top_depth_ask) / (top_depth_bid + top_depth_ask)

    anomalies: List[Dict[str, float]] = []
    for side_name, levels in ("bid", bids), ("ask", asks):
        for idx in range(1, len(levels)):
            prev = levels[idx - 1][1]
            curr = levels[idx][1]
            if prev <= 0:
                continue
            drop = (prev - curr) / prev
            if drop > 0.55:
                anomalies.append(
                    {
                        "side": side_name,
                        "level": idx + 1,
                        "drop_ratio": round(drop, 6),
                        "size": curr,
                    }
                )

    queue_skew = 0.0
    if bids and asks:
        queue_skew = (bids[0][1] - asks[0][1]) / max(bids[0][1] + asks[0][1], 1e-9)

    anomaly_score = min(1.0, max((abs(depth_imbalance) + len(anomalies) * 0.1) / 2, 0))

    return {
        "top_bid_depth": round(top_depth_bid, 6),
        "top_ask_depth": round(top_depth_ask, 6),
        "depth_imbalance": round(depth_imbalance, 6),
        "queue_skew": round(queue_skew, 6),
        "anomaly_score": round(anomaly_score, 6),
        "liquidity_gaps": anomalies,
    }


def _pearson(series_a: Sequence[float], series_b: Sequence[float]) -> float:
    if len(series_a) != len(series_b) or len(series_a) < 2:
        raise HTTPException(status_code=422, detail="Series lengths must match and be >= 2")
    mean_a = statistics.fmean(series_a)
    mean_b = statistics.fmean(series_b)
    cov = sum((a - mean_a) * (b - mean_b) for a, b in zip(series_a, series_b))
    var_a = sum((a - mean_a) ** 2 for a in series_a)
    var_b = sum((b - mean_b) ** 2 for b in series_b)
    if var_a == 0 or var_b == 0:
        raise HTTPException(status_code=422, detail="Zero variance encountered in correlation")
    return cov / math.sqrt(var_a * var_b)


def _lag(series_a: Sequence[float], series_b: Sequence[float], max_lag: int) -> int:
    best_lag = 0
    best_corr = float("-inf")
    for lag in range(-max_lag, max_lag + 1):
        if lag < 0:
            a = series_a[: lag or None]
            b = series_b[-lag:]
        elif lag > 0:
            a = series_a[lag:]
            b = series_b[: -lag or None]
        else:
            a = series_a
            b = series_b
        if len(a) < 2 or len(b) < 2:
            continue
        corr = _pearson(a, b)
        if corr > best_corr:
            best_corr = corr
            best_lag = lag
    return best_lag


def _rolling_beta(series_alt: Sequence[float], series_base: Sequence[float], window: int) -> float:
    if len(series_alt) < window or len(series_base) < window:
        raise HTTPException(status_code=422, detail=f"Need at least {window} points for beta")
    alt_window = series_alt[-window:]
    base_window = series_base[-window:]
    mean_alt = statistics.fmean(alt_window)
    mean_base = statistics.fmean(base_window)
    covariance = sum((a - mean_alt) * (b - mean_base) for a, b in zip(alt_window, base_window))
    variance = sum((b - mean_base) ** 2 for b in base_window)
    if variance == 0:
        raise HTTPException(status_code=422, detail="Base series variance is zero")
    return covariance / variance


def _garch_forecast(prices: Sequence[float], horizon: int = 12) -> Dict[str, object]:
    if len(prices) < 30:
        raise HTTPException(status_code=422, detail="Need at least 30 observations for GARCH")
    log_returns = np.diff(np.log(np.array(prices)))
    if log_returns.size < 1:
        raise HTTPException(status_code=422, detail="Insufficient returns for volatility forecasting")

    alpha = 0.07
    beta = 0.9
    variance = float(np.var(log_returns))
    omega = variance * (1 - alpha - beta)
    if omega <= 0:
        omega = variance * 0.05

    sigma2 = variance
    forecasts: List[float] = []
    for ret in log_returns[-5:]:
        sigma2 = omega + alpha * ret**2 + beta * sigma2
    for _ in range(horizon):
        sigma2 = omega + (alpha + beta) * sigma2
        forecasts.append(math.sqrt(max(sigma2, 0)))

    window = min(20, log_returns.size)
    recent = log_returns[-window:]
    mean = float(np.mean(recent))
    std = float(np.std(recent)) or 1e-6
    jumps = []
    for idx, ret in enumerate(log_returns[-horizon:], start=len(log_returns) - horizon):
        z_score = (ret - mean) / std
        if abs(z_score) > 4:
            jumps.append({"index": idx + 1, "return": float(ret), "z_score": round(float(z_score), 6)})

    return {
        "variance": round(variance, 10),
        "forecasts": [round(float(v), 10) for v in forecasts],
        "jump_events": jumps,
    }


def _detect_whales(trades: Sequence[Trade], threshold_sigma: float) -> Dict[str, object]:
    if not trades:
        raise HTTPException(status_code=422, detail="No trades available for whale detection")
    volumes = [t.volume for t in trades]
    mean = statistics.fmean(volumes)
    std = statistics.pstdev(volumes) if len(volumes) > 1 else 0.0
    if std == 0:
        std = mean * 0.1
    threshold = mean + threshold_sigma * std
    whales = [t for t in trades if t.volume >= threshold]
    payload = [
        {
            "side": trade.side,
            "volume": trade.volume,
            "price": trade.price,
            "ts": trade.ts,
            "sigma": round((trade.volume - mean) / std if std else 0.0, 6),
        }
        for trade in whales
    ]
    return {
        "threshold_volume": round(threshold, 6),
        "count": len(payload),
        "share_of_trades": round(len(payload) / len(trades), 6),
        "trades": payload,
    }


def _stress_test(symbol: str, prices: Sequence[float], order_book: Mapping[str, Sequence[Sequence[float]]]) -> Dict[str, object]:
    current_price = prices[-1]
    flash_crash_price = round(current_price * 0.82, 6)
    spread_widen_price = round(current_price * 0.97, 6)

    bids = list(order_book.get("bids", []))
    asks = list(order_book.get("asks", []))
    total_bid_depth = sum(level[1] for level in bids)
    total_ask_depth = sum(level[1] for level in asks)

    flash_crash_liquidity = {
        "price": flash_crash_price,
        "expected_slippage": round((current_price - flash_crash_price) / current_price, 6),
        "depth_absorption": round(min(total_bid_depth, total_ask_depth * 1.4), 6),
    }

    spread_widening = {
        "price": spread_widen_price,
        "new_spread": round((asks[0][0] - bids[0][0]) * 3, 6) if bids and asks else None,
        "depth_reduction": round(total_bid_depth * 0.35 + total_ask_depth * 0.35, 6),
    }

    return {
        "symbol": symbol,
        "flash_crash": flash_crash_liquidity,
        "spread_widening": spread_widening,
    }


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------


class OrderFlowResponse(BaseModel):
    symbol: str
    window: int
    buy_volume: float
    sell_volume: float
    imbalance: float
    whale_threshold: float
    whale_ratio: float
    queue_skew: float
    depth_imbalance: float
    anomaly_score: float
    liquidity_gaps: List[Dict[str, float]]
    ts: datetime


class CrossAssetResponse(BaseModel):
    base_symbol: str
    alt_symbol: str
    beta: float
    correlation: float
    lead_lag: int
    ts: datetime


class VolatilityResponse(BaseModel):
    symbol: str
    variance: float
    forecasts: List[float]
    jump_events: List[Dict[str, float]]
    ts: datetime


class WhaleResponse(BaseModel):
    symbol: str
    threshold_volume: float
    count: int
    share_of_trades: float
    trades: List[Dict[str, object]]
    ts: datetime


class StressTestResponse(BaseModel):
    symbol: str
    flash_crash: Dict[str, float]
    spread_widening: Dict[str, float | None]
    ts: datetime


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


app = FastAPI(title="Advanced Signal Service", version="1.0.0")



_PRIMARY_DSN_ENV = "SIGNAL_DATABASE_URL"
_FALLBACK_DSN_ENV = "TIMESCALE_DSN"
_PRIMARY_SCHEMA_ENV = "SIGNAL_SCHEMA"
_FALLBACK_SCHEMA_ENV = "TIMESCALE_SCHEMA"


def _resolve_market_data_dsn() -> str:
    dsn = os.getenv(_PRIMARY_DSN_ENV) or os.getenv(_FALLBACK_DSN_ENV)
    if not dsn:
        raise RuntimeError(
            "SIGNAL_DATABASE_URL or TIMESCALE_DSN must be configured with a PostgreSQL/Timescale DSN for the signal service."
        )
    return dsn


def _resolve_market_data_schema() -> str | None:
    return os.getenv(_PRIMARY_SCHEMA_ENV) or os.getenv(_FALLBACK_SCHEMA_ENV)


@app.on_event("startup")
def _configure_market_data_adapter() -> None:
    dsn = _resolve_market_data_dsn()
    schema = _resolve_market_data_schema()
    app.state.market_data_adapter = TimescaleMarketDataAdapter(database_url=dsn, schema=schema)


@app.on_event("shutdown")
def _reset_market_data_adapter() -> None:
    if hasattr(app.state, "market_data_adapter"):
        adapter = getattr(app.state, "market_data_adapter", None)
        if adapter is not None:
            engine = getattr(adapter, "_engine", None)
            if engine is not None:
                try:
                    engine.dispose()
                except Exception:  # pragma: no cover - defensive cleanup
                    logger.debug("Failed to dispose Timescale engine during shutdown", exc_info=True)
        app.state.market_data_adapter = None



def _market_data_adapter() -> MarketDataAdapter:
    adapter = getattr(app.state, "market_data_adapter", None)
    if adapter is None:
        raise HTTPException(status_code=503, detail="Market data adapter is not configured")
    return adapter


@app.get("/signals/orderflow/{symbol}", response_model=OrderFlowResponse)
def order_flow_signals(
    symbol: str,
    window: int = Query(300, ge=60, le=3600),
    _caller: str = Depends(require_admin_account),
) -> OrderFlowResponse:
    adapter = _market_data_adapter()
    try:
        trades = adapter.recent_trades(symbol, window=window)
        order_book = adapter.order_book_snapshot(symbol)
    except MarketDataUnavailable as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    order_flow = _order_flow_metrics(trades)
    queue = _queue_depth_anomalies(order_book)

    latest_trade_ts = max((trade.ts for trade in trades), default=None)
    _ensure_fresh(symbol, "trades", latest_trade_ts, timedelta(seconds=window * 2))
    book_ts = _coerce_datetime(order_book.get("as_of"))
    _ensure_fresh(symbol, "order_book", book_ts, timedelta(seconds=max(60, window // 2)))

    ts = datetime.now(timezone.utc)
    return OrderFlowResponse(
        symbol=symbol,
        window=window,
        buy_volume=order_flow["buy_volume"],
        sell_volume=order_flow["sell_volume"],
        imbalance=order_flow["imbalance"],
        whale_threshold=order_flow["whale_threshold"],
        whale_ratio=order_flow["whale_ratio"],
        queue_skew=queue["queue_skew"],
        depth_imbalance=queue["depth_imbalance"],
        anomaly_score=queue["anomaly_score"],
        liquidity_gaps=queue["liquidity_gaps"],
        ts=ts,
    )


@app.get("/signals/crossasset", response_model=CrossAssetResponse)
def cross_asset_signals(
    base_symbol: str,
    alt_symbol: str,
    window: int = Query(180, ge=30, le=720),
    max_lag: int = Query(10, ge=1, le=50),
    _caller: str = Depends(require_admin_account),
) -> CrossAssetResponse:
    adapter = _market_data_adapter()
    try:
        base_series = adapter.price_history(base_symbol, length=window)
        base_ts = adapter.latest_price_timestamp(base_symbol)
        alt_series = adapter.price_history(alt_symbol, length=window)
        alt_ts = adapter.latest_price_timestamp(alt_symbol)
    except MarketDataUnavailable as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    _ensure_fresh(base_symbol, "prices", base_ts, timedelta(hours=6))
    _ensure_fresh(alt_symbol, "prices", alt_ts, timedelta(hours=6))

    beta = _rolling_beta(alt_series, base_series, window=min(window, 120))
    correlation = _pearson(base_series, alt_series)
    lag = _lag(base_series, alt_series, max_lag=max_lag)
    ts = datetime.now(timezone.utc)
    return CrossAssetResponse(
        base_symbol=base_symbol,
        alt_symbol=alt_symbol,
        beta=round(beta, 6),
        correlation=round(correlation, 6),
        lead_lag=lag,
        ts=ts,
    )


@app.get("/signals/volatility/{symbol}", response_model=VolatilityResponse)
def volatility_signals(
    symbol: str,
    window: int = Query(240, ge=60, le=960),
    horizon: int = Query(12, ge=1, le=60),
    _caller: str = Depends(require_admin_account),
) -> VolatilityResponse:
    adapter = _market_data_adapter()
    try:
        prices = adapter.price_history(symbol, length=window)
        price_ts = adapter.latest_price_timestamp(symbol)
    except MarketDataUnavailable as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    _ensure_fresh(symbol, "prices", price_ts, timedelta(hours=6))

    garch = _garch_forecast(prices, horizon=horizon)
    ts = datetime.now(timezone.utc)
    return VolatilityResponse(
        symbol=symbol,
        variance=garch["variance"],
        forecasts=garch["forecasts"],
        jump_events=garch["jump_events"],
        ts=ts,
    )


@app.get("/signals/whales/{symbol}", response_model=WhaleResponse)
def whale_signals(
    symbol: str,
    window: int = Query(900, ge=120, le=7200),
    threshold_sigma: float = Query(2.5, ge=1.0, le=6.0),
    _caller: str = Depends(require_admin_account),
) -> WhaleResponse:
    adapter = _market_data_adapter()
    try:
        trades = adapter.recent_trades(symbol, window=window)
    except MarketDataUnavailable as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    latest_trade_ts = max((trade.ts for trade in trades), default=None)
    _ensure_fresh(symbol, "trades", latest_trade_ts, timedelta(seconds=window * 2))

    whales = _detect_whales(trades, threshold_sigma)
    ts = datetime.now(timezone.utc)
    return WhaleResponse(
        symbol=symbol,
        threshold_volume=whales["threshold_volume"],
        count=whales["count"],
        share_of_trades=whales["share_of_trades"],
        trades=whales["trades"],
        ts=ts,
    )


@app.get("/signals/stress/{symbol}", response_model=StressTestResponse)
def stress_test_signals(
    symbol: str,
    window: int = Query(240, ge=60, le=960),
    _caller: str = Depends(require_admin_account),
) -> StressTestResponse:
    adapter = _market_data_adapter()
    try:
        prices = adapter.price_history(symbol, length=window)
        price_ts = adapter.latest_price_timestamp(symbol)
        book = adapter.order_book_snapshot(symbol)
    except MarketDataUnavailable as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    _ensure_fresh(symbol, "prices", price_ts, timedelta(hours=6))
    book_ts = _coerce_datetime(book.get("as_of"))
    _ensure_fresh(symbol, "order_book", book_ts, timedelta(minutes=10))

    stress = _stress_test(symbol, prices, book)
    ts = datetime.now(timezone.utc)
    return StressTestResponse(
        symbol=stress["symbol"],
        flash_crash=stress["flash_crash"],
        spread_widening=stress["spread_widening"],
        ts=ts,
    )


__all__ = [
    "app",
    "order_flow_signals",
    "cross_asset_signals",
    "volatility_signals",
    "whale_signals",
    "stress_test_signals",
]
