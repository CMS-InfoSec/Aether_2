"""Advanced market microstructure and risk signal service.

This FastAPI application bundles together a selection of advanced
microstructure analytics that we rely on for monitoring derivatives and
spot venues.  The implementation is intentionally self-contained and uses
synthetic-yet-deterministic market data so the service remains functional
in development and unit test environments without external market data
feeds.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Synthetic market data generation
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Trade:
    """Lightweight trade representation used by the simulator."""

    side: str
    volume: float
    price: float
    ts: datetime


class MarketDataSimulator:
    """Produce deterministic synthetic market data series for analytics."""

    def __init__(self) -> None:
        self._trade_cache: MutableMapping[tuple[str, int], List[Trade]] = {}
        self._price_cache: MutableMapping[tuple[str, int], List[float]] = {}

    @staticmethod
    def _rng_seed(symbol: str) -> int:
        return sum(ord(char) for char in symbol.upper())

    def _rng(self, symbol: str, window: int) -> np.random.Generator:
        seed = self._rng_seed(symbol) + window * 7919
        return np.random.default_rng(seed)

    def price_series(self, symbol: str, length: int = 240) -> List[float]:
        cache_key = (symbol, length)
        if cache_key in self._price_cache:
            return self._price_cache[cache_key]

        rng = self._rng(symbol, length)
        base_level = 50 + (self._rng_seed(symbol) % 200)
        prices: List[float] = []
        price = float(base_level)
        for idx in range(length):
            seasonal = math.sin(idx / 18 + base_level / 17) * 0.012
            drift = math.cos(idx / 80 + base_level / 23) * 0.004
            noise = rng.normal(0, 0.006)
            price *= 1 + seasonal + drift + noise
            price = max(price, 0.5)
            prices.append(round(price, 6))
        self._price_cache[cache_key] = prices
        return prices

    def correlated_price_series(
        self, base_symbol: str, alt_symbol: str, length: int = 240
    ) -> tuple[List[float], List[float]]:
        base = self.price_series(base_symbol, length)
        rng = self._rng(alt_symbol, length)
        noise = rng.normal(0, 0.5, size=length)
        lag = (self._rng_seed(alt_symbol) % 5) - 2
        alt = []
        for idx in range(length):
            base_idx = min(max(idx - lag, 0), length - 1)
            base_price = base[base_idx]
            adjustment = 1 + noise[idx] / 100
            alt.append(round(base_price * adjustment, 6))
        return base, alt

    def trades(self, symbol: str, window: int = 600) -> List[Trade]:
        cache_key = (symbol, window)
        if cache_key in self._trade_cache:
            return self._trade_cache[cache_key]

        rng = self._rng(symbol, window)
        base_price = self.price_series(symbol, length=1)[-1]
        now = datetime.now(timezone.utc)
        trades: List[Trade] = []
        count = max(60, min(600, window // 5))
        for idx in range(count):
            side = "buy" if rng.random() < 0.5 + math.sin(idx / 30) * 0.05 else "sell"
            volume = float(max(0.01, rng.gamma(2.0, 1.5)))
            price = float(max(0.1, base_price * (1 + rng.normal(0, 0.0015))))
            ts = now - timedelta(seconds=window - idx * window / count)
            trades.append(Trade(side=side, volume=round(volume, 6), price=round(price, 6), ts=ts))
        self._trade_cache[cache_key] = trades
        return trades

    def order_book(self, symbol: str, levels: int = 10) -> Dict[str, List[List[float]]]:
        rng = self._rng(symbol, levels)
        base_price = self.price_series(symbol, length=1)[-1]
        spread = max(0.1, base_price * 0.0015)
        bids: List[List[float]] = []
        asks: List[List[float]] = []
        depth_scale = 1 + (self._rng_seed(symbol) % 7) / 5
        for level in range(levels):
            decay = 0.9 ** level
            bid_price = round(base_price - spread * (level + 1), 6)
            ask_price = round(base_price + spread * (level + 1), 6)
            bid_size = round(max(0.05, rng.uniform(0.2, 5.0) * depth_scale * decay), 6)
            ask_size = round(max(0.05, rng.uniform(0.2, 5.0) * depth_scale * decay), 6)
            bids.append([bid_price, bid_size])
            asks.append([ask_price, ask_size])
        return {"bids": bids, "asks": asks}


SIMULATOR = MarketDataSimulator()


# ---------------------------------------------------------------------------
# Helper computations
# ---------------------------------------------------------------------------


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


@app.get("/signals/orderflow/{symbol}", response_model=OrderFlowResponse)
def order_flow_signals(symbol: str, window: int = Query(300, ge=60, le=3600)) -> OrderFlowResponse:
    trades = SIMULATOR.trades(symbol, window=window)
    order_book = SIMULATOR.order_book(symbol)
    order_flow = _order_flow_metrics(trades)
    queue = _queue_depth_anomalies(order_book)
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
) -> CrossAssetResponse:
    base_series, alt_series = SIMULATOR.correlated_price_series(base_symbol, alt_symbol, length=window)
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
) -> VolatilityResponse:
    prices = SIMULATOR.price_series(symbol, length=window)
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
) -> WhaleResponse:
    trades = SIMULATOR.trades(symbol, window=window)
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
def stress_test_signals(symbol: str, window: int = Query(240, ge=60, le=960)) -> StressTestResponse:
    prices = SIMULATOR.price_series(symbol, length=window)
    book = SIMULATOR.order_book(symbol)
    stress = _stress_test(symbol, prices, book)
    ts = datetime.now(timezone.utc)
    return StressTestResponse(symbol=stress["symbol"], flash_crash=stress["flash_crash"], spread_widening=stress["spread_widening"], ts=ts)


__all__ = [
    "app",
    "order_flow_signals",
    "cross_asset_signals",
    "volatility_signals",
    "whale_signals",
    "stress_test_signals",
]
