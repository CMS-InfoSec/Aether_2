"""Automated feature discovery and promotion pipeline.

This module periodically scans market data stored in TimescaleDB, engineers
candidate alpha features, evaluates their predictive utility using LightGBM,
and promotes the strongest signals into the Feast feature store. Every
evaluation cycle is logged to ``feature_evolution_log`` so modelers can audit
which features were considered and when they were deployed.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import time
import dataclasses
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:  # Optional dependency – enforced at runtime.
    import lightgbm as lgb
except Exception:  # pragma: no cover - executed when LightGBM is unavailable.
    lgb = None  # type: ignore

try:  # Optional dependency – enforced at runtime when Feast integration is used.
    from feast import FeatureStore, Field, FeatureView
    from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
        PostgreSQLSource,
    )
    from feast.types import Float32
except Exception:  # pragma: no cover - executed when Feast is unavailable.
    FeatureStore = None  # type: ignore
    Field = None  # type: ignore
    FeatureView = None  # type: ignore
    PostgreSQLSource = None  # type: ignore
    Float32 = None  # type: ignore

try:  # Optional dependency – enforced at runtime.
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - executed when psycopg is unavailable.
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class FeatureCandidate:
    """Container describing an engineered feature candidate."""

    name: str
    values: pd.Series
    score: float | None = None
    sharpe: float | None = None
    accuracy: float | None = None

    def as_dataframe(self) -> pd.DataFrame:
        """Return the candidate values as a tidy dataframe."""

        df = self.values.reset_index()
        df.columns = ["symbol", "event_ts", self.name]
        return df


@dataclass(frozen=True)
class FeatureDiscoveryConfig:
    """Configuration options for the feature discovery engine."""

    timescale_dsn: str
    feast_repo: Optional[str] = None
    ohlcv_table: str = "market_data.ohlcv"
    order_book_table: str = "market_data.order_book"
    symbol_column: str = "symbol"
    timestamp_column: str = "event_ts"
    lookback: timedelta = timedelta(days=30)
    scan_interval: timedelta = timedelta(hours=1)
    min_observations: int = 500
    max_features: int = 10
    feature_prefix: str = "auto_feature"


class FeatureDiscoveryEngine:
    """Coordinates the end-to-end feature discovery workflow."""

    def __init__(self, config: FeatureDiscoveryConfig) -> None:
        self.config = config
        if psycopg is None:  # pragma: no cover - enforced in runtime environments.
            raise RuntimeError(
                "psycopg is required for auto feature discovery but is not installed."
            )
        if lgb is None:  # pragma: no cover - enforced in runtime environments.
            raise RuntimeError(
                "LightGBM is required for auto feature discovery but is not installed."
            )
        self._ensure_support_tables()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run_once(self) -> None:
        """Execute a single discovery iteration."""

        LOGGER.info("Starting auto-feature discovery cycle")
        market_data = self._load_market_data()
        if market_data.empty:
            LOGGER.warning("No market data available for feature discovery; skipping cycle")
            return

        candidates = list(self._generate_candidates(market_data))
        LOGGER.info("Generated %d feature candidates", len(candidates))
        if not candidates:
            return

        target = self._derive_target(market_data)
        scored_candidates = self._score_candidates(candidates, target)
        ranked = sorted(
            scored_candidates,
            key=lambda candidate: candidate.score if candidate.score is not None else -np.inf,
            reverse=True,
        )
        promoted = ranked[: self.config.max_features]
        LOGGER.info("Promoting top %d features to Feast", len(promoted))
        self._persist_candidates(ranked)
        self._promote_features(promoted)
        self._log_promotions(ranked, promoted)

    def run_forever(self) -> None:
        """Continuously execute discovery cycles at the configured interval."""

        while True:
            cycle_start = time.monotonic()
            try:
                self.run_once()
            except Exception:  # pragma: no cover - runtime safety.
                LOGGER.exception("Auto-feature discovery cycle failed")
            elapsed = timedelta(seconds=time.monotonic() - cycle_start)
            sleep_for = max(self.config.scan_interval - elapsed, timedelta())
            if sleep_for > timedelta():
                LOGGER.debug("Sleeping for %s before next cycle", sleep_for)
                time.sleep(sleep_for.total_seconds())

    # ------------------------------------------------------------------
    # Data access helpers
    # ------------------------------------------------------------------
    def _connect(self) -> "psycopg.Connection[dict]":
        assert psycopg is not None  # nosec - validated in __init__
        return psycopg.connect(self.config.timescale_dsn, row_factory=dict_row)

    def _ensure_support_tables(self) -> None:
        """Ensure helper tables exist for storing discovered feature metadata."""

        create_feature_values = """
        CREATE TABLE IF NOT EXISTS auto_feature_values (
            symbol TEXT NOT NULL,
            event_ts TIMESTAMPTZ NOT NULL,
            feature_name TEXT NOT NULL,
            feature_value DOUBLE PRECISION NOT NULL,
            PRIMARY KEY (symbol, event_ts, feature_name)
        )
        """
        create_log = """
        CREATE TABLE IF NOT EXISTS feature_evolution_log (
            feature TEXT NOT NULL,
            score DOUBLE PRECISION,
            promoted BOOLEAN NOT NULL,
            ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(create_feature_values)
                cur.execute(create_log)
            conn.commit()

    def _load_market_data(self) -> pd.DataFrame:
        """Fetch OHLCV and order book snapshots for the configured lookback window."""

        start_ts = datetime.now(timezone.utc) - self.config.lookback
        end_ts = datetime.now(timezone.utc)
        with self._connect() as conn:
            ohlcv_sql = f"""
                SELECT {self.config.symbol_column} AS symbol,
                       {self.config.timestamp_column} AS event_ts,
                       open, high, low, close, volume
                FROM {self.config.ohlcv_table}
                WHERE {self.config.timestamp_column} BETWEEN %(start)s AND %(end)s
                ORDER BY event_ts
            """
            order_sql = f"""
                SELECT {self.config.symbol_column} AS symbol,
                       {self.config.timestamp_column} AS event_ts,
                       best_bid, best_ask, bid_volume, ask_volume
                FROM {self.config.order_book_table}
                WHERE {self.config.timestamp_column} BETWEEN %(start)s AND %(end)s
                ORDER BY event_ts
            """
            params = {"start": start_ts, "end": end_ts}
            ohlcv = pd.read_sql(ohlcv_sql, conn, params=params)
            try:
                order_book = pd.read_sql(order_sql, conn, params=params)
            except Exception:
                LOGGER.warning(
                    "Order book table %s could not be loaded; proceeding with OHLCV only",
                    self.config.order_book_table,
                )
                order_book = pd.DataFrame(
                    [],
                    columns=["symbol", "event_ts", "best_bid", "best_ask", "bid_volume", "ask_volume"],
                )

        if ohlcv.empty:
            return pd.DataFrame()

        ohlcv["event_ts"] = pd.to_datetime(ohlcv["event_ts"], utc=True)
        order_book["event_ts"] = pd.to_datetime(order_book["event_ts"], utc=True)
        merged = pd.merge(ohlcv, order_book, on=["symbol", "event_ts"], how="left")
        merged = merged.set_index(["symbol", "event_ts"]).sort_index()
        LOGGER.debug("Loaded %d market data rows", len(merged))
        if len(merged) < self.config.min_observations:
            LOGGER.warning(
                "Insufficient observations (%d < %d) for reliable scoring",  # noqa: TRY003
                len(merged),
                self.config.min_observations,
            )
        return merged

    # ------------------------------------------------------------------
    # Feature engineering
    # ------------------------------------------------------------------
    def _generate_candidates(self, market_data: pd.DataFrame) -> Iterator[FeatureCandidate]:
        """Yield a diverse set of engineered feature candidates."""

        close = market_data["close"]
        returns = close.groupby(level=0).pct_change()
        volume = market_data.get("volume")

        # Technical indicators: moving averages and RSI-like oscillator.
        for window in (5, 14, 30):
            ma = close.groupby(level=0).transform(lambda s: s.rolling(window).mean())
            yield FeatureCandidate(name=f"{self.config.feature_prefix}_ma_{window}", values=ma)

        momentum = returns.groupby(level=0).transform(lambda s: s.rolling(5).sum())
        volatility = returns.groupby(level=0).transform(lambda s: s.rolling(20).std())
        rsi = self._compute_rsi(close, period=14)
        yield FeatureCandidate(name=f"{self.config.feature_prefix}_momentum_5", values=momentum)
        yield FeatureCandidate(name=f"{self.config.feature_prefix}_volatility_20", values=volatility)
        yield FeatureCandidate(name=f"{self.config.feature_prefix}_rsi_14", values=rsi)

        if volume is not None:
            vol_imbalance = volume.groupby(level=0).transform(lambda s: s.rolling(10).mean())
            yield FeatureCandidate(name=f"{self.config.feature_prefix}_volume_trend", values=vol_imbalance)

        # Order book derived features when data is present.
        if {"best_bid", "best_ask"}.issubset(market_data.columns):
            spread = (market_data["best_ask"] - market_data["best_bid"]) / market_data["close"]
            yield FeatureCandidate(name=f"{self.config.feature_prefix}_spread", values=spread)

        if {"bid_volume", "ask_volume"}.issubset(market_data.columns):
            imbalance = (market_data["bid_volume"] - market_data["ask_volume"]) / (
                market_data[["bid_volume", "ask_volume"]].sum(axis=1)
            )
            yield FeatureCandidate(name=f"{self.config.feature_prefix}_book_imbalance", values=imbalance)

        # Cross-symbol correlations: realized correlation with the market proxy.
        correlation = self._compute_cross_correlation(returns)
        yield FeatureCandidate(name=f"{self.config.feature_prefix}_market_corr", values=correlation)

        # Volatility surface approximations: realized vol across multiple horizons.
        for window in (10, 30, 60):
            realized_vol = returns.groupby(level=0).transform(lambda s: s.rolling(window).std())
            yield FeatureCandidate(name=f"{self.config.feature_prefix}_rv_{window}", values=realized_vol)

    def _compute_rsi(self, close: pd.Series, period: int = 14) -> pd.Series:
        """Compute a Relative Strength Index style oscillator."""

        grouped = close.groupby(level=0)

        def _rsi(series: pd.Series) -> pd.Series:
            delta = series.diff()
            up = delta.clip(lower=0)
            down = -delta.clip(upper=0)
            roll_up = up.ewm(alpha=1 / period, adjust=False).mean()
            roll_down = down.ewm(alpha=1 / period, adjust=False).mean()
            rs = roll_up / (roll_down + 1e-9)
            return 100 - (100 / (1 + rs))

        return grouped.transform(_rsi)

    def _compute_cross_correlation(self, returns: pd.Series) -> pd.Series:
        """Compute rolling correlation of each symbol's returns with the cross-sectional mean."""

        df = returns.unstack(level=0)
        market = df.mean(axis=1)
        corr = df.rolling(60, min_periods=30).corr(market)
        return corr.stack().reindex(returns.index)

    # ------------------------------------------------------------------
    # Modeling and scoring
    # ------------------------------------------------------------------
    def _derive_target(self, market_data: pd.DataFrame) -> pd.Series:
        """Derive a simple forward return target for supervised evaluation."""

        close = market_data["close"].groupby(level=0)
        forward_return = close.shift(-1) / close - 1
        return forward_return

    def _score_candidates(
        self, candidates: Sequence[FeatureCandidate], target: pd.Series
    ) -> List[FeatureCandidate]:
        """Score each candidate feature via walk-forward LightGBM models."""

        scored: List[FeatureCandidate] = []
        for candidate in candidates:
            series = candidate.values
            df = pd.DataFrame({"feature": series, "target": target})
            df = df.dropna()
            if df.empty:
                LOGGER.debug("Skipping %s – insufficient data after dropna", candidate.name)
                scored.append(candidate)
                continue

            # Convert multi-index into explicit columns for modeling.
            df = df.reset_index().rename(columns={"level_0": "symbol", "level_1": "event_ts"})
            df = df.sort_values("event_ts")
            feature_values = df[["feature"]].values
            target_values = (df["target"] > 0).astype(int).values
            forward_returns = df["target"].values

            accuracy_scores: List[float] = []
            sharpe_values: List[float] = []
            total_weight = 0

            tscv = self._time_series_splits(len(df))
            for train_idx, test_idx in tscv:
                X_train, y_train = feature_values[train_idx], target_values[train_idx]
                X_test, y_test = feature_values[test_idx], target_values[test_idx]
                if len(np.unique(y_train)) < 2:
                    LOGGER.debug(
                        "Skipping split for %s due to single-class training labels", candidate.name
                    )
                    continue
                train_set = lgb.Dataset(X_train, label=y_train)
                valid_set = lgb.Dataset(X_test, label=y_test, reference=train_set)
                params = {
                    "objective": "binary",
                    "learning_rate": 0.05,
                    "num_leaves": 15,
                    "metric": "binary_logloss",
                    "verbosity": -1,
                }
                model = lgb.train(
                    params,
                    train_set,
                    num_boost_round=200,
                    valid_sets=[valid_set],
                    early_stopping_rounds=20,
                    verbose_eval=False,
                )
                probs = model.predict(X_test)
                preds = (probs >= 0.5).astype(int)
                accuracy = float((preds == y_test).mean())
                strategy_returns = np.sign(probs - 0.5) * forward_returns[test_idx]
                sharpe = self._sharpe_ratio(strategy_returns)
                weight = len(test_idx)
                accuracy_scores.append(accuracy * weight)
                sharpe_values.append(sharpe * weight)
                total_weight += weight

            if total_weight == 0:
                LOGGER.debug("No valid folds for %s", candidate.name)
                scored.append(candidate)
                continue

            weighted_accuracy = sum(accuracy_scores) / total_weight
            weighted_sharpe = sum(sharpe_values) / total_weight
            composite_score = weighted_accuracy + 0.1 * weighted_sharpe
            scored.append(
                dataclasses.replace(
                    candidate,
                    score=composite_score,
                    accuracy=weighted_accuracy,
                    sharpe=weighted_sharpe,
                )
            )
            LOGGER.info(
                "Feature %s scored %.4f (accuracy=%.4f, sharpe=%.4f)",
                candidate.name,
                composite_score,
                weighted_accuracy,
                weighted_sharpe,
            )
        return scored

    def _time_series_splits(self, n_rows: int, n_splits: int = 5) -> Iterator[Tuple[np.ndarray, np.ndarray]]:
        """Generate expanding window splits for walk-forward validation."""

        if n_rows < n_splits + 1:
            yield from ()
            return
        indices = np.arange(n_rows)
        fold_size = n_rows // (n_splits + 1)
        for fold in range(1, n_splits + 1):
            train_end = fold * fold_size
            test_end = (fold + 1) * fold_size
            if test_end > n_rows:
                test_end = n_rows
            train_idx = indices[:train_end]
            test_idx = indices[train_end:test_end]
            if len(test_idx) == 0:
                continue
            yield train_idx, test_idx

    def _sharpe_ratio(self, returns: np.ndarray) -> float:
        if returns.size == 0:
            return 0.0
        mean = float(np.mean(returns))
        std = float(np.std(returns))
        if std == 0:
            return 0.0
        # Annualise assuming hourly observations.
        scale = math.sqrt(24 * 252)
        return (mean / std) * scale

    # ------------------------------------------------------------------
    # Persistence and promotion
    # ------------------------------------------------------------------
    def _persist_candidates(self, candidates: Sequence[FeatureCandidate]) -> None:
        """Persist feature values for later retrieval by Feast."""

        rows: List[Tuple[str, datetime, str, float]] = []
        for candidate in candidates:
            df = candidate.as_dataframe().dropna()
            for row in df.itertuples(index=False):
                rows.append((row.symbol, row.event_ts.to_pydatetime(), candidate.name, float(row[2])))
        if not rows:
            LOGGER.debug("No feature values to persist")
            return
        insert_sql = """
            INSERT INTO auto_feature_values (symbol, event_ts, feature_name, feature_value)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, event_ts, feature_name)
            DO UPDATE SET feature_value = EXCLUDED.feature_value
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, rows)
            conn.commit()

    def _promote_features(self, features: Sequence[FeatureCandidate]) -> None:
        """Apply promoted features to the Feast repository."""

        if not features:
            return
        if self.config.feast_repo is None:
            LOGGER.warning(
                "Feast repository path is not configured; skipping promotion for %d features",
                len(features),
            )
            return
        if FeatureStore is None or FeatureView is None or PostgreSQLSource is None:
            LOGGER.warning("Feast is not available in this environment; skipping promotion")
            return

        store = FeatureStore(repo_path=self.config.feast_repo)
        applied_views: List[FeatureView] = []
        for feature in features:
            view_name = feature.name
            source = PostgreSQLSource(
                name=f"{view_name}_source",
                query="""
                    SELECT symbol, event_ts, feature_value
                    FROM auto_feature_values
                    WHERE feature_name = %(feature_name)s
                """,
                timestamp_field="event_ts",
            )
            fv = FeatureView(
                name=view_name,
                entities=["symbol"],
                ttl=timedelta(days=7),
                schema=[Field(name="feature_value", dtype=Float32)],
                online=True,
                source=source,
                tags={"source": "auto_discovery"},
            )
            applied_views.append(fv)
            store.apply([fv])
            LOGGER.info("Promoted feature %s to Feast", feature.name)

        if applied_views:
            store.refresh_registry()

    def _log_promotions(
        self, candidates: Sequence[FeatureCandidate], promoted: Sequence[FeatureCandidate]
    ) -> None:
        """Persist promotion decisions to ``feature_evolution_log``."""

        promoted_names = {feature.name for feature in promoted}
        rows = []
        now = datetime.now(timezone.utc)
        for candidate in candidates:
            rows.append(
                (
                    candidate.name,
                    candidate.score,
                    candidate.name in promoted_names,
                    now,
                )
            )
        insert_sql = """
            INSERT INTO feature_evolution_log (feature, score, promoted, ts)
            VALUES (%s, %s, %s, %s)
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, rows)
            conn.commit()


def _configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Automated Timescale -> Feast feature discovery")
    parser.add_argument("--dsn", default=os.getenv("TIMESCALE_DSN"), help="TimescaleDB DSN")
    parser.add_argument(
        "--feast-repo",
        default=os.getenv("FEAST_REPO", "data/feast"),
        help="Path to the Feast repository",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=int(os.getenv("FEATURE_LOOKBACK_DAYS", "30")),
        help="Number of days to include in the discovery window",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=int(os.getenv("FEATURE_SCAN_INTERVAL_MIN", "60")),
        help="Minutes between discovery cycles",
    )
    parser.add_argument(
        "--max-features",
        type=int,
        default=int(os.getenv("FEATURE_MAX_PROMOTE", "10")),
        help="Maximum number of features to promote per cycle",
    )
    parser.add_argument("--once", action="store_true", help="Run a single discovery iteration")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args(argv)
    if not args.dsn:
        parser.error("Timescale DSN is required via --dsn or TIMESCALE_DSN")
    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    _configure_logging(verbose=args.verbose)
    config = FeatureDiscoveryConfig(
        timescale_dsn=args.dsn,
        feast_repo=args.feast_repo,
        lookback=timedelta(days=args.lookback_days),
        scan_interval=timedelta(minutes=args.interval_minutes),
        max_features=args.max_features,
    )
    engine = FeatureDiscoveryEngine(config)
    if args.once:
        engine.run_once()
    else:
        engine.run_forever()


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
