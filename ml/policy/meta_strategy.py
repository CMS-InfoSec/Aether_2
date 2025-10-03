"""Meta strategy allocation and FastAPI endpoint.

This module tracks historical performance of multiple trading strategies and
learns which strategy tends to dominate in different market regimes.  A simple
classifier is retrained on every new observation to score the incoming regime
features and we expose the resulting allocation through a FastAPI endpoint.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Mapping, Optional

import numpy as np
import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sklearn.base import ClassifierMixin
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from services.common.security import require_admin_account

logger = logging.getLogger(__name__)

STRATEGIES: List[str] = ["trend", "mean-reversion", "scalping", "hedging"]


@dataclass
class StrategyObservation:
    """Container describing a single training example."""

    symbol: str
    regime: str
    features: Mapping[str, float]
    performance: Mapping[str, float]
    best_strategy: str
    timestamp: datetime


class MetaStrategyAllocator:
    """Learn meta weights for the available strategies.

    The allocator keeps an in-memory history of observations.  Every
    observation contains feature data about the detected market regime and
    realised performance for each leaf strategy.  We use a simple logistic
    regression classifier to predict which strategy should dominate for a
    similar regime and convert the predicted probabilities into allocation
    weights.
    """

    def __init__(
        self,
        history_window: int = 512,
        classifier: Optional[ClassifierMixin] = None,
        min_train_size: int = 20,
        smoothing: float = 0.6,
    ) -> None:
        self.history_window = history_window
        self.min_train_size = min_train_size
        self.smoothing = smoothing
        self._observations: List[StrategyObservation] = []
        self._feature_columns: List[str] = []
        self._classifier: ClassifierMixin = classifier or Pipeline(
            steps=[
                ("scaler", StandardScaler()),
                (
                    "clf",
                    LogisticRegression(
                        multi_class="multinomial",
                        max_iter=500,
                        class_weight="balanced",
                    ),
                ),
            ]
        )
        self._latest_allocations: Dict[str, Dict[str, object]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def update(
        self,
        symbol: str,
        regime: str,
        features: Mapping[str, float],
        performance: Mapping[str, float],
        timestamp: Optional[datetime] = None,
    ) -> Dict[str, float]:
        """Add a new observation and update the allocation weights.

        Parameters
        ----------
        symbol:
            The symbol for which the observation was generated.
        regime:
            Regime label describing the market environment.
        features:
            Dictionary containing the numeric regime features that will be
            provided to the classifier.
        performance:
            Mapping from strategy name to realised performance (e.g. Sharpe
            or PnL).  Only the configured strategies are considered.
        timestamp:
            Optional timestamp.  ``datetime.now(timezone.utc)`` is used when
            omitted.
        """

        ts = timestamp or datetime.now(timezone.utc)
        filtered_performance = {
            name: float(performance.get(name, 0.0)) for name in STRATEGIES
        }

        if not filtered_performance:
            raise ValueError("No performance metrics provided for strategies")

        best_strategy = max(filtered_performance, key=filtered_performance.__getitem__)
        observation = StrategyObservation(
            symbol=symbol,
            regime=regime,
            features=dict(features),
            performance=filtered_performance,
            best_strategy=best_strategy,
            timestamp=ts,
        )
        self._append_observation(observation)

        weights = self._compute_weights(observation)
        self._latest_allocations[symbol] = {
            "symbol": symbol,
            "regime": regime,
            "weights": weights,
            "timestamp": ts,
        }
        strategy_allocation_log(symbol, regime, weights, ts)
        return weights

    def get_allocation(self, symbol: str) -> Dict[str, object]:
        """Return the latest allocation for ``symbol``.

        Raises
        ------
        KeyError
            If no allocation has been computed for the provided symbol.
        """

        if symbol not in self._latest_allocations:
            raise KeyError(f"No allocation is available for symbol '{symbol}'")
        return self._latest_allocations[symbol]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _append_observation(self, observation: StrategyObservation) -> None:
        self._observations.append(observation)
        if len(self._observations) > self.history_window:
            # Drop the oldest observation to maintain the rolling window.
            self._observations.pop(0)

        feature_keys = sorted(observation.features)
        if not self._feature_columns:
            self._feature_columns = feature_keys
        else:
            # Ensure we remember the union of feature columns so new data is
            # aligned correctly.
            self._feature_columns = sorted(
                set(self._feature_columns).union(feature_keys)
            )

    def _compute_weights(self, observation: StrategyObservation) -> Dict[str, float]:
        classifier_ready = len(self._observations) >= self.min_train_size
        if classifier_ready:
            try:
                self._train_classifier()
            except ValueError as exc:  # pragma: no cover - defensive guard
                logger.warning("Failed to train meta strategy classifier: %s", exc)
                classifier_ready = False

        if classifier_ready:
            features = self._vectorise_features(observation.features)
            probabilities = self._classifier.predict_proba(features)[0]
            probability_map = dict(zip(self._classifier.classes_, probabilities))
            weights = {name: probability_map.get(name, 0.0) for name in STRATEGIES}
        else:
            weights = self._fallback_weights(observation.performance)

        blended_weights = self._blend_with_recent_performance(weights, observation)
        return blended_weights

    def _train_classifier(self) -> None:
        data = [obs for obs in self._observations]
        feature_matrix = np.vstack(
            [self._vectorise_features(obs.features) for obs in data]
        )
        labels = np.array([obs.best_strategy for obs in data])

        df = pd.DataFrame(feature_matrix, columns=self._feature_columns)
        # Guard against constant features which can cause the solver to fail.
        df = df.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        feature_matrix = df.to_numpy(dtype=float)
        self._classifier.fit(feature_matrix, labels)

    def _vectorise_features(self, features: Mapping[str, float]) -> np.ndarray:
        vector = np.zeros(len(self._feature_columns), dtype=float)
        feature_map = {**{k: 0.0 for k in self._feature_columns}, **features}
        for idx, key in enumerate(self._feature_columns):
            vector[idx] = float(feature_map.get(key, 0.0))
        return vector.reshape(1, -1)

    def _fallback_weights(self, performance: Mapping[str, float]) -> Dict[str, float]:
        performance_array = np.array([performance.get(name, 0.0) for name in STRATEGIES])
        clipped = np.clip(performance_array, a_min=0.0, a_max=None)
        if clipped.sum() == 0:
            return {name: 1.0 / len(STRATEGIES) for name in STRATEGIES}
        weights = clipped / clipped.sum()
        return dict(zip(STRATEGIES, weights))

    def _blend_with_recent_performance(
        self,
        weights: Mapping[str, float],
        observation: StrategyObservation,
    ) -> Dict[str, float]:
        normalised_perf = self._normalise_performance(observation.performance)
        blended = {}
        for strategy in STRATEGIES:
            prior = weights.get(strategy, 0.0)
            perf = normalised_perf.get(strategy, 0.0)
            blended[strategy] = self.smoothing * prior + (1 - self.smoothing) * perf

        total = sum(blended.values())
        if total <= 0:
            return {name: 1.0 / len(STRATEGIES) for name in STRATEGIES}
        return {k: v / total for k, v in blended.items()}

    def _normalise_performance(self, performance: Mapping[str, float]) -> Dict[str, float]:
        values = np.array([performance.get(name, 0.0) for name in STRATEGIES])
        max_abs = np.max(np.abs(values))
        if max_abs == 0:
            return {name: 1.0 / len(STRATEGIES) for name in STRATEGIES}
        normalised = (values / max_abs + 1.0) / 2.0
        total = normalised.sum()
        if total == 0:
            return {name: 1.0 / len(STRATEGIES) for name in STRATEGIES}
        normalised /= total
        return dict(zip(STRATEGIES, normalised))


class StrategyWeightsResponse(BaseModel):
    """Response payload for the ``/meta/strategy_weights`` endpoint."""

    symbol: str = Field(..., description="Trading symbol")
    regime: str = Field(..., description="Detected market regime")
    weights: Dict[str, float] = Field(
        ...,
        description="Allocation weights per strategy",
        example={name: 0.25 for name in STRATEGIES},
    )
    timestamp: datetime = Field(..., description="Timestamp for the allocation")


app = FastAPI(title="Meta Strategy Allocation", version="1.0.0")
_allocator = MetaStrategyAllocator()


@app.get("/meta/strategy_weights", response_model=StrategyWeightsResponse)
def get_strategy_weights(
    symbol: str = Query(..., min_length=1),
    admin_account: str = Depends(require_admin_account),
) -> StrategyWeightsResponse:
    """Expose the most recent allocation for ``symbol``.

    The endpoint returns the last computed allocation or raises a ``404`` if we
    have not yet observed the requested symbol.
    """

    try:
        allocation = _allocator.get_allocation(symbol)
    except KeyError as exc:  # pragma: no cover - HTTP layer
        logger.warning(
            "Meta strategy allocation missing",
            extra={"symbol": symbol, "account_id": admin_account},
        )
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    audit_details = {
        "symbol": allocation.get("symbol", symbol),
        "regime": allocation.get("regime"),
        "weights": dict(allocation.get("weights", {})),
        "account_id": admin_account,
        "timestamp": allocation.get("timestamp"),
    }
    timestamp = audit_details.get("timestamp")
    if isinstance(timestamp, datetime):
        audit_details["timestamp"] = timestamp.isoformat()

    logger.info("Meta strategy allocation served", extra=audit_details)
    return StrategyWeightsResponse(**allocation)


def strategy_allocation_log(
    symbol: str,
    regime: str,
    weights: Mapping[str, float],
    timestamp: datetime,
) -> None:
    """Log the meta strategy allocation decision.

    This function centralises logging so it can be replaced with a more
    sophisticated audit sink in the future.  Currently we simply emit a
    structured log line using the module logger.
    """

    logger.info(
        "Meta strategy allocation decided",
        extra={
            "symbol": symbol,
            "regime": regime,
            "weights": dict(weights),
            "timestamp": timestamp.isoformat(),
        },
    )


__all__ = [
    "MetaStrategyAllocator",
    "STRATEGIES",
    "StrategyObservation",
    "StrategyWeightsResponse",
    "app",
    "get_strategy_weights",
    "strategy_allocation_log",
]
