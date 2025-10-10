"""Meta strategy allocation and FastAPI endpoint.

This module tracks historical performance of multiple trading strategies and
learns which strategy tends to dominate in different market regimes.  A simple
classifier is retrained on every new observation to score the incoming regime
features and we expose the resulting allocation through a FastAPI endpoint.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Mapping, Optional, Sequence

try:  # pragma: no cover - optional scientific stack
    import numpy as np
except Exception:  # pragma: no cover - fallback when numpy is unavailable
    np = None  # type: ignore[assignment]

try:  # pragma: no cover - optional scientific stack
    import pandas as pd
except Exception:  # pragma: no cover - fallback when pandas is unavailable
    pd = None  # type: ignore[assignment]

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

try:  # pragma: no cover - optional scientific stack
    from sklearn.base import ClassifierMixin
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
except Exception:  # pragma: no cover - fallback when sklearn is unavailable
    ClassifierMixin = object  # type: ignore[misc,assignment]
    LogisticRegression = None  # type: ignore[assignment]
    Pipeline = None  # type: ignore[assignment]
    StandardScaler = None  # type: ignore[assignment]

from services.common.security import require_admin_account
from services.common.spot import require_spot_http
from shared.spot import normalize_spot_symbol

logger = logging.getLogger(__name__)

STRATEGIES: List[str] = ["trend", "mean-reversion", "scalping", "hedging"]


_SCIENTIFIC_STACK_AVAILABLE = all(
    dependency is not None
    for dependency in (np, pd, Pipeline, StandardScaler, LogisticRegression)
)


class _FallbackClassifier:
    """Logistic-regression style classifier without third-party dependencies."""

    _LEARNING_RATE = 0.1
    _EPOCHS = 50
    _L2 = 0.001

    def __init__(self) -> None:
        self.classes_: List[str] = list(STRATEGIES)
        self._weights: Dict[str, List[float]] = {}
        self._feature_count = 0

    def fit(
        self, feature_matrix: Sequence[Sequence[float]], labels: Sequence[object]
    ) -> "_FallbackClassifier":
        samples = list(feature_matrix)
        if not samples:
            self._weights = {name: [0.0] for name in STRATEGIES}
            return self

        self._feature_count = len(samples[0]) if samples[0] else 0
        if any(len(sample) != self._feature_count for sample in samples):
            raise ValueError("Feature matrix contains inconsistent sample lengths")

        if self._feature_count == 0:
            counts: Dict[str, int] = {name: 0 for name in STRATEGIES}
            for label in labels:
                counts[str(label)] = counts.get(str(label), 0) + 1
            total = sum(counts.values())
            if total:
                self._weights = {
                    name: [counts.get(name, 0) / total]
                    for name in STRATEGIES
                }
            else:  # pragma: no cover - defensive fallback
                self._weights = {name: [1.0 / len(STRATEGIES)] for name in STRATEGIES}
            return self

        for name in STRATEGIES:
            if name not in self._weights or len(self._weights[name]) != self._feature_count + 1:
                self._weights[name] = [0.0] * (self._feature_count + 1)

        encoded_labels = [str(label) for label in labels]
        for _ in range(self._EPOCHS):
            for features, label in zip(samples, encoded_labels):
                for strategy in STRATEGIES:
                    weights = self._weights[strategy]
                    activation = self._dot(weights, features)
                    probability = self._sigmoid(activation)
                    target = 1.0 if label == strategy else 0.0
                    error = target - probability
                    for idx, value in enumerate(features):
                        weights[idx] += self._LEARNING_RATE * (
                            error * value - self._L2 * weights[idx]
                        )
                    # Bias term is stored in the last position.
                    weights[-1] += self._LEARNING_RATE * error

        return self

    def predict_proba(
        self, feature_matrix: Sequence[Sequence[float]]
    ) -> List[List[float]]:
        probabilities: List[List[float]] = []
        for features in feature_matrix:
            if len(features) != self._feature_count:
                raise ValueError("Feature vector length does not match trained model")
            scores = []
            for strategy in STRATEGIES:
                weights = self._weights.get(strategy)
                if not weights:
                    scores.append(0.0)
                    continue
                activation = self._dot(weights, features)
                scores.append(activation)
            probabilities.append(self._softmax(scores))
        return probabilities

    def _dot(self, weights: Sequence[float], features: Sequence[float]) -> float:
        # Bias term stored at the end of the weight vector.
        total = weights[-1]
        for weight, value in zip(weights[:-1], features):
            total += weight * value
        return total

    @staticmethod
    def _sigmoid(value: float) -> float:
        if value < -60:
            return 0.0
        if value > 60:
            return 1.0
        return 1.0 / (1.0 + math.exp(-value))

    @staticmethod
    def _softmax(scores: Sequence[float]) -> List[float]:
        if not scores:
            return [0.0 for _ in STRATEGIES]
        max_score = max(scores)
        exp_scores = [math.exp(score - max_score) for score in scores]
        total = sum(exp_scores)
        if total == 0:
            return [1.0 / len(scores)] * len(scores)
        return [score / total for score in exp_scores]


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
        self._using_fallback_classifier = False

        if classifier is not None:
            self._classifier: object = classifier
        elif _SCIENTIFIC_STACK_AVAILABLE:
            self._classifier = Pipeline(
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
        else:
            logger.warning(
                "Scientific stack unavailable; falling back to heuristic meta strategy allocations"
            )
            self._classifier = _FallbackClassifier()
            self._using_fallback_classifier = True
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

        allocation = self._latest_allocations.get(symbol)
        if allocation is None:
            normalized = normalize_spot_symbol(symbol)
            for candidate, candidate_allocation in self._latest_allocations.items():
                if normalize_spot_symbol(candidate) == normalized and normalized:
                    allocation = candidate_allocation
                    break
        if allocation is None:
            raise KeyError(f"No allocation is available for symbol '{symbol}'")
        return allocation

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
            probabilities = self._predict_probabilities(features)
            probability_map = dict(zip(self._get_classifier_classes(), probabilities))
            weights = {name: float(probability_map.get(name, 0.0)) for name in STRATEGIES}
        else:
            weights = self._fallback_weights(observation.performance)

        blended_weights = self._blend_with_recent_performance(weights, observation)
        return blended_weights

    def _train_classifier(self) -> None:
        data = [obs for obs in self._observations]
        feature_matrix = [self._vectorise_features(obs.features) for obs in data]
        labels = [obs.best_strategy for obs in data]

        if self._using_fallback_classifier:
            self._classifier.fit(feature_matrix, labels)  # type: ignore[call-arg]
            return

        assert _SCIENTIFIC_STACK_AVAILABLE  # nosec - guarded during construction
        assert np is not None and pd is not None  # satisfy type checkers

        matrix = np.asarray(feature_matrix, dtype=float)
        df = pd.DataFrame(matrix, columns=self._feature_columns)
        # Guard against constant features which can cause the solver to fail.
        df = df.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        cleaned = df.to_numpy(dtype=float)
        target = np.array(labels)
        self._classifier.fit(cleaned, target)  # type: ignore[arg-type]

    def _vectorise_features(self, features: Mapping[str, float]) -> List[float]:
        vector = [0.0] * len(self._feature_columns)
        for idx, key in enumerate(self._feature_columns):
            value = features.get(key, 0.0)
            try:
                vector[idx] = float(value)
            except (TypeError, ValueError):  # pragma: no cover - defensive guard
                vector[idx] = 0.0
        return vector

    def _predict_probabilities(self, features: Sequence[float]) -> Sequence[float]:
        if self._using_fallback_classifier:
            probabilities = self._classifier.predict_proba([list(features)])  # type: ignore[attr-defined]
            return probabilities[0]

        assert np is not None  # nosec - ensured by constructor
        feature_array = np.asarray(features, dtype=float).reshape(1, -1)
        probabilities = self._classifier.predict_proba(feature_array)[0]  # type: ignore[arg-type]
        if hasattr(probabilities, "tolist"):
            return probabilities.tolist()
        return probabilities

    def _get_classifier_classes(self) -> Sequence[str]:
        classes = getattr(self._classifier, "classes_", None)
        if classes is None:
            return STRATEGIES
        if np is not None and hasattr(classes, "tolist"):
            return [str(value) for value in classes.tolist()]
        return [str(value) for value in classes]

    def _fallback_weights(self, performance: Mapping[str, float]) -> Dict[str, float]:
        performance_values = [float(performance.get(name, 0.0)) for name in STRATEGIES]
        clipped = [value if value > 0 else 0.0 for value in performance_values]
        total = sum(clipped)
        if total == 0:
            return {name: 1.0 / len(STRATEGIES) for name in STRATEGIES}
        return {name: clipped[idx] / total for idx, name in enumerate(STRATEGIES)}

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
        values = [float(performance.get(name, 0.0)) for name in STRATEGIES]
        max_abs = max((abs(value) for value in values), default=0.0)
        if max_abs == 0:
            return {name: 1.0 / len(STRATEGIES) for name in STRATEGIES}
        normalised = [((value / max_abs) + 1.0) / 2.0 for value in values]
        total = sum(normalised)
        if total == 0:
            return {name: 1.0 / len(STRATEGIES) for name in STRATEGIES}
        return {name: normalised[idx] / total for idx, name in enumerate(STRATEGIES)}


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

    normalized = require_spot_http(symbol, logger=logger)

    try:
        allocation = _allocator.get_allocation(normalized)
    except KeyError as exc:  # pragma: no cover - HTTP layer
        logger.warning(
            "Meta strategy allocation missing",
            extra={"symbol": normalized, "account_id": admin_account},
        )
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    audit_details = {
        "symbol": allocation.get("symbol", normalized),
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
