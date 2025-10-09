"""Tests covering the meta-strategy fallback classifier."""

from __future__ import annotations

from typing import Dict

import pytest

from ml.policy import meta_strategy


def _performance(winner: str) -> Dict[str, float]:
    payload = {name: 0.0 for name in meta_strategy.STRATEGIES}
    payload[winner] = 1.0
    return payload


@pytest.mark.parametrize(
    "feature,expected",
    [
        (3.0, "trend"),
        (-3.0, "mean-reversion"),
    ],
)
def test_fallback_classifier_responds_to_features(feature: float, expected: str) -> None:
    allocator = meta_strategy.MetaStrategyAllocator(
        classifier=meta_strategy._FallbackClassifier(),
        min_train_size=1,
        smoothing=1.0,
        history_window=128,
    )
    allocator._using_fallback_classifier = True

    training_samples = [
        ({"momentum": -2.0}, "mean-reversion"),
        ({"momentum": -1.5}, "mean-reversion"),
        ({"momentum": -0.5}, "mean-reversion"),
        ({"momentum": 0.5}, "trend"),
        ({"momentum": 1.5}, "trend"),
        ({"momentum": 2.0}, "trend"),
    ]

    for features, winner in training_samples:
        allocator.update(
            symbol="BTC/USD",
            regime="bull",
            features=features,
            performance=_performance(winner),
        )

    vector = allocator._vectorise_features({"momentum": feature})
    probabilities = allocator._predict_probabilities(vector)
    mapping = dict(zip(allocator._get_classifier_classes(), probabilities))

    assert mapping[expected] == max(mapping.values())
