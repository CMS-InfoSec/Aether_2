"""Model registry and lightweight ensemble scaffolding for the policy service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Sequence


@dataclass
class ModelPrediction:
    """Container holding the results of an ensemble inference."""

    edge_bps: float
    confidence: Dict[str, float]
    take_profit_bps: float
    stop_loss_bps: float


class ModelEnsemble:
    """Very small facade around an ensemble of models."""

    def __init__(self, name: str, version: str, confidence_threshold: float = 0.55) -> None:
        self.name = name
        self.version = version
        self.confidence_threshold = confidence_threshold

    def score_features(self, features: Sequence[float]) -> float:
        if not features:
            return 0.0
        return sum(features) / float(len(features))

    def predict(
        self,
        *,
        features: Sequence[float],
        book_snapshot: Dict[str, float],
        state: Dict[str, float],
        expected_edge_bps: Optional[float] = None,
    ) -> ModelPrediction:
        edge = expected_edge_bps if expected_edge_bps is not None else self.score_features(features)
        conviction = float(state.get("conviction", 0.5))
        liquidity = float(state.get("liquidity_score", 0.5))
        spread_bps = float(book_snapshot.get("spread_bps", 0.0))
        imbalance = float(book_snapshot.get("imbalance", 0.0))

        edge += (conviction - 0.5) * 20.0
        edge += (liquidity - 0.5) * 10.0
        edge -= spread_bps * 0.1
        edge += imbalance * 5.0

        take_profit = max(edge * 1.5, 5.0)
        stop_loss = max(abs(edge) * 0.5, 5.0)

        model_conf = min(1.0, max(0.0, 0.5 + edge / 200.0))
        state_conf = min(1.0, max(0.0, liquidity))
        execution_conf = min(1.0, max(0.0, 1.0 - spread_bps / 100.0))

        confidence = {
            "model_confidence": round(model_conf, 4),
            "state_confidence": round(state_conf, 4),
            "execution_confidence": round(execution_conf, 4),
        }

        return ModelPrediction(
            edge_bps=edge,
            confidence=confidence,
            take_profit_bps=take_profit,
            stop_loss_bps=stop_loss,
        )


class ModelRegistry:
    """Registry returning the latest ensemble per account/instrument combination."""

    def __init__(self) -> None:
        self._ensembles: Dict[str, ModelEnsemble] = {}
        self._default = ModelEnsemble(name="default", version="1.0.0")

    def register(self, key: str, ensemble: ModelEnsemble) -> None:
        self._ensembles[key] = ensemble

    def get_latest_ensemble(self, account_id: str, instrument: str) -> ModelEnsemble:
        key = f"{account_id}:{instrument}"
        if key in self._ensembles:
            return self._ensembles[key]
        if account_id in self._ensembles:
            return self._ensembles[account_id]
        return self._default


_REGISTRY = ModelRegistry()


def get_model_registry() -> ModelRegistry:
    return _REGISTRY
