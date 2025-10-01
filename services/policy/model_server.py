"""Lightweight model server abstraction for the policy service.

This module simulates loading the latest intent model from MLflow and exposes
an inference entry point used by the policy API.  The MLflow integration is a
stub so that the service can run locally without any external dependencies
while preserving the expected control flow of the production system.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple

from services.common.schemas import ActionTemplate, BookSnapshot, ConfidenceMetrics

logger = logging.getLogger(__name__)


@dataclass
class Intent:
    """Represents a policy intent returned by the model server."""

    edge_bps: float
    confidence: ConfidenceMetrics
    take_profit_bps: float
    stop_loss_bps: float
    selected_action: str
    action_templates: List[ActionTemplate]
    approved: bool
    reason: str | None = None

    @classmethod
    def null(cls, reason: str = "model_unavailable") -> "Intent":
        """Return a do-nothing intent used when inference fails."""

        confidence = ConfidenceMetrics(
            model_confidence=0.0,
            state_confidence=0.0,
            execution_confidence=0.0,
            overall_confidence=0.0,
        )
        template = ActionTemplate(
            name="abstain",
            venue_type="none",
            edge_bps=0.0,
            fee_bps=0.0,
            confidence=0.0,
        )
        return cls(
            edge_bps=0.0,
            confidence=confidence,
            take_profit_bps=0.0,
            stop_loss_bps=0.0,
            selected_action="abstain",
            action_templates=[template],
            approved=False,
            reason=reason,
        )

    @property
    def is_null(self) -> bool:
        return not self.approved and self.edge_bps == 0.0 and self.selected_action == "abstain"


class DummyPolicyModel:
    """Simplified model used while a real MLflow artifact is unavailable."""

    def __init__(self, name: str, confidence_threshold: float = 0.55) -> None:
        self.name = name
        self.confidence_threshold = confidence_threshold
        # Seed the RNG to provide deterministic behaviour per model key.
        self._rng = random.Random(hash(name) & 0xFFFF_FFFF)

    def predict(
        self,
        features: Sequence[float],
        book_snapshot: BookSnapshot,
        *,
        horizon: int | None = None,
    ) -> Intent:
        feature_score = self._average(features)
        spread_penalty = book_snapshot.spread_bps * 0.1
        imbalance_adjustment = book_snapshot.imbalance * 8.0
        noise = self._rng.uniform(-1.5, 1.5)

        edge = feature_score + imbalance_adjustment - spread_penalty + noise

        if horizon:
            horizon_scale = max(0.5, min(2.0, horizon / float(15 * 60)))
            edge *= horizon_scale

        take_profit = max(5.0, abs(edge) * 1.2)
        stop_loss = max(5.0, abs(edge) * 0.6)

        model_conf = min(1.0, max(0.0, 0.5 + edge / 200.0))
        state_conf = min(1.0, max(0.0, 0.5 + book_snapshot.imbalance / 2.0))
        exec_conf = min(1.0, max(0.0, 1.0 - book_snapshot.spread_bps / 100.0))

        confidence = ConfidenceMetrics(
            model_confidence=round(model_conf, 4),
            state_confidence=round(state_conf, 4),
            execution_confidence=round(exec_conf, 4),
        )

        maker_edge = round(edge - book_snapshot.spread_bps * 0.05, 4)
        taker_edge = round(edge - book_snapshot.spread_bps * 0.2, 4)

        maker_template = ActionTemplate(
            name="maker",
            venue_type="maker",
            edge_bps=maker_edge,
            fee_bps=0.0,
            confidence=round(confidence.execution_confidence, 4),
        )
        taker_template = ActionTemplate(
            name="taker",
            venue_type="taker",
            edge_bps=taker_edge,
            fee_bps=0.0,
            confidence=round(confidence.execution_confidence * 0.95, 4),
        )

        action_templates = [maker_template, taker_template]
        preferred = max(action_templates, key=lambda template: template.edge_bps)
        approved = preferred.edge_bps > 0 and (confidence.overall_confidence or 0.0) >= self.confidence_threshold

        if approved:
            reason: str | None = None
            selected_action = preferred.name
        else:
            selected_action = "abstain"
            if preferred.edge_bps <= 0:
                reason = "Non-positive edge"
            else:
                reason = "Confidence below threshold"

        return Intent(
            edge_bps=edge,
            confidence=confidence,
            take_profit_bps=take_profit,
            stop_loss_bps=stop_loss,
            selected_action=selected_action,
            action_templates=action_templates,
            approved=approved,
            reason=reason,
        )

    @staticmethod
    def _average(values: Iterable[float]) -> float:
        total = 0.0
        count = 0
        for value in values:
            total += float(value)
            count += 1
        if count == 0:
            return 0.0
        return total / float(count)

    def explain(
        self, feature_values: Mapping[str, float] | Sequence[float]
    ) -> Dict[str, float]:
        """Return a deterministic attribution for the supplied features."""

        if isinstance(feature_values, Mapping):
            items: Iterable[Tuple[str, float]] = (
                (str(key), float(value)) for key, value in feature_values.items()
            )
        else:
            items = ((f"feature_{idx}", float(value)) for idx, value in enumerate(feature_values))

        values = list(items)
        if not values:
            return {}

        normalizer = float(len(values))
        if normalizer == 0:
            return {}

        return {name: contribution / normalizer for name, contribution in values}


class MLflowClientStub:
    """Very small stand-in for the MLflow client used in production."""

    def __init__(self) -> None:
        self._model_cache: Dict[str, DummyPolicyModel] = {}

    def load_latest_model(self, name: str) -> DummyPolicyModel:
        model = self._model_cache.get(name)
        if model is None:
            logger.debug("Loading new dummy model for %s", name)
            model = DummyPolicyModel(name)
            self._model_cache[name] = model
        return model


_client = MLflowClientStub()


def _model_name(account_id: str, symbol: str, variant: str | None = None) -> str:
    suffix = f"::{variant}" if variant else ""
    return f"policy-intent::{account_id}::{symbol}{suffix}".lower()


def predict_intent(
    account_id: str,
    symbol: str,
    features: Sequence[float],
    book_snapshot: BookSnapshot | Dict[str, float],
    model_variant: str | None = None,
    *,
    horizon: int | None = None,
) -> Intent:
    """Run inference against the latest MLflow model and return an intent.

    Any exception is converted into a null intent to keep the policy service
    responsive even when the model registry is unavailable.
    """

    try:
        snapshot = (
            book_snapshot
            if isinstance(book_snapshot, BookSnapshot)
            else BookSnapshot(**book_snapshot)
        )
        model_key = _model_name(account_id, symbol, model_variant)
        if horizon:
            model_key = f"{model_key}::h{int(horizon)}"
        model = _client.load_latest_model(model_key)
        return model.predict(features, snapshot, horizon=horizon)
    except Exception:  # pragma: no cover - defensive safety net
        logger.exception("Failed to generate intent for account=%s symbol=%s", account_id, symbol)
        return Intent.null()


def get_active_model(account_id: str, symbol: str) -> DummyPolicyModel:
    """Return the latest model instance for ``account_id`` and ``symbol``."""

    model_key = _model_name(account_id, symbol)
    return _client.load_latest_model(model_key)
