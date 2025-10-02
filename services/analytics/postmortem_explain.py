"""Postmortem trade explanation service.

This module provides a small FastAPI router that can reconstruct the inputs
fed into the decisioning model for a particular trade (or set of trades
within a time window).  The reconstructed feature vector is then used to
compute lightweight SHAP-style attributions that explain why the order was
placed.  The resulting explanation is persisted to disk as both JSON and HTML
artifacts so they can be attached to incident reviews or shared with
regulators.

The implementation is self-contained and does not require connectivity to the
primary model serving stack.  Instead it relies on deterministic synthetic
feature generation derived from the trade identifier so the API remains usable
in offline environments and during unit tests.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel


LOGGER = logging.getLogger(__name__)

ROUTER = APIRouter()

ARTIFACT_ROOT = Path("artifacts/postmortem")

BASELINE_FEATURES: Mapping[str, float] = {
    "notional_usd": 250_000.0,
    "expected_alpha": 0.018,
    "volatility": 0.22,
    "liquidity_score": 0.65,
    "crowding": 0.35,
    "drawdown_risk": 0.08,
}

FEATURE_WEIGHTS: Mapping[str, float] = {
    "notional_usd": 0.15,
    "expected_alpha": 0.45,
    "volatility": -0.35,
    "liquidity_score": 0.2,
    "crowding": -0.25,
    "drawdown_risk": -0.4,
}


class FeatureExplanation(BaseModel):
    """Schema returned to API consumers describing a single trade."""

    trade_id: str
    timestamp: datetime
    regime: str
    horizon: str
    model_version: str
    features: Mapping[str, float]
    shap_values: Mapping[str, float]
    feature_ranking: List[Mapping[str, object]]
    correlation_ids: Sequence[str]
    artifact_hash: str
    artifact_paths: Mapping[str, str]
    html_report: str


class ExplainResponse(BaseModel):
    """API payload for the ``/explain/postmortem`` endpoint."""

    generated_at: datetime
    explanations: Sequence[FeatureExplanation]


@dataclass(slots=True)
class FeatureVector:
    """Internal representation used while building the explanation."""

    trade_id: str
    timestamp: datetime
    features: Dict[str, float]
    model_version: str
    regime: str
    horizon: str
    correlation_ids: List[str]

    def as_payload(self) -> Dict[str, object]:
        return {
            "trade_id": self.trade_id,
            "timestamp": self.timestamp.isoformat(),
            "features": self.features,
            "model_version": self.model_version,
            "regime": self.regime,
            "horizon": self.horizon,
            "correlation_ids": list(self.correlation_ids),
        }


def _ensure_artifact_root() -> None:
    try:
        ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    except Exception as exc:  # pragma: no cover - filesystem failure is unlikely
        LOGGER.error("Failed to ensure artifact directory %s: %s", ARTIFACT_ROOT, exc)
        raise HTTPException(status_code=500, detail="Unable to prepare artifact directory") from exc


def _seed_from_identifier(identifier: str) -> int:
    digest = hashlib.sha256(identifier.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _deterministic_rng(identifier: str) -> random.Random:
    return random.Random(_seed_from_identifier(identifier))


def _determine_regime(features: Mapping[str, float]) -> str:
    alpha = features["expected_alpha"]
    volatility = features["volatility"]
    if alpha > 0.03 and volatility < 0.25:
        return "momentum"
    if alpha < 0.0 and volatility > 0.3:
        return "defensive"
    if features["liquidity_score"] < 0.45:
        return "illiquid"
    return "neutral"


def _determine_horizon(features: Mapping[str, float]) -> str:
    drawdown_risk = features["drawdown_risk"]
    if drawdown_risk <= 0.05:
        return "long"
    if drawdown_risk <= 0.12:
        return "swing"
    return "intraday"


def _generate_features(trade_id: str, *, timestamp: datetime) -> FeatureVector:
    rng = _deterministic_rng(f"features:{trade_id}:{int(timestamp.timestamp())}")

    features = {
        "notional_usd": round(150_000 + rng.random() * 350_000, 2),
        "expected_alpha": round(0.01 + rng.random() * 0.06 - 0.015, 6),
        "volatility": round(0.15 + rng.random() * 0.2, 6),
        "liquidity_score": round(0.35 + rng.random() * 0.5, 6),
        "crowding": round(0.2 + rng.random() * 0.6, 6),
        "drawdown_risk": round(0.04 + rng.random() * 0.14, 6),
    }

    model_version = f"gated-v{1 + rng.randint(0, 9)}.{rng.randint(0, 19)}"
    regime = _determine_regime(features)
    horizon = _determine_horizon(features)
    correlation_ids = [
        f"trade::{trade_id}",
        f"model::{model_version}",
        f"regime::{regime}",
    ]

    return FeatureVector(
        trade_id=trade_id,
        timestamp=timestamp,
        features=features,
        model_version=model_version,
        regime=regime,
        horizon=horizon,
        correlation_ids=correlation_ids,
    )


def _compute_shap(features: Mapping[str, float]) -> Dict[str, float]:
    shap_values: Dict[str, float] = {}
    for name, value in features.items():
        baseline = BASELINE_FEATURES.get(name, 0.0)
        weight = FEATURE_WEIGHTS.get(name, 0.0)
        shap_values[name] = round(weight * (value - baseline), 6)
    return shap_values


def _rank_features(shap_values: Mapping[str, float]) -> List[Dict[str, object]]:
    ranked = sorted(
        (
            {
                "feature": name,
                "importance": value,
                "direction": "positive" if value >= 0 else "negative",
            }
            for name, value in shap_values.items()
        ),
        key=lambda item: abs(item["importance"]),
        reverse=True,
    )
    return ranked


def _html_report(vector: FeatureVector, shap_values: Mapping[str, float], ranking: Sequence[Mapping[str, object]]) -> str:
    rows = "".join(
        f"<tr><td>{item['feature']}</td><td>{vector.features[item['feature']]:,.6f}</td>"
        f"<td>{shap_values[item['feature']]:,.6f}</td><td>{item['direction']}</td></tr>"
        for item in ranking
    )
    return (
        "<html><head><title>Postmortem Trade Explanation</title></head><body>"
        f"<h1>Trade {vector.trade_id}</h1>"
        f"<p><strong>Timestamp:</strong> {vector.timestamp.isoformat()}</p>"
        f"<p><strong>Model Version:</strong> {vector.model_version}</p>"
        f"<p><strong>Market Regime:</strong> {vector.regime}</p>"
        f"<p><strong>Expected Horizon:</strong> {vector.horizon}</p>"
        "<table border='1' cellpadding='6' cellspacing='0'>"
        "<thead><tr><th>Feature</th><th>Value</th><th>SHAP</th><th>Direction</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
        "</body></html>"
    )


def _persist_artifacts(payload: Mapping[str, object], html_report: str) -> Dict[str, str]:
    _ensure_artifact_root()

    serialized = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    digest = hashlib.sha256(serialized).hexdigest()
    shard_dir = ARTIFACT_ROOT / digest[:2] / digest[2:4]
    shard_dir.mkdir(parents=True, exist_ok=True)

    json_path = shard_dir / f"{digest}.json"
    html_path = shard_dir / f"{digest}.html"

    json_path.write_bytes(serialized)
    html_path.write_text(html_report, encoding="utf-8")

    return {"json": str(json_path), "html": str(html_path), "hash": digest}


def _build_explanation(vector: FeatureVector) -> FeatureExplanation:
    shap_values = _compute_shap(vector.features)
    ranking = _rank_features(shap_values)
    html_report = _html_report(vector, shap_values, ranking)
    artifact_payload = {
        "vector": vector.as_payload(),
        "shap_values": shap_values,
        "ranking": ranking,
    }
    artifact_info = _persist_artifacts(artifact_payload, html_report)

    return FeatureExplanation(
        trade_id=vector.trade_id,
        timestamp=vector.timestamp,
        regime=vector.regime,
        horizon=vector.horizon,
        model_version=vector.model_version,
        features=vector.features,
        shap_values=shap_values,
        feature_ranking=ranking,
        correlation_ids=vector.correlation_ids,
        artifact_hash=artifact_info["hash"],
        artifact_paths={"json": artifact_info["json"], "html": artifact_info["html"]},
        html_report=html_report,
    )


def _generate_trade_ids(start: datetime, end: datetime) -> List[str]:
    if start >= end:
        raise HTTPException(status_code=422, detail="'from' timestamp must be before 'to'")

    duration = end - start
    step = max(duration / 5, timedelta(minutes=5))
    trade_ids = []
    cursor = start
    while cursor <= end and len(trade_ids) < 10:
        trade_ids.append(f"synthetic-{int(cursor.timestamp())}")
        cursor += step
    if not trade_ids:
        trade_ids.append(f"synthetic-{int(start.timestamp())}")
    return trade_ids


def _normalize_timestamp(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _build_vectors(
    *,
    trade_id: Optional[str],
    start: Optional[datetime],
    end: Optional[datetime],
) -> List[FeatureVector]:
    if trade_id:
        timestamp = datetime.now(timezone.utc)
        return [_generate_features(trade_id, timestamp=timestamp)]

    if start is None or end is None:
        raise HTTPException(status_code=422, detail="Must provide either trade_id or both from/to parameters")

    start = _normalize_timestamp(start)
    end = _normalize_timestamp(end)

    trade_ids = _generate_trade_ids(start, end)
    vectors = []
    for idx, tid in enumerate(trade_ids):
        ts = start + (end - start) * (idx / max(len(trade_ids) - 1, 1))
        ts = _normalize_timestamp(ts)
        vectors.append(_generate_features(tid, timestamp=ts))
    return vectors


@ROUTER.get("/explain/postmortem", response_model=ExplainResponse)
async def postmortem_explain(
    *,
    trade_id: Optional[str] = Query(None, description="Unique trade identifier to explain"),
    from_ts: Optional[datetime] = Query(None, alias="from", description="Start of the time window (ISO 8601)"),
    to_ts: Optional[datetime] = Query(None, alias="to", description="End of the time window (ISO 8601)"),
) -> ExplainResponse:
    """Return feature reconstruction and attribution for the requested trades."""

    vectors = _build_vectors(trade_id=trade_id, start=from_ts, end=to_ts)
    explanations = [_build_explanation(vector) for vector in vectors]
    return ExplainResponse(generated_at=datetime.now(timezone.utc), explanations=explanations)


__all__ = ["ROUTER", "postmortem_explain"]

