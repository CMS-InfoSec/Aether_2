from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from auth.service import InMemoryAdminRepository

from app import create_app
from auth.service import InMemorySessionStore
from policy_service import RegimeSnapshot, _reset_regime_state, regime_classifier
from services.models.meta_learner import get_meta_learner, meta_governance_log


def setup_function() -> None:
    get_meta_learner().reset()
    meta_governance_log.reset()
    _reset_regime_state()


def test_meta_learner_prefers_best_regime_model() -> None:
    learner = get_meta_learner()
    now = datetime.now(timezone.utc)
    learner.record_performance(
        symbol="BTC-USD",
        regime="trend",
        model="trend_model",
        score=1.4,
        ts=now - timedelta(minutes=10),
    )
    learner.record_performance(
        symbol="BTC-USD",
        regime="trend",
        model="trend_model",
        score=1.8,
        ts=now - timedelta(minutes=1),
    )
    learner.record_performance(
        symbol="BTC-USD",
        regime="trend",
        model="meanrev_model",
        score=0.2,
        ts=now - timedelta(minutes=5),
    )

    weights = learner.predict_weights("BTC-USD", "trend")

    assert set(weights) == {"meanrev_model", "trend_model", "vol_breakout"}
    assert weights["trend_model"] > weights["meanrev_model"]
    assert math.isclose(sum(weights.values()), 1.0, rel_tol=1e-6)


def test_meta_weights_endpoint_logs_governance_record() -> None:
    learner = get_meta_learner()
    now = datetime.now(timezone.utc)
    learner.record_performance(
        symbol="ETH-USD",
        regime="range",
        model="meanrev_model",
        score=0.9,
        ts=now - timedelta(minutes=15),
    )
    learner.record_performance(
        symbol="ETH-USD",
        regime="range",
        model="trend_model",
        score=0.4,
        ts=now - timedelta(minutes=7),
    )

    snapshot = RegimeSnapshot(
        symbol="ETH-USD",
        regime="range",
        volatility=0.01,
        trend_strength=0.2,
        feature_scale=1.0,
        size_scale=0.85,
        sample_count=30,
        updated_at=now,
    )
    with regime_classifier._lock:  # type: ignore[attr-defined]
        regime_classifier._snapshots["ETH-USD"] = snapshot  # type: ignore[attr-defined]


    app = create_app(admin_repository=InMemoryAdminRepository())

    client = TestClient(app)

    response = client.get(
        "/meta/weights",
        params={"symbol": "ETH-USD"},
        headers={"X-Account-ID": "company"},
    )
    assert response.status_code == 200

    payload = response.json()
    assert payload["symbol"] == "ETH-USD"
    assert payload["regime"] == "range"
    weights = payload["weights"]
    assert math.isclose(sum(weights.values()), 1.0, rel_tol=1e-6)
    assert weights["meanrev_model"] > weights["trend_model"]

    records = meta_governance_log.records("ETH-USD")
    assert records, "expected governance entry to be recorded"
    logged_weights = json.loads(records[-1].weights_json)
    assert logged_weights.keys() == weights.keys()


def test_meta_learner_rejects_derivative_symbol() -> None:
    learner = get_meta_learner()

    with pytest.raises(ValueError, match="spot market"):
        learner.record_performance(
            symbol="BTC-PERP",
            regime="trend",
            model="trend_model",
            score=1.2,
        )


def test_meta_governance_log_rejects_non_spot_requests() -> None:
    with pytest.raises(ValueError, match="spot market"):
        meta_governance_log.records("ETH-PERP")
