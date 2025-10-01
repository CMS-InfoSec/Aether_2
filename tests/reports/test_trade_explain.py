from __future__ import annotations

from typing import Dict

from fastapi.testclient import TestClient
import pytest


pytest.importorskip("pandas", exc_type=ImportError)

import report_service


class StubModel:
    def __init__(self, shap_values: Dict[str, float]) -> None:
        self._shap_values = shap_values

    def explain(self, features: Dict[str, float]) -> Dict[str, float]:
        # ignore provided features and return the predetermined attributions
        return self._shap_values


def test_trade_explain_returns_sorted_top_features(monkeypatch) -> None:
    trade_record = {
        "fill_id": "fill-123",
        "account_id": "alpha",
        "instrument": "BTC-USD",
        "order_metadata": {
            "features": {
                "alpha": 1.0,
                "beta": 2.0,
                "gamma": 3.0,
                "delta": 4.0,
                "epsilon": 5.0,
                "zeta": 6.0,
            },
            "model_version": "2024.06.01",
        },
    }

    shap_values = {
        "alpha": 0.1,
        "beta": -0.2,
        "gamma": 0.05,
        "delta": -0.6,
        "epsilon": 0.3,
        "zeta": -0.4,
    }

    monkeypatch.setattr(report_service, "_load_trade_record", lambda trade_id: trade_record)
    monkeypatch.setattr(report_service, "get_active_model", lambda account_id, instrument: StubModel(shap_values))

    client = TestClient(report_service.app)
    response = client.get("/reports/trade_explain", params={"trade_id": "fill-123"})
    assert response.status_code == 200

    payload = response.json()
    assert payload["trade_id"] == "fill-123"
    assert payload["account_id"] == "alpha"
    assert payload["instrument"] == "BTC-USD"
    assert payload["model_version"] == "2024.06.01"

    feature_importance = payload["feature_importance"]
    assert len(feature_importance) == 6
    assert feature_importance[0]["feature"] == "delta"
    assert feature_importance[0]["importance"] == -0.6
    assert feature_importance[-1]["feature"] == "gamma"

    top_features = payload["top_features"]
    assert len(top_features) == 5
    assert [entry["feature"] for entry in top_features] == [
        "delta",
        "zeta",
        "epsilon",
        "beta",
        "alpha",
    ]


def test_trade_explain_missing_features_returns_422(monkeypatch) -> None:
    trade_record = {"fill_id": "fill-404", "account_id": "beta", "instrument": "ETH-USD"}

    monkeypatch.setattr(report_service, "_load_trade_record", lambda trade_id: trade_record)

    client = TestClient(report_service.app)
    response = client.get("/reports/trade_explain", params={"trade_id": "fill-404"})
    assert response.status_code == 422
    assert response.json()["detail"] == "Trade is missing feature metadata"
