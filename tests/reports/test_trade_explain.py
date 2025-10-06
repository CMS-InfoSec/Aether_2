from __future__ import annotations

import logging
from typing import Dict

import pytest
from fastapi.testclient import TestClient


pytest.importorskip("pandas", exc_type=ImportError)

import pandas as pd
import report_service
from services.common.security import require_admin_account


class StubModel:
    def __init__(self, shap_values: Dict[str, float]) -> None:
        self._shap_values = shap_values

    def explain(self, features: Dict[str, float]) -> Dict[str, float]:
        # ignore provided features and return the predetermined attributions
        return self._shap_values


def test_trade_explain_requires_authentication(monkeypatch) -> None:
    monkeypatch.setattr(
        report_service,
        "_load_trade_record",
        lambda trade_id: {"fill_id": trade_id, "account_id": "alpha", "instrument": "BTC-USD"},
    )

    client = TestClient(report_service.app)
    response = client.get("/reports/trade_explain", params={"trade_id": "fill-unauth"})

    assert response.status_code == 401


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
    client.app.dependency_overrides[require_admin_account] = lambda: "alpha"
    try:
        response = client.get("/reports/trade_explain", params={"trade_id": "fill-123"})
    finally:
        client.app.dependency_overrides.pop(require_admin_account, None)
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


def test_trade_explain_rejects_mismatched_account(monkeypatch) -> None:
    trade_record = {"fill_id": "fill-403", "account_id": "beta", "instrument": "ETH-USD", "features": {}}

    monkeypatch.setattr(report_service, "_load_trade_record", lambda trade_id: trade_record)

    client = TestClient(report_service.app)
    client.app.dependency_overrides[require_admin_account] = lambda: "alpha"
    try:
        response = client.get("/reports/trade_explain", params={"trade_id": "fill-403"})
    finally:
        client.app.dependency_overrides.pop(require_admin_account, None)

    assert response.status_code == 403
    assert response.json()["detail"] == "Authenticated account is not authorized for the requested account."


def test_trade_explain_missing_features_returns_422(monkeypatch) -> None:
    trade_record = {"fill_id": "fill-404", "account_id": "beta", "instrument": "ETH-USD"}

    monkeypatch.setattr(report_service, "_load_trade_record", lambda trade_id: trade_record)

    client = TestClient(report_service.app)
    client.app.dependency_overrides[require_admin_account] = lambda: "beta"
    try:
        response = client.get("/reports/trade_explain", params={"trade_id": "fill-404"})
    finally:
        client.app.dependency_overrides.pop(require_admin_account, None)
    assert response.status_code == 422
    assert response.json()["detail"] == "Trade is missing feature metadata"


def test_trade_explain_rejects_non_spot_instrument(monkeypatch) -> None:
    trade_record = {"fill_id": "fill-spot-422", "account_id": "alpha", "instrument": "BTC-PERP"}

    monkeypatch.setattr(report_service, "_load_trade_record", lambda trade_id: trade_record)

    client = TestClient(report_service.app)
    client.app.dependency_overrides[require_admin_account] = lambda: "alpha"
    try:
        response = client.get("/reports/trade_explain", params={"trade_id": "fill-spot-422"})
    finally:
        client.app.dependency_overrides.pop(require_admin_account, None)

    assert response.status_code == 422
    assert (
        response.json()["detail"]
        == "instrument 'BTC-PERP' is not a supported USD spot market instrument"
    )


def test_trade_explain_normalises_instrument(monkeypatch) -> None:
    trade_record = {
        "fill_id": "fill-spot-normalise",
        "account_id": "alpha",
        "instrument": "eth/usd",
        "order_metadata": {"features": {"alpha": 1.0}},
    }

    shap_values = {"alpha": 0.5}

    monkeypatch.setattr(report_service, "_load_trade_record", lambda trade_id: trade_record)
    monkeypatch.setattr(report_service, "get_active_model", lambda account_id, instrument: StubModel(shap_values))

    client = TestClient(report_service.app)
    client.app.dependency_overrides[require_admin_account] = lambda: "alpha"
    try:
        response = client.get("/reports/trade_explain", params={"trade_id": "fill-spot-normalise"})
    finally:
        client.app.dependency_overrides.pop(require_admin_account, None)

    assert response.status_code == 200
    assert response.json()["instrument"] == "ETH-USD"


def test_filter_spot_instruments_drops_derivatives(caplog) -> None:
    frame = pd.DataFrame(
        {
            "instrument": ["BTC-USD", "ETH-PERP", "eth_usd", "ADAUP-USD", None],
            "size": [1, 2, 3, 4, 5],
            "price": [10, 20, 30, 40, 50],
            "fee": [0, 0, 0, 0, 0],
            "side": ["buy"] * 5,
        }
    )

    with caplog.at_level(logging.WARNING):
        filtered = report_service._filter_spot_instruments(frame, context="unit-test")

    assert filtered["instrument"].tolist() == ["BTC-USD", "ETH-USD"]
    assert "Dropping non-spot instruments" in caplog.text
