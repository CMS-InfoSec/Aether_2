import logging
from collections.abc import Iterator

import pytest
pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

import model_server


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(model_server.app) as test_client:
        yield test_client


def test_predict_allows_authorized_admin(client: TestClient, caplog: pytest.LogCaptureFixture) -> None:
    payload = {
        "account_id": "ACC-1",
        "symbol": "BTC-USD",
        "features": {"momentum": 1.0, "volatility": 0.5},
        "book_snapshot": {"spread": 10.0, "mid": 20000.0},
    }
    headers = {"X-Account-ID": "company"}

    with caplog.at_level(logging.INFO, logger="model_server"):
        response = client.post("/models/predict", json=payload, headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["action"] in {"hold", "enter"}

    audit_records = [record for record in caplog.records if record.getMessage() == "inference_log"]
    assert audit_records, "Expected inference audit log entry"
    assert audit_records[-1].actor_account == "company"


def test_predict_rejects_missing_credentials(client: TestClient) -> None:
    payload = {
        "account_id": "ACC-1",
        "symbol": "BTC-USD",
        "features": {"momentum": 1.0},
        "book_snapshot": {},
    }

    response = client.post("/models/predict", json=payload)

    assert response.status_code == 401


def test_active_model_requires_admin_identity(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    headers = {"X-Account-ID": "company"}

    with caplog.at_level(logging.INFO, logger="model_server"):
        response = client.get("/models/active", params={"symbol": "BTC-USD"}, headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTC-USD"

    audit_records = [record for record in caplog.records if record.getMessage() == "active_model_lookup"]
    assert audit_records, "Expected active model lookup audit log entry"
    assert audit_records[-1].actor_account == "company"


def test_active_model_rejects_missing_credentials(client: TestClient) -> None:
    response = client.get("/models/active", params={"symbol": "BTC-USD"})

    assert response.status_code == 401
