import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from services.common.adapters import TimescaleAdapter
from services.risk.cvar_forecast import DEFAULT_SIMULATIONS
from services.risk.main import app


@pytest.fixture(autouse=True)
def reset_timescale() -> None:
    TimescaleAdapter.reset()
    yield
    TimescaleAdapter.reset()


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


def test_cvar_forecast_success(client: TestClient) -> None:
    adapter = TimescaleAdapter(account_id="company")
    adapter.record_instrument_exposure("BTC-USD", 150_000.0)

    response = client.get(
        "/risk/cvar",
        params={"account_id": "company", "horizon": "24h"},
        headers={"X-Account-ID": "company"},
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload["account_id"] == "company"
    assert payload["horizon"] == "24h"
    assert payload["simulations"] == DEFAULT_SIMULATIONS
    assert payload["var_95"] >= 0.0
    assert payload["cvar_95"] >= payload["var_95"]
    assert 0.0 <= payload["probability_loss_cap_hit"] <= 1.0
    assert payload["loss_cap"] >= 0.0

    distribution = payload["distribution"]
    for key in ("nav", "pnl", "loss"):
        assert key in distribution
        summary = distribution[key]
        assert {"mean", "std", "min", "max", "percentiles"}.issubset(summary.keys())
        percentiles = summary["percentiles"]
        for pct in ("1", "5", "25", "50", "75", "95", "99"):
            assert pct in percentiles

    assert payload["positions"]["BTC-USD"] == pytest.approx(150_000.0)

    history = TimescaleAdapter(account_id="company").cvar_results()
    assert history
    record = history[-1]
    assert record["horizon"] == "24h"
    assert record["account_id"] == "company"
    assert record["var95"] == pytest.approx(payload["var_95"])
    assert record["cvar95"] == pytest.approx(payload["cvar_95"])
    assert record["prob_cap_hit"] == pytest.approx(payload["probability_loss_cap_hit"])
    assert "ts" in record


def test_cvar_forecast_requires_admin(client: TestClient) -> None:
    response = client.get("/risk/cvar", params={"account_id": "company", "horizon": "24h"})
    assert response.status_code == 403


def test_cvar_forecast_account_mismatch(client: TestClient) -> None:
    response = client.get(
        "/risk/cvar",
        params={"account_id": "director-1", "horizon": "24h"},
        headers={"X-Account-ID": "company"},
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "Account mismatch between header and query parameter."


def test_cvar_forecast_invalid_horizon(client: TestClient) -> None:
    response = client.get(
        "/risk/cvar",
        params={"account_id": "company", "horizon": "bad"},
        headers={"X-Account-ID": "company"},
    )
    assert response.status_code == 400
    assert "Invalid horizon" in response.json()["detail"]
