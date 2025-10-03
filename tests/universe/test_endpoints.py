from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.common.adapters import RedisFeastAdapter
from services.common.security import ADMIN_ACCOUNTS
from services.universe.main import app

from services.universe.repository import UniverseRepository
from tests.universe.conftest import UniverseTimescaleFixture



@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def configure_repository(universe_timescale: UniverseTimescaleFixture) -> None:
    UniverseRepository.reset()
    universe_timescale.rebind()
    universe_timescale.clear()
    yield
    UniverseRepository.reset()
    universe_timescale.rebind()
    universe_timescale.clear()


@pytest.mark.parametrize("account_id", sorted(ADMIN_ACCOUNTS))
def test_get_universe_allows_admin_accounts(
    client: TestClient, account_id: str, universe_timescale: UniverseTimescaleFixture
) -> None:
    universe_timescale.add_snapshot(
        base_asset="BTC",
        quote_asset="USD",
        market_cap=1.2e12,
        global_volume_24h=5.0e10,
        kraken_volume_24h=2.5e10,
        volatility_30d=0.45,
    )
    universe_timescale.add_fee_override(
        "BTC-USD", currency="USD", maker=0.1, taker=0.2
    )

    adapter = RedisFeastAdapter(account_id=account_id)
    expected_instruments = adapter.approved_instruments()
    expected_overrides = {}
    for instrument in expected_instruments:
        override = adapter.fee_override(instrument)
        if override:
            expected_overrides[instrument] = override

    response = client.get(
        "/universe/approved",
        headers={"X-Account-ID": account_id},
    )


    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id

    assert isinstance(body["instruments"], list)
    assert all(symbol.split("-")[-1] in {"USD", "USDT"} for symbol in body["instruments"])
    assert isinstance(body["fee_overrides"], dict)



def test_get_universe_rejects_non_admin(client: TestClient) -> None:
    response = client.get(
        "/universe/approved",
        headers={"X-Account-ID": "guest"},
    )


    assert response.status_code == 403



@pytest.mark.parametrize("account_id", sorted(ADMIN_ACCOUNTS))
def test_universe_endpoint_uses_fallback_when_timescale_empty(
    client: TestClient, account_id: str, universe_timescale: UniverseTimescaleFixture
) -> None:

    universe_timescale.clear()

    response = client.get(
        "/universe/approved",
        headers={"X-Account-ID": account_id},
    )

    assert response.status_code == 200
    body = response.json()

    adapter = RedisFeastAdapter(account_id=account_id)
    expected_instruments = adapter.approved_instruments()

    assert body["instruments"] == expected_instruments
    assert all(symbol.endswith("-USD") for symbol in body["instruments"])
    expected_overrides = {}
    for instrument in expected_instruments:
        override = adapter.fee_override(instrument)
        if override:
            expected_overrides[instrument] = override

    assert body["fee_overrides"] == expected_overrides

