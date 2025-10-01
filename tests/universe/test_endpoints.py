from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.common.adapters import RedisFeastAdapter
from services.common.security import ADMIN_ACCOUNTS
from services.universe.main import app

from services.universe.repository import UniverseRepository



@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def reset_universe_repository() -> None:
    UniverseRepository.reset()
    yield
    UniverseRepository.reset()


@pytest.mark.parametrize("account_id", sorted(ADMIN_ACCOUNTS))
def test_get_universe_allows_admin_accounts(client: TestClient, account_id: str) -> None:

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

    assert body["instruments"] == expected_instruments
    assert body["fee_overrides"] == expected_overrides


def test_get_universe_rejects_non_admin(client: TestClient) -> None:
    response = client.get(
        "/universe/approved",
        headers={"X-Account-ID": "guest"},
    )


    assert response.status_code == 403


def test_universe_endpoint_filters_to_usd_when_timescale_empty(client: TestClient) -> None:

    UniverseRepository.seed_market_snapshots([])

    response = client.get(
        "/universe/approved",
        headers={"X-Account-ID": "admin-apac"},
    )

    assert response.status_code == 200
    body = response.json()

    adapter = RedisFeastAdapter(account_id="admin-apac")
    expected_instruments = adapter.approved_instruments()

    assert body["instruments"] == expected_instruments
    assert all(symbol.endswith("-USD") for symbol in body["instruments"])
    expected_overrides = {}
    for instrument in expected_instruments:
        override = adapter.fee_override(instrument)
        if override:
            expected_overrides[instrument] = override

    assert body["fee_overrides"] == expected_overrides
