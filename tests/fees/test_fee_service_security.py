from __future__ import annotations

from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from services.fees.main import app


@pytest.fixture(name="client")
def client_fixture() -> Iterator[TestClient]:
    with TestClient(app) as client:
        yield client


def test_get_effective_fee_requires_admin_header(client: TestClient) -> None:
    response = client.get(
        "/fees/effective",
        params={"pair": "BTC-USD", "liquidity": "maker", "notional": 1000},
    )

    assert response.status_code == 403


def test_get_effective_fee_rejects_non_admin(client: TestClient) -> None:
    response = client.get(
        "/fees/effective",
        params={"pair": "BTC-USD", "liquidity": "maker", "notional": 1000},
        headers={"X-Account-ID": "guest"},
    )

    assert response.status_code == 403


@pytest.mark.parametrize("admin_account", ["company", "director-1", "director-2"])
def test_get_effective_fee_allows_admin_header(
    client: TestClient, admin_account: str
) -> None:
    response = client.get(
        "/fees/effective",
        params={"pair": "BTC-USD", "liquidity": "maker", "notional": 1000},
        headers={"X-Account-ID": admin_account},
    )

    assert response.status_code == 200
    payload = response.json()
    assert set(payload.keys()) == {"bps", "usd", "tier_id", "basis_ts"}


def test_get_fee_tiers_require_admin_header(client: TestClient) -> None:
    response = client.get("/fees/tiers")

    assert response.status_code == 403


def test_get_fee_tiers_rejects_non_admin(client: TestClient) -> None:
    response = client.get("/fees/tiers", headers={"X-Account-ID": "guest"})

    assert response.status_code == 403


def test_get_fee_tiers_returns_data_for_admin(client: TestClient) -> None:
    response = client.get("/fees/tiers", headers={"X-Account-ID": "company"})

    assert response.status_code == 200
    tiers = response.json()
    assert isinstance(tiers, list)
    assert tiers, "Seeded tiers should be available"


def test_get_volume_30d_requires_admin_header(client: TestClient) -> None:
    response = client.get("/fees/volume30d")

    assert response.status_code == 403


def test_get_volume_30d_rejects_non_admin(client: TestClient) -> None:
    response = client.get("/fees/volume30d", headers={"X-Account-ID": "guest"})

    assert response.status_code == 403


def test_get_volume_30d_returns_payload_for_admin(client: TestClient) -> None:
    response = client.get("/fees/volume30d", headers={"X-Account-ID": "company"})

    assert response.status_code == 200
    payload = response.json()
    assert set(payload.keys()) == {"notional_usd_30d", "updated_at"}
