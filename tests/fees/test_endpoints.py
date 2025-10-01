from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS
from services.fees.main import app
from services.universe.repository import UniverseRepository


@pytest.fixture(autouse=True)
def reset_universe_repository() -> None:
    UniverseRepository.reset()
    yield
    UniverseRepository.reset()


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.mark.parametrize("account_id", sorted(ADMIN_ACCOUNTS))
def test_get_effective_fees_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    UniverseRepository.seed_fee_overrides(
        {
            "default": {"currency": "USD", "maker": 0.05, "taker": 0.1},
        }
    )

    response = client.get(
        "/fees/effective",
        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-ID": account_id},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id
    assert set(body.keys()) == {"account_id", "effective_from", "fee"}
    assert body["fee"]["currency"] == "USD"
    assert body["fee"]["maker"] == pytest.approx(0.05)
    assert body["fee"]["taker"] == pytest.approx(0.1)


def test_get_effective_fees_rejects_non_admin(client: TestClient) -> None:
    response = client.get(
        "/fees/effective",
        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-ID": "guest"},
    )

    assert response.status_code == 403
