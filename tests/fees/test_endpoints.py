from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List

import pytest
from fastapi.testclient import TestClient

import services.fees.main as fees_main
from services.common.security import ADMIN_ACCOUNTS
from services.fees.main import app


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(app)


@pytest.mark.parametrize("account_id", sorted(ADMIN_ACCOUNTS))
def test_get_effective_fees_returns_tiered_breakdown(
    monkeypatch: pytest.MonkeyPatch, client: TestClient, account_id: str
) -> None:
    basis_ts = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)

    class StubRedisAdapter:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def fee_tiers(self, pair: str) -> List[Dict[str, float | str]]:
            assert pair == "BTC-USD"
            return [
                {
                    "tier_id": "starter",
                    "volume_threshold": 0.0,
                    "maker_bps": 2.5,
                    "taker_bps": 3.5,
                    "currency": "USD",
                    "basis_ts": basis_ts,
                }
            ]

        def fee_override(self, instrument: str) -> Dict[str, float | str]:
            return {"currency": "USD", "maker": 2.5, "taker": 3.5}

    class StubTimescaleAdapter:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def rolling_volume(self, pair: str) -> Dict[str, float | datetime]:
            assert pair == "BTC-USD"
            return {"notional": 150_000.0, "basis_ts": basis_ts}

    monkeypatch.setattr(fees_main, "RedisFeastAdapter", StubRedisAdapter)
    monkeypatch.setattr(fees_main, "TimescaleAdapter", StubTimescaleAdapter)

    response = client.get(
        "/fees/effective",
        params={"pair": "BTC-USD", "liquidity": "maker", "notional": 250_000},
        headers={"X-Account-ID": account_id},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == account_id
    fee = body["fee"]
    assert fee["currency"] == "USD"
    assert fee["maker"] == pytest.approx(2.5)
    assert fee["taker"] == pytest.approx(3.5)
    assert fee["maker_detail"]["bps"] == pytest.approx(2.5)
    assert fee["maker_detail"]["usd"] == pytest.approx(250_000 * 2.5 / 10_000)
    assert fee["maker_detail"]["tier_id"] == "starter"
    returned_basis = fee["maker_detail"]["basis_ts"]
    parsed_basis = datetime.fromisoformat(returned_basis.replace("Z", "+00:00"))
    assert parsed_basis == basis_ts
    assert fee["taker_detail"]["usd"] == pytest.approx(250_000 * 3.5 / 10_000)


def test_get_effective_fees_rejects_non_admin(client: TestClient) -> None:
    response = client.get(
        "/fees/effective",
        params={"pair": "BTC-USD", "liquidity": "maker", "notional": 10_000},
        headers={"X-Account-ID": "guest"},
    )

    assert response.status_code == 403


def test_get_effective_fees_transitions_between_tiers(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    basis_ts = datetime(2024, 3, 1, 15, 0, tzinfo=timezone.utc)

    class StubRedisAdapter:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def fee_tiers(self, pair: str) -> List[Dict[str, float | str]]:
            return [
                {
                    "tier_id": "standard",
                    "volume_threshold": 0.0,
                    "maker_bps": 4.0,
                    "taker_bps": 6.0,
                    "currency": "USD",
                },
                {
                    "tier_id": "vip",
                    "volume_threshold": 1_000_000.0,
                    "maker_bps": 2.0,
                    "taker_bps": 4.0,
                    "currency": "USD",
                },
            ]

        def fee_override(self, instrument: str) -> Dict[str, float | str]:
            return {"currency": "USD", "maker": 4.0, "taker": 6.0}

    class StubTimescaleAdapter:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def rolling_volume(self, pair: str) -> Dict[str, float | datetime]:
            return {"notional": 900_000.0, "basis_ts": basis_ts}

    monkeypatch.setattr(fees_main, "RedisFeastAdapter", StubRedisAdapter)
    monkeypatch.setattr(fees_main, "TimescaleAdapter", StubTimescaleAdapter)

    first = client.get(
        "/fees/effective",

        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-Id": "company"},

    )
    assert first.status_code == 200
    first_body = first.json()["fee"]
    assert first_body["maker"] == pytest.approx(4.0)
    assert first_body["maker_detail"]["tier_id"] == "standard"

    second = client.get(
        "/fees/effective",

        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-Id": "company"},

    )
    assert second.status_code == 200
    second_body = second.json()["fee"]
    assert second_body["taker"] == pytest.approx(4.0)
    assert second_body["taker_detail"]["tier_id"] == "vip"


def test_get_effective_fees_uses_fallback_when_schedule_missing(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    class StubRedisAdapter:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def fee_tiers(self, pair: str) -> List[Dict[str, float | str]]:
            return []

        def fee_override(self, instrument: str) -> Dict[str, float | str]:
            if instrument == "default":
                return {"currency": "USD", "maker": 1.5, "taker": 2.0}
            return {}

    class StubTimescaleAdapter:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def rolling_volume(self, pair: str) -> Dict[str, float | datetime]:
            return {"notional": 0.0, "basis_ts": datetime.now(timezone.utc)}

    monkeypatch.setattr(fees_main, "RedisFeastAdapter", StubRedisAdapter)
    monkeypatch.setattr(fees_main, "TimescaleAdapter", StubTimescaleAdapter)

    response = client.get(
        "/fees/effective",

        params={"isolation_segment": "seg-fees", "fee_tier": "standard"},
        headers={"X-Account-Id": "director-1"},
    )

    assert response.status_code == 200
    assert repositories and repositories[-1].account_id == "director-1"
    assert repositories[-1].fee_override_calls == ["default"]
    payload = response.json()
    assert payload["fee"]["maker"] == 0.25
    assert payload["fee"]["taker"] == 0.4

