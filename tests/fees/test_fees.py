from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List

import pytest
from fastapi.testclient import TestClient

import services.fees.main as fees_main
from services.common.security import ADMIN_ACCOUNTS
from services.fees.main import app


client = TestClient(app)


def _install_stub_adapters(
    monkeypatch: pytest.MonkeyPatch,
    *,
    tiers: List[Dict[str, float | str]],
    rolling: Dict[str, float | datetime],
) -> None:
    class StubRedisAdapter:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def fee_tiers(self, pair: str) -> List[Dict[str, float | str]]:
            return tiers

        def fee_override(self, instrument: str) -> Dict[str, float | str]:
            return {"currency": "USD", "maker": tiers[0]["maker_bps"], "taker": tiers[0]["taker_bps"]}

    class StubTimescaleAdapter:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def rolling_volume(self, pair: str) -> Dict[str, float | datetime]:
            return rolling

    monkeypatch.setattr(fees_main, "RedisFeastAdapter", StubRedisAdapter)
    monkeypatch.setattr(fees_main, "TimescaleAdapter", StubTimescaleAdapter)


def test_fees_effective_authorized_accounts(monkeypatch: pytest.MonkeyPatch) -> None:
    basis_ts = datetime(2024, 2, 1, 0, 0, tzinfo=timezone.utc)
    tiers = [
        {
            "tier_id": "starter",
            "volume_threshold": 0.0,
            "maker_bps": 1.2,
            "taker_bps": 2.4,
            "currency": "USD",
            "basis_ts": basis_ts,
        }
    ]
    rolling = {"notional": 10_000.0, "basis_ts": basis_ts}
    _install_stub_adapters(monkeypatch, tiers=tiers, rolling=rolling)

    for account in ADMIN_ACCOUNTS:
        response = client.get(
            "/fees/effective",
            params={"pair": "BTC-USD", "liquidity": "maker", "notional": 50_000},
            headers={"X-Account-ID": account},
        )
        assert response.status_code == 200
        data = response.json()
        assert set(data.keys()) == {"account_id", "effective_from", "fee"}
        assert data["account_id"] == account
        assert data["fee"]["currency"] == "USD"
        assert data["fee"]["maker"] == pytest.approx(1.2)
        assert data["fee"]["maker_detail"]["usd"] == pytest.approx(50_000 * 1.2 / 10_000)


def test_fees_effective_rejects_non_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    tiers = [
        {
            "tier_id": "starter",
            "volume_threshold": 0.0,
            "maker_bps": 1.0,
            "taker_bps": 2.0,
            "currency": "USD",
            "basis_ts": datetime.now(timezone.utc),
        }
    ]
    rolling = {"notional": 0.0, "basis_ts": datetime.now(timezone.utc)}
    _install_stub_adapters(monkeypatch, tiers=tiers, rolling=rolling)

    response = client.get(
        "/fees/effective",
        params={"pair": "BTC-USD", "liquidity": "maker", "notional": 10_000},
        headers={"X-Account-ID": "shadow"},
    )
    assert response.status_code == 403
