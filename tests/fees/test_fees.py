from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS



@pytest.fixture(name="client")
def client_fixture(fees_app) -> TestClient:
    return TestClient(fees_app)


def _install_stub_adapters(
    monkeypatch: pytest.MonkeyPatch,
    fee_service_module,
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

    monkeypatch.setattr(fee_service_module, "RedisFeastAdapter", StubRedisAdapter)
    monkeypatch.setattr(fee_service_module, "TimescaleAdapter", StubTimescaleAdapter)


def test_fees_effective_authorized_accounts(
    monkeypatch: pytest.MonkeyPatch, fee_service_module, client: TestClient
) -> None:
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
    _install_stub_adapters(monkeypatch, fee_service_module, tiers=tiers, rolling=rolling)

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


def test_fees_effective_rejects_non_admin(
    monkeypatch: pytest.MonkeyPatch, fee_service_module, client: TestClient
) -> None:
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
    _install_stub_adapters(monkeypatch, fee_service_module, tiers=tiers, rolling=rolling)

    response = client.get(
        "/fees/effective",
        params={"pair": "BTC-USD", "liquidity": "maker", "notional": 10_000},
        headers={"X-Account-ID": "shadow"},
    )
    assert response.status_code == 403
