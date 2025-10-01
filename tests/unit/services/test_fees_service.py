"""Unit tests for the fee schedule service."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Iterable

import pytest
from fastapi.testclient import TestClient

from services.fees import fee_service


@pytest.fixture
def fees_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    tiers = [
        SimpleNamespace(
            tier_id="tier_0",
            notional_threshold_usd=Decimal("0"),
            maker_bps=Decimal("10"),
            taker_bps=Decimal("20"),
            effective_from=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ),
        SimpleNamespace(
            tier_id="tier_1",
            notional_threshold_usd=Decimal("100000"),
            maker_bps=Decimal("8"),
            taker_bps=Decimal("18"),
            effective_from=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ),
    ]

    monkeypatch.setattr(fee_service, "_ordered_tiers", lambda session: list(tiers))

    volume = SimpleNamespace(
        notional_usd_30d=Decimal("150000"),
        updated_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
    )

    class FakeSession:
        def get(self, model, key):
            if model is fee_service.AccountVolume30d and key == "company":
                return volume
            return None

        def close(self) -> None:
            return None

    def override_session():
        session = FakeSession()
        try:
            yield session
        finally:
            session.close()

    fee_service.app.dependency_overrides[fee_service.get_session] = override_session
    fee_service.app.dependency_overrides[fee_service.require_admin_account] = lambda: "company"
    return TestClient(fee_service.app)


def teardown_module(module) -> None:  # type: ignore[override]
    fee_service.app.dependency_overrides.clear()


def test_fee_service_requires_admin_header() -> None:
    client = TestClient(fee_service.app)
    response = client.get("/fees/tiers")
    assert response.status_code == 403


def test_effective_fee_uses_volume_and_tiers(fees_client: TestClient) -> None:
    response = fees_client.get(
        "/fees/effective",
        params={"pair": "BTC/USD", "liquidity": "maker", "notional": 50_000},
        headers={"X-Account-ID": "company"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["bps"] == pytest.approx(8.0)
    assert body["tier_id"] == "tier_1"


def test_fee_tiers_endpoint_returns_configured_schedule(fees_client: TestClient) -> None:
    response = fees_client.get("/fees/tiers", headers={"X-Account-ID": "company"})
    assert response.status_code == 200
    tiers = response.json()
    assert len(tiers) == 2
    assert tiers[1]["tier_id"] == "tier_1"
