from __future__ import annotations


import pytest

from services.common.adapters import TimescaleAdapter



@pytest.fixture(autouse=True)
def reset_timescale() -> None:
    TimescaleAdapter.reset()
    TimescaleAdapter.reset_rotation_state()
    yield

    TimescaleAdapter.reset()
    TimescaleAdapter.reset_rotation_state()



def test_record_daily_usage_accumulates() -> None:
    adapter = TimescaleAdapter(account_id="admin-eu")

    adapter.record_daily_usage(1_000.0, 250.0)
    snapshot = adapter.record_daily_usage(500.0, 125.0)

    assert snapshot["loss"] == pytest.approx(1_500.0)
    assert snapshot["fee"] == pytest.approx(375.0)

    current = adapter.get_daily_usage()
    assert current == snapshot


def test_instrument_exposure_tracks_notional() -> None:
    adapter = TimescaleAdapter(account_id="admin-eu")

    adapter.record_instrument_exposure("BTC-USD", 10_000.0)
    updated = adapter.record_instrument_exposure("BTC-USD", 5_000.0)

    assert updated == pytest.approx(15_000.0)
    assert adapter.instrument_exposure("BTC-USD") == pytest.approx(15_000.0)
    assert adapter.instrument_exposure("ETH-USD") == pytest.approx(0.0)


def test_record_event_publishes_to_risk_stream() -> None:
    adapter = TimescaleAdapter(account_id="admin-eu")

    adapter.record_event("test_event", {"foo": "bar"})
    events = adapter.events()

    assert events["risk"]
    event = events["risk"][0]
    assert event["type"] == "test_event"
    assert event["payload"] == {"foo": "bar"}
    assert "timestamp" in event


def test_credential_rotation_status_round_trip() -> None:
    adapter = TimescaleAdapter(account_id="admin-eu")

    record = adapter.record_credential_rotation(secret_name="kraken-keys-admin-eu")
    status = adapter.credential_rotation_status()

    assert status == record
    assert set(record) == {"secret_name", "created_at", "rotated_at"}
    assert record["secret_name"] == "kraken-keys-admin-eu"
    assert record["created_at"] == record["rotated_at"]

    TimescaleAdapter.reset_rotation_state(account_id="admin-eu")
    assert adapter.credential_rotation_status() is None


def test_risk_config_can_be_overridden_without_side_effects() -> None:
    adapter = TimescaleAdapter(account_id="admin-eu")

    default_snapshot = adapter.load_risk_config()
    adapter.update_risk_config(kill_switch=True, nav=3_000_000.0)

    updated = adapter.load_risk_config()
    assert updated["kill_switch"] is True
    assert updated["nav"] == pytest.approx(3_000_000.0)

    # Ensure copies are returned to callers
    default_snapshot["nav"] = 123.0
    reloaded = adapter.load_risk_config()
    assert reloaded["nav"] == pytest.approx(3_000_000.0)

