from __future__ import annotations


from datetime import datetime, timezone

import pytest

from services.common.adapters import TimescaleAdapter



@pytest.fixture(autouse=True)
def reset_timescale() -> None:
    TimescaleAdapter.reset()
    yield

    TimescaleAdapter.reset()



def test_record_daily_usage_accumulates() -> None:
    adapter = TimescaleAdapter(account_id="company")

    adapter.record_daily_usage(1_000.0, 250.0)
    adapter.record_daily_usage(500.0, 125.0)

    snapshot = adapter.get_daily_usage()
    assert snapshot["loss"] == pytest.approx(1_500.0)
    assert snapshot["fee"] == pytest.approx(375.0)


def test_instrument_exposure_tracks_notional() -> None:
    adapter = TimescaleAdapter(account_id="company")

    adapter.record_instrument_exposure("BTC-USD", 10_000.0)
    adapter.record_instrument_exposure("BTC-USD", 5_000.0)

    assert adapter.instrument_exposure("BTC-USD") == pytest.approx(15_000.0)
    assert adapter.instrument_exposure("ETH-USD") == pytest.approx(0.0)


def test_record_event_publishes_to_risk_stream() -> None:
    adapter = TimescaleAdapter(account_id="company")

    adapter.record_event("test_event", {"foo": "bar"})
    events = adapter.events()

    assert events["events"]
    event = events["events"][0]
    assert event["type"] == "test_event"
    assert event["payload"] == {"foo": "bar"}
    assert "timestamp" in event


def test_credential_rotation_status_round_trip() -> None:
    adapter = TimescaleAdapter(account_id="company")

    rotation_time = datetime.now(timezone.utc)
    record = adapter.record_credential_rotation(
        secret_name="kraken-keys-company", rotated_at=rotation_time
    )
    status = adapter.credential_rotation_status()

    assert status == record

    assert set(status.keys()) == {"secret_name", "created_at", "rotated_at"}
    assert status["created_at"] == status["rotated_at"]


    TimescaleAdapter.reset(account_id="company")
    assert adapter.credential_rotation_status() is None


def test_reset_clears_target_account_only() -> None:
    europe = TimescaleAdapter(account_id="company")
    us = TimescaleAdapter(account_id="director-1")

    europe.record_instrument_exposure("BTC-USD", 10_000.0)
    us.record_instrument_exposure("BTC-USD", 5_000.0)

    europe.record_event("rotation", {"details": "rotate"})
    us.record_event("heartbeat", {"status": "ok"})

    rotation_time = datetime.now(timezone.utc)
    europe.record_credential_rotation(secret_name="kraken-keys-company", rotated_at=rotation_time)
    us.record_credential_rotation(secret_name="kraken-keys-director-1", rotated_at=rotation_time)

    TimescaleAdapter.reset(account_id="company")

    assert europe.instrument_exposure("BTC-USD") == pytest.approx(0.0)
    assert europe.events()["events"] == []
    assert europe.credential_rotation_status() is None

    assert us.instrument_exposure("BTC-USD") == pytest.approx(5_000.0)
    assert us.events()["events"]
    assert us.credential_rotation_status() is not None


def test_risk_config_can_be_overridden_without_side_effects() -> None:
    adapter = TimescaleAdapter(account_id="company")

    default_snapshot = adapter.load_risk_config()
    TimescaleAdapter._risk_configs[adapter.account_id].update(
        {"kill_switch": True, "nav": 3_000_000.0}
    )

    updated = adapter.load_risk_config()
    assert updated["kill_switch"] is True
    assert updated["nav"] == pytest.approx(3_000_000.0)

    # Ensure copies are returned to callers
    default_snapshot["nav"] = 123.0
    reloaded = adapter.load_risk_config()
    assert reloaded["nav"] == pytest.approx(3_000_000.0)

