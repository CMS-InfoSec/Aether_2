import math
from datetime import datetime, timedelta, timezone

import pytest

from services.common.adapters import TimescaleAdapter


def test_timescale_adapter_initializes_exposure_cache() -> None:
    account_id = "test"
    TimescaleAdapter._instrument_exposures.pop(account_id, None)

    adapter = TimescaleAdapter(account_id=account_id)

    assert adapter.account_id == account_id
    assert account_id in TimescaleAdapter._instrument_exposures
    assert TimescaleAdapter._instrument_exposures[account_id] == {}

    TimescaleAdapter._instrument_exposures.pop(account_id, None)


@pytest.fixture
def adapter() -> TimescaleAdapter:
    TimescaleAdapter.reset()
    instance = TimescaleAdapter(account_id="unit-test")
    yield instance
    TimescaleAdapter.reset()


def test_load_risk_config_returns_copy(adapter: TimescaleAdapter) -> None:
    config = adapter.load_risk_config()
    assert config["nav"] == pytest.approx(1_000_000.0)
    config["nav"] = 42.0

    fresh = adapter.load_risk_config()
    assert fresh["nav"] == pytest.approx(1_000_000.0)
    assert fresh is not config


def test_daily_usage_tracking_is_scoped_to_today(adapter: TimescaleAdapter) -> None:
    adapter.record_daily_usage(125.5, 12.0)
    usage = adapter.get_daily_usage()
    assert usage == {"loss": pytest.approx(125.5), "fee": pytest.approx(12.0)}

    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    TimescaleAdapter._daily_usage[adapter.account_id][yesterday] = {"loss": 999.0, "fee": 888.0}

    refreshed = adapter.get_daily_usage()
    assert refreshed == usage


def test_instrument_exposure_accumulates(adapter: TimescaleAdapter) -> None:
    assert adapter.instrument_exposure("BTC-USD") == pytest.approx(0.0)
    adapter.record_instrument_exposure("BTC-USD", 10_000.0)
    adapter.record_instrument_exposure("BTC-USD", 2_500.25)
    assert adapter.instrument_exposure("BTC-USD") == pytest.approx(12_500.25)


def test_record_event_is_tracked(adapter: TimescaleAdapter) -> None:
    payload = {"reason": "nav limit", "exposure_ratio": 0.42}
    adapter.record_event("nav_limit_breach", payload)

    events = TimescaleAdapter._events[adapter.account_id]["events"]
    assert len(events) == 1
    event = events[0]
    assert event["type"] == "nav_limit_breach"
    assert event["payload"] == payload
    assert isinstance(event["timestamp"], datetime)

    payload["reason"] = "mutated"
    assert events[0]["payload"]["reason"] == "nav limit"


def test_order_lifecycle_helpers_store_payloads(adapter: TimescaleAdapter) -> None:
    ack_payload = {"order_id": "abc", "status": "ok"}
    fill_payload = {"order_id": "abc", "quantity": 1.5}

    adapter.record_ack(ack_payload)
    adapter.record_fill(fill_payload)

    ack_entries = TimescaleAdapter._events[adapter.account_id]["acks"]
    fill_entries = TimescaleAdapter._events[adapter.account_id]["fills"]

    assert len(ack_entries) == 1
    assert len(fill_entries) == 1
    assert ack_entries[0]["payload"] == ack_payload
    assert fill_entries[0]["payload"] == fill_payload
    assert isinstance(ack_entries[0]["recorded_at"], datetime)
    assert isinstance(fill_entries[0]["recorded_at"], datetime)

    ack_payload["status"] = "mutated"
    fill_payload["quantity"] = math.pi
    assert ack_entries[0]["payload"]["status"] == "ok"
    assert fill_entries[0]["payload"]["quantity"] == pytest.approx(1.5)


def test_credential_rotation_status_returns_latest_entry(adapter: TimescaleAdapter) -> None:
    assert adapter.credential_rotation_status() is None

    rotation_time = datetime.now(timezone.utc)
    adapter.record_credential_rotation(secret_name="kraken-unit-test", rotated_at=rotation_time)

    status = adapter.credential_rotation_status()
    assert status == {
        "secret_name": "kraken-unit-test",
        "created_at": rotation_time,
        "rotated_at": rotation_time,
    }

    later_rotation = rotation_time + timedelta(hours=1)
    adapter.record_credential_rotation(secret_name="kraken-unit-test", rotated_at=later_rotation)

    latest_status = adapter.credential_rotation_status()
    assert latest_status == {
        "secret_name": "kraken-unit-test",
        "created_at": rotation_time,
        "rotated_at": later_rotation,
    }
    assert latest_status is not status


def test_reset_clears_global_rolling_volume_cache() -> None:
    TimescaleAdapter.reset()
    TimescaleAdapter.seed_rolling_volume(
        {
            "acct-1": {
                "BTC-USD": {
                    "notional": 123.45,
                    "basis_ts": datetime.now(timezone.utc),
                }
            }
        }
    )

    assert TimescaleAdapter._rolling_volume

    TimescaleAdapter.reset()

    assert TimescaleAdapter._rolling_volume == {}


def test_reset_clears_account_rolling_volume_cache_only_for_target_account() -> None:
    TimescaleAdapter.reset()
    TimescaleAdapter.seed_rolling_volume(
        {
            "acct-1": {
                "BTC-USD": {
                    "notional": 123.45,
                    "basis_ts": datetime.now(timezone.utc),
                }
            },
            "acct-2": {
                "ETH-USD": {
                    "notional": 678.9,
                    "basis_ts": datetime.now(timezone.utc),
                }
            },
        }
    )

    TimescaleAdapter.reset(account_id="acct-1")

    assert "acct-1" not in TimescaleAdapter._rolling_volume
    assert "acct-2" in TimescaleAdapter._rolling_volume
