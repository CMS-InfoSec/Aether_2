from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.common.adapters import TimescaleAdapter


def setup_function() -> None:
    TimescaleAdapter.reset()
    TimescaleAdapter.reset_rotation_state()


def test_daily_usage_accumulates_per_account() -> None:
    adapter = TimescaleAdapter(account_id="alpha")

    adapter.record_daily_usage(100.0, 10.0)
    adapter.record_daily_usage(50.0, 5.0)

    usage = adapter.get_daily_usage()

    assert usage["loss"] == pytest.approx(150.0)
    assert usage["fee"] == pytest.approx(15.0)


def test_instrument_exposure_tracks_by_symbol() -> None:
    adapter = TimescaleAdapter(account_id="alpha")

    adapter.record_instrument_exposure("BTC-USD", 25_000.0)
    adapter.record_instrument_exposure("BTC-USD", 5_000.0)
    adapter.record_instrument_exposure("ETH-USD", 10_000.0)

    assert adapter.instrument_exposure("BTC-USD") == pytest.approx(30_000.0)
    assert adapter.instrument_exposure("ETH-USD") == pytest.approx(10_000.0)
    assert adapter.instrument_exposure("SOL-USD") == pytest.approx(0.0)


def test_record_event_appends_risk_event_log() -> None:
    adapter = TimescaleAdapter(account_id="alpha")

    payload = {"reason": "breach", "context": {"latency": 500.0}}
    adapter.record_event("latency_circuit_breaker", payload)

    events = adapter.events()["risk"]
    assert len(events) == 1
    event = events[0]
    assert event["type"] == "latency_circuit_breaker"
    assert event["payload"] == payload
    assert isinstance(event["timestamp"], datetime)
    assert event["timestamp"].tzinfo == timezone.utc


def test_credential_rotation_returns_latest_state() -> None:
    adapter = TimescaleAdapter(account_id="alpha")

    first = adapter.record_credential_rotation(secret_name="kraken-keys-alpha")
    second = adapter.record_credential_rotation(secret_name="kraken-keys-alpha")

    assert first["secret_name"] == "kraken-keys-alpha"
    assert "created_at" in first and "rotated_at" in first
    assert second["created_at"] == first["created_at"]
    assert second["rotated_at"] >= first["rotated_at"]

    status = adapter.credential_rotation_status()
    assert status is not None
    assert status["secret_name"] == "kraken-keys-alpha"
