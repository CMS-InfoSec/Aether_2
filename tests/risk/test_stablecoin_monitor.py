from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.risk.stablecoin_monitor import (
    StablecoinMonitor,
    StablecoinMonitorConfig,
)


class _FakeClock:
    def __init__(self, start: datetime) -> None:
        self._now = start

    def __call__(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


@pytest.fixture
def monitor_with_clock() -> tuple[StablecoinMonitor, _FakeClock]:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    clock = _FakeClock(start)
    config = StablecoinMonitorConfig(
        depeg_threshold_bps=50,
        recovery_threshold_bps=10,
        feed_max_age_seconds=30,
        monitored_symbols=("USDC-USD",),
        trusted_feeds=("primary_fx",),
    )
    monitor = StablecoinMonitor(config=config, clock=clock)
    return monitor, clock


def test_monitor_detects_and_clears_depeg(
    monitor_with_clock: tuple[StablecoinMonitor, _FakeClock]
) -> None:
    monitor, _ = monitor_with_clock
    # Initial update causes a depeg event.
    status = monitor.update("USDC-USD", 0.9940, feed="primary_fx")
    assert status.depegged is True
    assert status.stale is False
    assert status.deviation_bps == pytest.approx(-60.0)

    # Mid-range deviations retain the prior depeg status until recovery threshold.
    status = monitor.update("USDC-USD", 0.9960, feed="primary_fx")
    assert status.depegged is True
    assert status.deviation_bps == pytest.approx(-40.0)

    # Recovery inside the threshold clears the depeg flag.
    status = monitor.update("USDC-USD", 1.0005, feed="primary_fx")
    assert status.depegged is False
    assert status.deviation_bps == pytest.approx(5.0)

    # Active depeg list is empty after recovery.
    assert monitor.active_depegs() == []


def test_monitor_marks_stale_quotes_as_safe(
    monitor_with_clock: tuple[StablecoinMonitor, _FakeClock]
) -> None:
    monitor, clock = monitor_with_clock
    monitor.update("USDC-USD", 0.9900, feed="primary_fx")
    statuses = monitor.active_depegs()
    assert statuses and statuses[0].depegged is True

    # Advance beyond the max age to mark the quote as stale.
    clock.advance(61)

    status = monitor.status("USDC-USD")
    assert status is not None
    assert status.stale is True
    assert status.depegged is False
    assert monitor.active_depegs() == []
