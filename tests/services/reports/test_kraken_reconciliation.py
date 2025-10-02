"""Tests for Kraken reconciliation utilities."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Sequence

import pytest

from services.reports.kraken_reconciliation import (
    InternalFill,
    KrakenFeeAdjustment,
    KrakenFill,
    KrakenNavSnapshot,
    KrakenReconciliationResult,
    KrakenReconciliationService,
    KrakenStatement,
)


class StubDownloader:
    def __init__(self, statement: KrakenStatement) -> None:
        self.statement = statement
        self.calls: list[tuple[str, datetime, datetime]] = []

    def fetch(self, account_id: str, start: datetime, end: datetime) -> KrakenStatement:
        self.calls.append((account_id, start, end))
        return self.statement


class StubLedger:
    def __init__(self, fills: Sequence[InternalFill], nav: float | None) -> None:
        self._fills = list(fills)
        self._nav = nav

    def fetch_fills(self, account_id: str, start: datetime, end: datetime) -> Sequence[InternalFill]:
        return list(self._fills)

    def fetch_nav(self, account_id: str, start: datetime, end: datetime) -> float | None:
        return self._nav


class StubGauge:
    def __init__(self) -> None:
        self.values: dict[str, float] = {}

    def labels(self, account_id: str):  # pragma: no cover - exercised in tests
        gauge = self

        class _Setter:
            def set(self, value: float) -> None:
                gauge.values[account_id] = value

        return _Setter()


class StubMetrics:
    def __init__(self) -> None:
        self.fee_difference = StubGauge()
        self.nav_difference = StubGauge()
        self.missing_fills = StubGauge()
        self.reconciliation_success = StubGauge()


class StubAlertManager:
    def __init__(self) -> None:
        self.events: list = []

    def handle_risk_event(self, event) -> None:  # pragma: no cover - simple recorder
        self.events.append(event)


def _window() -> tuple[datetime, datetime]:
    start = datetime(2024, 5, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def test_reconciliation_applies_fee_adjustments() -> None:
    metrics = StubMetrics()

    start, end = _window()
    fill_time = start + timedelta(hours=1)
    statement = KrakenStatement(
        account_id="acct-1",
        period_start=start,
        period_end=end,
        fills=[
            KrakenFill(
                order_id="ord-1",
                executed_at=fill_time,
                quantity=1.0,
                price=100.0,
                fee=1.5,
            )
        ],
        nav_snapshots=[KrakenNavSnapshot(as_of=end - timedelta(hours=2), nav=1000.0)],
        fee_adjustments=[KrakenFeeAdjustment(amount=-0.5, reason="rebate")],
    )
    downloader = StubDownloader(statement)
    internal_fill = InternalFill(
        order_id="ord-1",
        executed_at=fill_time,
        quantity=1.0,
        price=100.0,
        fee=1.0,
    )
    ledger = StubLedger([internal_fill], nav=1000.0)

    service = KrakenReconciliationService(
        downloader=downloader,
        ledger=ledger,
        metrics=metrics,
        alert_manager=None,
        tolerance=0.1,
    )

    result = service.reconcile_account("acct-1", start, end)

    assert isinstance(result, KrakenReconciliationResult)
    assert result.ok, "adjustments should allow reconciliation to succeed"
    assert result.fee_difference == pytest.approx(0.0)
    assert metrics.fee_difference.values["acct-1"] == pytest.approx(0.0)


def test_reconciliation_detects_missing_fills_and_alerts() -> None:
    metrics = StubMetrics()

    start, end = _window()
    statement = KrakenStatement(
        account_id="acct-2",
        period_start=start,
        period_end=end,
        fills=[
            KrakenFill(
                order_id="ord-2",
                executed_at=start + timedelta(hours=2),
                quantity=0.25,
                price=250.0,
                fee=0.25,
            )
        ],
        nav_snapshots=[KrakenNavSnapshot(as_of=end - timedelta(hours=1), nav=2000.0)],
        fee_adjustments=[],
    )
    downloader = StubDownloader(statement)
    ledger = StubLedger([], nav=2000.0)
    alert_manager = StubAlertManager()

    service = KrakenReconciliationService(
        downloader=downloader,
        ledger=ledger,
        metrics=metrics,
        alert_manager=alert_manager,
        tolerance=0.01,
    )

    result = service.reconcile_account("acct-2", start, end)

    assert not result.ok
    assert result.missing_fill_count == 1
    assert alert_manager.events, "expected alert event to be recorded"
    event = alert_manager.events[0]
    assert getattr(event, "event_type", "") == "KrakenReconciliationMismatch"
    assert metrics.missing_fills.values["acct-2"] == pytest.approx(1.0)
    assert result.exceeds_tolerance(0.01)


def test_reconciliation_success_records_metrics() -> None:
    metrics = StubMetrics()

    start, end = _window()
    fill_time = start + timedelta(hours=3)
    statement = KrakenStatement(
        account_id="acct-9",
        period_start=start,
        period_end=end,
        fills=[
            KrakenFill(
                order_id="ord-9",
                executed_at=fill_time,
                quantity=2.0,
                price=75.0,
                fee=0.5,
            )
        ],
        nav_snapshots=[KrakenNavSnapshot(as_of=end - timedelta(minutes=30), nav=1500.0)],
        fee_adjustments=[],
    )
    downloader = StubDownloader(statement)
    ledger = StubLedger(
        [
            InternalFill(
                order_id="ord-9",
                executed_at=fill_time,
                quantity=2.0,
                price=75.0,
                fee=0.5,
            )
        ],
        nav=1500.0,
    )

    service = KrakenReconciliationService(
        downloader=downloader,
        ledger=ledger,
        metrics=metrics,
        alert_manager=None,
        tolerance=0.5,
    )

    result = service.reconcile_account("acct-9", start, end)

    assert result.ok
    report = result.as_dict()
    assert report["status"] == "matched"
    assert metrics.reconciliation_success.values["acct-9"] == pytest.approx(1.0)
    assert metrics.nav_difference.values["acct-9"] == pytest.approx(0.0)
