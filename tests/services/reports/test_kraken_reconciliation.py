"""Tests for Kraken reconciliation utilities."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
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
    def __init__(self, fills: Sequence[InternalFill], nav: Decimal | None) -> None:
        self._fills = list(fills)
        self._nav = nav

    def fetch_fills(self, account_id: str, start: datetime, end: datetime) -> Sequence[InternalFill]:
        return list(self._fills)

    def fetch_nav(self, account_id: str, start: datetime, end: datetime) -> Decimal | None:
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
                quantity=Decimal("1"),
                price=Decimal("100"),
                fee=Decimal("1.5"),
            )
        ],
        nav_snapshots=[
            KrakenNavSnapshot(as_of=end - timedelta(hours=2), nav=Decimal("1000"))
        ],
        fee_adjustments=[
            KrakenFeeAdjustment(amount=Decimal("-0.5"), reason="rebate")
        ],
    )
    downloader = StubDownloader(statement)
    internal_fill = InternalFill(
        order_id="ord-1",
        executed_at=fill_time,
        quantity=Decimal("1"),
        price=Decimal("100"),
        fee=Decimal("1.0"),
    )
    ledger = StubLedger([internal_fill], nav=Decimal("1000"))

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
    assert result.fee_difference == Decimal("0")
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
                quantity=Decimal("0.25"),
                price=Decimal("250"),
                fee=Decimal("0.25"),
            )
        ],
        nav_snapshots=[
            KrakenNavSnapshot(as_of=end - timedelta(hours=1), nav=Decimal("2000"))
        ],
        fee_adjustments=[],
    )
    downloader = StubDownloader(statement)
    ledger = StubLedger([], nav=Decimal("2000"))
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
                quantity=Decimal("2"),
                price=Decimal("75"),
                fee=Decimal("0.5"),
            )
        ],
        nav_snapshots=[
            KrakenNavSnapshot(as_of=end - timedelta(minutes=30), nav=Decimal("1500"))
        ],
        fee_adjustments=[],
    )
    downloader = StubDownloader(statement)
    ledger = StubLedger(
        [
            InternalFill(
                order_id="ord-9",
                executed_at=fill_time,
                quantity=Decimal("2"),
                price=Decimal("75"),
                fee=Decimal("0.5"),
            )
        ],
        nav=Decimal("1500"),
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


def test_reconciliation_handles_high_precision_values() -> None:
    metrics = StubMetrics()

    start, end = _window()
    fill_time = start + timedelta(hours=4)

    quantity = Decimal("0.00012345")
    price = Decimal("42123.987654")
    fee = Decimal("0.00001234")
    adjustment = Decimal("-0.00000005")
    nav_value = Decimal("987654.32109876")

    statement = KrakenStatement(
        account_id="acct-precision",
        period_start=start,
        period_end=end,
        fills=[
            KrakenFill(
                order_id="ord-precision",
                executed_at=fill_time,
                quantity=quantity,
                price=price,
                fee=fee,
            )
        ],
        nav_snapshots=[KrakenNavSnapshot(as_of=end - timedelta(minutes=10), nav=nav_value)],
        fee_adjustments=[KrakenFeeAdjustment(amount=adjustment, reason="promo")],
    )

    internal_fill = InternalFill(
        order_id="ord-precision",
        executed_at=fill_time,
        quantity=quantity,
        price=price,
        fee=fee + adjustment,
    )
    ledger = StubLedger([internal_fill], nav=nav_value)

    service = KrakenReconciliationService(
        downloader=StubDownloader(statement),
        ledger=ledger,
        metrics=metrics,
        alert_manager=None,
        tolerance=0.0001,
    )

    result = service.reconcile_account("acct-precision", start, end)

    expected_total_fees = fee + adjustment
    assert result.external_total_fees == expected_total_fees
    assert result.internal_total_fees == expected_total_fees
    assert result.fee_difference == Decimal("0")
    assert result.nav_difference == Decimal("0")

    report = result.as_dict()
    assert report["status"] == "matched"
    assert pytest.approx(float(expected_total_fees)) == report["external_total_fees"]
    assert metrics.fee_difference.values["acct-precision"] == pytest.approx(0.0)
