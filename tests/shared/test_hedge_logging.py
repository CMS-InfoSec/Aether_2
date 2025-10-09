from __future__ import annotations

import csv
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from shared import hedge_logging


def _sample_entry(account: str = "alpha", *, timestamp: datetime | None = None) -> hedge_logging.HedgeLogEntry:
    ts = timestamp or datetime(2024, 6, 10, 12, 0, tzinfo=timezone.utc)
    return hedge_logging.HedgeLogEntry(
        timestamp=ts,
        account_id=account,
        symbol="USDT-USD",
        side="BUY",
        previous_allocation=Decimal("100000"),
        target_allocation=Decimal("120000"),
        delta_usd=Decimal("20000"),
        quantity=Decimal("19950.5"),
        price=Decimal("1.0025"),
        risk_score=Decimal("1.35"),
        drawdown_pct=Decimal("0.08"),
        atr=Decimal("12.4"),
        realized_vol=Decimal("0.32"),
        order_id="hedge-abc",
    )


def test_hedge_logger_appends_csv(tmp_path: Path) -> None:
    path = tmp_path / "hedge.csv"
    logger = hedge_logging.HedgeLogger(path=path)

    with hedge_logging.override_hedge_logger(logger):
        hedge_logging.log_hedge_event(_sample_entry())

    assert path.exists()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert reader.fieldnames == list(hedge_logging.HEDGE_LOG_COLUMNS)
    assert len(rows) == 1
    first = rows[0]
    assert first["order_id"] == "hedge-abc"
    assert float(first["target_allocation"]) == pytest.approx(120000.0)


def test_iter_hedge_log_rows_filters_by_account_and_time(tmp_path: Path) -> None:
    path = tmp_path / "hedge.csv"
    logger = hedge_logging.HedgeLogger(path=path)

    older = datetime(2024, 6, 9, 12, 0, tzinfo=timezone.utc)
    newer = datetime(2024, 6, 11, 12, 0, tzinfo=timezone.utc)

    with hedge_logging.override_hedge_logger(logger):
        hedge_logging.log_hedge_event(_sample_entry(account="alpha", timestamp=older))
        hedge_logging.log_hedge_event(_sample_entry(account="beta", timestamp=newer))

    start = datetime(2024, 6, 10, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 6, 12, 0, 0, tzinfo=timezone.utc)

    filtered = list(
        hedge_logging.iter_hedge_log_rows(
            account_id="beta", start=start, end=end, path=path
        )
    )

    assert len(filtered) == 1
    row = filtered[0]
    assert row["account_id"] == "beta"
    assert float(row["target_allocation"]) == pytest.approx(120000.0)

    # Verify read helper mirrors the iterator
    records = hedge_logging.read_hedge_log(
        account_id="beta", start=start, end=end, path=path
    )
    assert records == filtered

