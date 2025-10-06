"""Regression tests for compliance pack spot-only enforcement."""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Dict, List, Mapping

import pytest

from compliance_pack import CompliancePackExporter, StorageConfig


class _StubCompliancePackExporter(CompliancePackExporter):
    """Test double that returns pre-seeded rows instead of querying a database."""

    def __init__(self, table_rows: Mapping[str, List[Mapping[str, Any]]]) -> None:
        super().__init__(config=StorageConfig(bucket="test-bucket"), dsn="postgresql://test")
        self._table_rows = table_rows

    def _fetch_table(  # type: ignore[override]
        self,
        conn: object,
        table: str,
        ts_column: str,
        start: dt.datetime,
        end: dt.datetime,
    ) -> List[Dict[str, Any]]:
        rows = self._table_rows.get(table, [])
        return [self._normalise_row(dict(row)) for row in rows]


def _utc(day: int) -> dt.datetime:
    return dt.datetime(2024, 1, day, tzinfo=dt.timezone.utc)


def test_normalise_trade_canonicalises_spot_symbol() -> None:
    exporter = CompliancePackExporter(config=StorageConfig(bucket="test"), dsn="postgresql://test")
    trade = {"symbol": "eth/usd", "trade_id": "abc-123"}

    normalized = exporter._normalise_trade(trade)

    assert normalized["instrument"] == "ETH-USD"
    assert normalized["symbol"] == "ETH-USD"


def test_normalise_trade_rejects_derivative_symbol() -> None:
    exporter = CompliancePackExporter(config=StorageConfig(bucket="test"), dsn="postgresql://test")

    with pytest.raises(ValueError):
        exporter._normalise_trade({"symbol": "BTC-PERP"})


def test_collect_records_filters_non_spot_trades_and_fills(caplog: pytest.LogCaptureFixture) -> None:
    table_rows = {
        "trade_log": [
            {"symbol": "btc-perp", "trade_id": "non-spot"},
            {"symbol": "btc-usd", "trade_id": "spot"},
        ],
        "fills": [
            {"instrument": "btc-perp", "fill_id": "fill-non-spot"},
            {"symbol": "btc-usd", "fill_id": "fill-spot"},
        ],
    }
    exporter = _StubCompliancePackExporter(table_rows)
    caplog.set_level(logging.WARNING)

    records = exporter._collect_records(conn=None, start=_utc(1), end=_utc(2))

    trades = records["trades"]
    fills = records["fills"]

    assert [trade["instrument"] for trade in trades] == ["BTC-USD"]
    assert [fill["instrument"] for fill in fills] == ["BTC-USD"]
    assert any("non-spot instrument" in message for message in caplog.messages)
