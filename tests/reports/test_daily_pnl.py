from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import pytest

from reports.daily_pnl import generate_daily_pnl
from reports.storage import ArtifactStorage


class FakeResult:
    def __init__(self, rows: Iterable[Mapping[str, Any]]):
        self._rows = list(rows)

    def mappings(self) -> List[Mapping[str, Any]]:
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class RecordingSession:
    def __init__(self, fills: List[Dict[str, Any]], orders: List[Dict[str, Any]]):
        self._fills = fills
        self._orders = orders
        self.audit_entries: List[Mapping[str, Any]] = []
        self.queries: List[str] = []

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> FakeResult:
        params = params or {}
        self.queries.append(query.strip())
        if "INSERT INTO audit_logs" in query:
            self.audit_entries.append(params)
            return FakeResult([])
        if "trading_fills" in query:
            account_ids = params.get("account_ids")
            rows = [
                row
                for row in self._fills
                if not account_ids or row["account_id"] in account_ids
            ]
            return FakeResult(rows)
        if "trading_orders" in query:
            account_ids = params.get("account_ids")
            rows = [
                row
                for row in self._orders
                if not account_ids or row["account_id"] in account_ids
            ]
            return FakeResult(rows)
        raise AssertionError(f"Unexpected query: {query}")


@pytest.fixture
def sample_session() -> RecordingSession:
    fills = [
        {
            "account_id": "alpha",
            "order_id": "ord-1",
            "instrument": "BTC-USD",
            "side": "BUY",
            "quantity": 1,
            "price": 100,
            "fee": 0.5,
        },
        {
            "account_id": "alpha",
            "order_id": "ord-2",
            "instrument": "BTC-USD",
            "side": "SELL",
            "quantity": 1,
            "price": 110,
            "fee": 0.6,
        },
        {
            "account_id": "beta",
            "order_id": "ord-3",
            "instrument": "ETH-USD",
            "side": "SELL",
            "quantity": 2,
            "price": 50,
            "fee": 0.4,
        },
    ]
    orders = [
        {"order_id": "ord-1", "account_id": "alpha", "instrument": "BTC-USD"},
        {"order_id": "ord-2", "account_id": "alpha", "instrument": "BTC-USD"},
        {"order_id": "ord-3", "account_id": "beta", "instrument": "ETH-USD"},
    ]
    return RecordingSession(fills, orders)


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="") as handle:
        return list(csv.DictReader(handle))


def test_generate_daily_pnl_fee_attribution(tmp_path: Path, sample_session: RecordingSession) -> None:
    storage = ArtifactStorage(tmp_path)
    keys = generate_daily_pnl(
        sample_session,
        storage,
        report_date=date(2024, 5, 1),
        output_formats=("csv",),
    )
    assert keys == ["daily_pnl/2024-05-01.csv"]
    csv_path = tmp_path / "global" / keys[0]
    rows = read_csv(csv_path)
    assert len(rows) == 2
    alpha_row = next(row for row in rows if row["account_id"] == "alpha")
    assert pytest.approx(float(alpha_row["gross_pnl"])) == 10.0
    assert pytest.approx(float(alpha_row["fees"])) == 1.1
    assert pytest.approx(float(alpha_row["net_pnl"])) == 8.9


def test_generate_daily_pnl_account_isolation(tmp_path: Path, sample_session: RecordingSession) -> None:
    storage = ArtifactStorage(tmp_path)
    keys = generate_daily_pnl(
        sample_session,
        storage,
        report_date=date(2024, 5, 1),
        account_ids=("alpha",),
        output_formats=("csv",),
    )
    csv_path = tmp_path / "multi-account" / keys[0]
    rows = read_csv(csv_path)
    assert {row["account_id"] for row in rows} == {"alpha"}


def test_generate_daily_pnl_creates_audit_log_entries(tmp_path: Path, sample_session: RecordingSession) -> None:
    storage = ArtifactStorage(tmp_path)
    keys = generate_daily_pnl(
        sample_session,
        storage,
        report_date=date(2024, 5, 1),
        output_formats=("csv",),
    )
    assert len(keys) == 1
    assert len(sample_session.audit_entries) == 1
    payload = json.loads(sample_session.audit_entries[0]["payload"])
    assert "daily_pnl/2024-05-01.csv" in payload["object_key"]
