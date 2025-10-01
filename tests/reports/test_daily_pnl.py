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
    def __init__(
        self,
        accounts: List[Dict[str, Any]],
        orders: List[Dict[str, Any]],
        fills: List[Dict[str, Any]],
    ) -> None:
        self._accounts = {account["account_id"]: account for account in accounts}
        self._orders = {order["order_id"]: order for order in orders}
        self._fills = fills
        self.audit_entries: List[Mapping[str, Any]] = []
        self.queries: List[str] = []

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized_query = " ".join(query.lower().split())
        self.queries.append(query.strip())

        if "insert into audit_log" in normalized_query:

            self.audit_entries.append(params)
            return FakeResult([])

        account_filter = params.get("account_ids")

        if "from fills" in normalized_query:
            start = params.get("start")
            end = params.get("end")
            rows: List[Dict[str, Any]] = []
            for fill in self._fills:
                fill_ts = fill["fill_ts"]
                if start and fill_ts < start:
                    continue
                if end and fill_ts >= end:
                    continue
                order = self._orders[fill["order_id"]]
                account = self._accounts[order["account_id"]]
                account_name = account["name"]
                if account_filter and account_name not in account_filter:
                    continue
                rows.append(
                    {
                        "account_id": account_name,
                        "order_id": fill["order_id"],
                        "side": order["side"],
                        "size": fill["size"],
                        "price": fill["price"],
                        "fee": fill.get("fee"),
                        "instrument": order["symbol"],
                    }
                )
            return FakeResult(rows)

        if "o.submitted_ts" in normalized_query:
            start = params.get("start")
            end = params.get("end")
            rows = []
            for order in self._orders.values():
                submitted = order["submitted_ts"]
                if start and submitted < start:
                    continue
                if end and submitted >= end:
                    continue
                account = self._accounts[order["account_id"]]
                account_name = account["name"]
                if account_filter and account_name not in account_filter:
                    continue
                rows.append(
                    {
                        "order_id": order["order_id"],
                        "account_id": account_name,
                        "instrument": order["symbol"],
                        "size": order["size"],
                        "submitted_ts": submitted,
                    }
                )
            return FakeResult(rows)

        raise AssertionError(f"Unexpected query: {query}")


@pytest.fixture
def sample_session() -> RecordingSession:
    from datetime import datetime, timezone

    accounts = [
        {"account_id": 1, "name": "alpha"},
        {"account_id": 2, "name": "beta"},
    ]
    orders = [
        {
            "order_id": "ord-1",
            "account_id": 1,
            "symbol": "BTC-USD",
            "side": "BUY",
            "size": 1,
            "submitted_ts": datetime(2024, 5, 1, 0, 0, tzinfo=timezone.utc),
        },
        {
            "order_id": "ord-2",
            "account_id": 1,
            "symbol": "BTC-USD",
            "side": "SELL",
            "size": 1,
            "submitted_ts": datetime(2024, 5, 1, 1, 0, tzinfo=timezone.utc),
        },
        {
            "order_id": "ord-3",
            "account_id": 2,
            "symbol": "ETH-USD",
            "side": "SELL",
            "size": 2,
            "submitted_ts": datetime(2024, 5, 1, 2, 0, tzinfo=timezone.utc),
        },
    ]
    fills = [
        {
            "fill_id": "fill-1",
            "order_id": "ord-1",
            "symbol": "BTC-USD",
            "fill_ts": datetime(2024, 5, 1, 0, 15, tzinfo=timezone.utc),
            "price": 100,
            "size": 1,
            "fee": 0.5,
        },
        {
            "fill_id": "fill-2",
            "order_id": "ord-2",
            "symbol": "BTC-USD",
            "fill_ts": datetime(2024, 5, 1, 1, 15, tzinfo=timezone.utc),
            "price": 110,
            "size": 1,
            "fee": 0.6,
        },
        {
            "fill_id": "fill-3",
            "order_id": "ord-3",
            "symbol": "ETH-USD",
            "fill_ts": datetime(2024, 5, 1, 2, 15, tzinfo=timezone.utc),
            "price": 50,
            "size": 2,
            "fee": 0.4,
        },
    ]
    return RecordingSession(accounts, orders, fills)


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
