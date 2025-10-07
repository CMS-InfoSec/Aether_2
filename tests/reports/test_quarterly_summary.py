from __future__ import annotations

import csv
import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import pytest

from reports.quarterly_summary import (
    AUDIT_QUERY,
    generate_quarterly_summary,
    merge_quarterly_metrics,
)
from reports.storage import ArtifactStorage


class FakeResult:
    def __init__(self, rows: Iterable[Mapping[str, Any]]):
        self._rows = list(rows)

    def mappings(self) -> List[Mapping[str, Any]]:
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class QuarterlyRecordingSession:
    def __init__(
        self,
        orders: List[Dict[str, Any]],
        fills: List[Dict[str, Any]],
        audits: List[Dict[str, Any]],
    ) -> None:
        self._orders = {order["order_id"]: order for order in orders}
        self._fills = fills
        self._audits = audits
        self.audit_entries: List[Mapping[str, Any]] = []
        self.queries: List[str] = []

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized_query = " ".join(query.lower().split())
        self.queries.append(query.strip())

        if "insert into audit_logs" in normalized_query:
            self.audit_entries.append(params)
            return FakeResult([])

        if "from orders" in normalized_query and "submitted_ts" in normalized_query:
            assert "symbol as instrument" in normalized_query
            start = params.get("start")
            end = params.get("end")
            aggregates: Dict[tuple[str, str], Dict[str, Any]] = {}
            for order in self._orders.values():
                submitted = order["submitted_ts"]
                if start and submitted < start:
                    continue
                if end and submitted >= end:
                    continue
                key = (order["account_id"], order["symbol"])
                bucket = aggregates.setdefault(
                    key, {"order_count": 0, "submitted_qty": 0.0}
                )
                bucket["order_count"] += 1
                size = order.get("size")
                if size is not None:
                    bucket["submitted_qty"] += float(size)
            rows = [
                {
                    "account_id": account_id,
                    "instrument": symbol,
                    "order_count": values["order_count"],
                    "submitted_qty": values["submitted_qty"],
                }
                for (account_id, symbol), values in aggregates.items()
            ]
            return FakeResult(rows)

        if "from fills" in normalized_query and "fill_ts" in normalized_query:
            assert "f.symbol as instrument" in normalized_query
            start = params.get("start")
            end = params.get("end")
            aggregates: Dict[tuple[str, str], Dict[str, Any]] = {}
            for fill in self._fills:
                fill_time = fill["fill_ts"]
                if start and fill_time < start:
                    continue
                if end and fill_time >= end:
                    continue
                order = self._orders[fill["order_id"]]
                key = (order["account_id"], fill.get("symbol", order["symbol"]))
                bucket = aggregates.setdefault(key, {"fill_count": 0, "notional": 0.0})
                bucket["fill_count"] += 1
                bucket["notional"] += float(fill.get("size", 0.0)) * float(
                    fill.get("price", 0.0)
                )
            rows = [
                {
                    "account_id": account_id,
                    "instrument": symbol,
                    "fill_count": values["fill_count"],
                    "notional": values["notional"],
                }
                for (account_id, symbol), values in aggregates.items()
            ]
            return FakeResult(rows)

        if "from audit_logs" in normalized_query:

            start = params.get("start")
            end = params.get("end")
            rows = []
            for entry in self._audits:

                created_at = entry["created_at"]
                if start and created_at < start:

                    continue
                if end and created_at >= end:
                    continue

                rows.append({"account_id": entry["actor"], "created_at": created_at})

            return FakeResult(rows)

        raise AssertionError(f"Unexpected query: {query}")


@pytest.fixture
def quarterly_session() -> QuarterlyRecordingSession:
    orders = [
        {
            "order_id": "ord-1",
            "account_id": "alpha",
            "symbol": "BTC-USD",
            "size": 1,
            "submitted_ts": datetime(2024, 4, 2, 0, 0, tzinfo=timezone.utc),
        },
        {
            "order_id": "ord-2",
            "account_id": "alpha",
            "symbol": "BTC-USD",
            "size": 1,
            "submitted_ts": datetime(2024, 5, 1, 0, 0, tzinfo=timezone.utc),
        },
        {
            "order_id": "ord-3",
            "account_id": "beta",
            "symbol": "ETH-USD",
            "size": 2,
            "submitted_ts": datetime(2024, 5, 5, 0, 0, tzinfo=timezone.utc),
        },
    ]
    fills = [
        {
            "fill_id": "fill-1",
            "order_id": "ord-1",
            "symbol": "BTC-USD",
            "fill_ts": datetime(2024, 4, 2, 0, 5, tzinfo=timezone.utc),
            "price": 100,
            "size": 1,
        },
        {
            "fill_id": "fill-2",
            "order_id": "ord-2",
            "symbol": "BTC-USD",
            "fill_ts": datetime(2024, 5, 1, 0, 5, tzinfo=timezone.utc),
            "price": 110,
            "size": 1,
        },
        {
            "fill_id": "fill-3",
            "order_id": "ord-3",
            "symbol": "ETH-USD",
            "fill_ts": datetime(2024, 5, 5, 0, 5, tzinfo=timezone.utc),
            "price": 50,
            "size": 2,
        },
    ]
    audits = [
        {
            "audit_id": "audit-1",
            "actor": "alpha",
            "created_at": datetime(2024, 4, 15, 0, 0, tzinfo=timezone.utc),

        },
        {
            "audit_id": "audit-2",
            "actor": "alpha",
            "created_at": datetime(2024, 5, 20, 0, 0, tzinfo=timezone.utc),

        },
        {
            "audit_id": "audit-3",
            "actor": "beta",
            "created_at": datetime(2024, 4, 20, 0, 0, tzinfo=timezone.utc),

        },
    ]
    return QuarterlyRecordingSession(orders, fills, audits)


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="") as handle:
        return list(csv.DictReader(handle))


def test_generate_quarterly_summary_outputs_expected_metrics(
    tmp_path: Path, quarterly_session: QuarterlyRecordingSession
) -> None:
    storage = ArtifactStorage(tmp_path)
    keys = generate_quarterly_summary(
        quarterly_session,
        storage,
        quarter_ending=date(2024, 6, 30),
        output_formats=("csv",),
    )
    assert keys == ["quarterly_summary/2024-06-30.csv"]
    csv_path = tmp_path / "global" / keys[0]
    rows = read_csv(csv_path)
    row_map = {
        (row["account_id"], row["instrument"]): row
        for row in rows
    }
    alpha_btc = row_map[("alpha", "BTC-USD")]
    assert int(alpha_btc["order_count"]) == 2
    assert int(alpha_btc["fill_count"]) == 2
    assert pytest.approx(float(alpha_btc["submitted_qty"])) == 2.0
    assert pytest.approx(float(alpha_btc["notional"])) == 210.0
    assert int(alpha_btc["audit_events"]) == 2

    beta_eth = row_map[("beta", "ETH-USD")]
    assert int(beta_eth["order_count"]) == 1
    assert int(beta_eth["fill_count"]) == 1
    assert pytest.approx(float(beta_eth["submitted_qty"])) == 2.0
    assert pytest.approx(float(beta_eth["notional"])) == 100.0
    assert int(beta_eth["audit_events"]) == 1

    assert quarterly_session.audit_entries, "expected audit entries to be recorded"


def test_merge_quarterly_metrics_preserves_decimal_precision() -> None:
    orders = [
        {
            "account_id": "acct-1",
            "instrument": "BTC-USD",
            "order_count": 1,
            "submitted_qty": Decimal("0.1"),
        },
        {
            "account_id": "acct-1",
            "instrument": "BTC-USD",
            "order_count": 1,
            "submitted_qty": Decimal("0.2"),
        },
    ]
    fills = [
        {
            "account_id": "acct-1",
            "instrument": "BTC-USD",
            "fill_count": 1,
            "notional": Decimal("0.03"),
        },
        {
            "account_id": "acct-1",
            "instrument": "BTC-USD",
            "fill_count": 1,
            "notional": Decimal("0.07"),
        },
    ]
    audits: List[Mapping[str, Any]] = []

    summaries = merge_quarterly_metrics(orders, fills, audits)

    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.submitted_qty == Decimal("0.3")
    assert summary.notional == Decimal("0.10")


def test_merge_quarterly_metrics_filters_non_spot_instruments(
    caplog: pytest.LogCaptureFixture,
) -> None:
    orders = [
        {
            "account_id": "acct-deriv",
            "instrument": "BTC-PERP",
            "order_count": 1,
            "submitted_qty": Decimal("1"),
        }
    ]
    fills = [
        {
            "account_id": "acct-deriv",
            "instrument": "BTC-PERP",
            "fill_count": 1,
            "notional": Decimal("100"),
        }
    ]

    audits: List[Mapping[str, Any]] = []

    with caplog.at_level(logging.WARNING):
        summaries = merge_quarterly_metrics(orders, fills, audits)

    assert summaries == []
    assert "Ignoring non-spot instrument" in caplog.text


def test_merge_quarterly_metrics_normalises_symbols() -> None:
    orders = [
        {
            "account_id": "acct-spot",
            "instrument": "eth/usd",
            "order_count": 1,
            "submitted_qty": Decimal("1"),
        }
    ]
    fills = [
        {
            "account_id": "acct-spot",
            "instrument": "ETH_usd",
            "fill_count": 1,
            "notional": Decimal("200"),
        }
    ]

    audits: List[Mapping[str, Any]] = []

    summaries = merge_quarterly_metrics(orders, fills, audits)

    assert len(summaries) == 1
    assert summaries[0].instrument == "ETH-USD"


def test_audit_query_returns_actor_created_at_rows(
    quarterly_session: QuarterlyRecordingSession,
) -> None:
    start = datetime(2024, 4, 1, tzinfo=timezone.utc)
    end = datetime(2024, 7, 1, tzinfo=timezone.utc)
    result = quarterly_session.execute(
        AUDIT_QUERY,
        {"start": start, "end": end},
    )
    rows = result.mappings()
    assert sorted((row["account_id"], row["created_at"]) for row in rows) == [
        ("alpha", datetime(2024, 4, 15, 0, 0, tzinfo=timezone.utc)),
        ("alpha", datetime(2024, 5, 20, 0, 0, tzinfo=timezone.utc)),
        ("beta", datetime(2024, 4, 20, 0, 0, tzinfo=timezone.utc)),
    ]
