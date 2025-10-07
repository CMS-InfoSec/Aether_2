from __future__ import annotations

import csv
import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import pytest

from reports.daily_pnl import (
    DailyPnlRow,
    compute_daily_pnl,
    fetch_daily_orders,
    generate_daily_pnl,
)
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
        self._accounts_by_id = {account["account_id"]: account for account in accounts}
        self._orders = {order["order_id"]: order for order in orders}
        self._fills = fills
        self.audit_entries: List[Mapping[str, Any]] = []
        self.queries: List[str] = []
        self.params: List[Mapping[str, Any]] = []

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized_query = " ".join(query.lower().split())
        self.queries.append(query.strip())
        self.params.append(dict(params))

        account_ids_param = params.get("account_ids")
        if account_ids_param is not None:
            if any(not isinstance(value, int) for value in account_ids_param):
                raise TypeError("operator does not exist: integer = text")

        if "insert into audit_logs" in normalized_query:

            self.audit_entries.append(params)
            return FakeResult([])

        account_filter_ids = params.get("account_ids") or []
        account_filter_slugs = params.get("account_slugs") or []

        if account_filter_slugs and "join accounts as a on a.account_id = o.account_id" not in normalized_query:
            raise AssertionError("Slug filters must join the accounts table")

        if "from fills" in normalized_query:
            start = params.get("start")
            end = params.get("end")
            rows: List[Dict[str, Any]] = []
            for fill in self._fills:
                fill_time = fill["fill_time"]
                if start and fill_time < start:
                    continue
                if end and fill_time >= end:
                    continue
                order = self._orders[fill["order_id"]]
                account_id = order["account_id"]
                account = self._accounts_by_id[account_id]
                slug = account.get("admin_slug")
                match_numeric = account_filter_ids and account_id in account_filter_ids
                match_slug = account_filter_slugs and slug in account_filter_slugs
                if account_filter_ids and account_filter_slugs:
                    if not (match_numeric or match_slug):
                        continue
                elif account_filter_ids and not match_numeric:
                    continue
                elif account_filter_slugs and not match_slug:
                    continue
                rows.append(
                    {
                        "account_id": account_id,
                        "order_id": fill["order_id"],
                        "side": order["side"],
                        "size": fill["size"],
                        "price": fill["price"],
                        "fee": fill.get("fee"),
                        "instrument": fill.get("market")
                        or fill.get("symbol")
                        or order.get("market")
                        or order.get("symbol"),
                        "fill_ts": fill_time,
                    }
                )
            return FakeResult(rows)

        if "o.submitted_at" in normalized_query:
            start = params.get("start")
            end = params.get("end")
            rows = []
            for order in self._orders.values():
                submitted = order["submitted_at"]
                if start and submitted < start:
                    continue
                if end and submitted >= end:
                    continue
                account_id = order["account_id"]
                account = self._accounts_by_id[account_id]
                slug = account.get("admin_slug")
                match_numeric = account_filter_ids and account_id in account_filter_ids
                match_slug = account_filter_slugs and slug in account_filter_slugs
                if account_filter_ids and account_filter_slugs:
                    if not (match_numeric or match_slug):
                        continue
                elif account_filter_ids and not match_numeric:
                    continue
                elif account_filter_slugs and not match_slug:
                    continue
                rows.append(
                    {
                        "order_id": order["order_id"],
                        "account_id": account_id,
                        "instrument": order.get("market") or order.get("symbol"),
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
        {"account_id": 101, "admin_slug": "alpha"},
        {"account_id": 202, "admin_slug": "beta"},
    ]
    orders = [
        {
            "order_id": "ord-1",
            "account_id": 101,
            "market": "BTC-USD",
            "symbol": "BTC-USD",
            "side": "BUY",
            "size": 1,
            "submitted_at": datetime(2024, 5, 1, 0, 0, tzinfo=timezone.utc),
        },
        {
            "order_id": "ord-2",
            "account_id": 101,
            "market": "BTC-USD",
            "symbol": "BTC-USD",
            "side": "SELL",
            "size": 1,
            "submitted_at": datetime(2024, 5, 1, 1, 0, tzinfo=timezone.utc),
        },
        {
            "order_id": "ord-3",
            "account_id": 202,
            "market": "ETH-USD",
            "symbol": "ETH-USD",
            "side": "SELL",
            "size": 2,
            "submitted_at": datetime(2024, 5, 1, 2, 0, tzinfo=timezone.utc),
        },
    ]
    fills = [
        {
            "fill_id": "fill-1",
            "order_id": "ord-1",
            "market": "BTC-USD",
            "symbol": "BTC-USD",
            "fill_time": datetime(2024, 5, 1, 0, 15, tzinfo=timezone.utc),
            "price": 100,
            "size": 1,
            "fee": 0.5,
        },
        {
            "fill_id": "fill-2",
            "order_id": "ord-2",
            "market": "BTC-USD",
            "symbol": "BTC-USD",
            "fill_time": datetime(2024, 5, 1, 1, 15, tzinfo=timezone.utc),
            "price": 110,
            "size": 1,
            "fee": 0.6,
        },
        {
            "fill_id": "fill-3",
            "order_id": "ord-3",
            "market": "ETH-USD",
            "symbol": "ETH-USD",
            "fill_time": datetime(2024, 5, 1, 2, 15, tzinfo=timezone.utc),
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
    alpha_row = next(row for row in rows if row["account_id"] == "101")
    assert Decimal(alpha_row["gross_pnl"]) == Decimal("10.00")
    assert Decimal(alpha_row["fees"]) == Decimal("1.1")
    assert Decimal(alpha_row["net_pnl"]) == Decimal("8.90")


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
    assert {row["account_id"] for row in rows} == {"101"}


def test_generate_daily_pnl_accepts_admin_slug_filter(
    tmp_path: Path, sample_session: RecordingSession
) -> None:
    storage = ArtifactStorage(tmp_path)
    generate_daily_pnl(
        sample_session,
        storage,
        report_date=date(2024, 5, 1),
        account_ids=("alpha",),
        output_formats=("csv",),
    )

    slug_params = [params for params in sample_session.params if "account_slugs" in params]
    assert slug_params
    for params in slug_params:
        assert params["account_slugs"] == ["alpha"]

    slug_queries = [
        query for query, params in zip(sample_session.queries, sample_session.params)
        if params.get("account_slugs")
    ]
    assert slug_queries
    for query in slug_queries:
        assert "JOIN accounts AS a ON a.account_id = o.account_id" in query

    for params in sample_session.params:
        if "account_ids" in params:
            assert all(isinstance(value, int) for value in params["account_ids"])


def test_generate_daily_pnl_queries_use_real_columns(
    tmp_path: Path, sample_session: RecordingSession
) -> None:
    storage = ArtifactStorage(tmp_path)
    generate_daily_pnl(
        sample_session,
        storage,
        report_date=date(2024, 5, 1),
        output_formats=("csv",),
    )

    normalized_queries = [" ".join(query.lower().split()) for query in sample_session.queries]

    fill_queries = [query for query in normalized_queries if "from fills" in query]
    assert fill_queries
    for query in fill_queries:
        assert "f.market as instrument" in query
        assert "where f.fill_time >= %(start)s and f.fill_time < %(end)s" in query

    order_queries = [query for query in normalized_queries if "from orders" in query]
    assert order_queries
    for query in order_queries:
        assert "o.market as instrument" in query
        assert "o.submitted_at as submitted_ts" in query
        assert "where o.submitted_at >= %(start)s and o.submitted_at < %(end)s" in query


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
    assert "daily_pnl/2024-05-01.csv" in payload["metadata"]["object_key"]


def test_generate_daily_pnl_high_precision_serialization(tmp_path: Path) -> None:
    from datetime import datetime, timezone

    pd = pytest.importorskip("pandas")

    accounts = [{"account_id": 404, "admin_slug": "gamma"}]
    orders = [
        {
            "order_id": "ord-hp",
            "account_id": 404,
            "market": "BTC-USD",
            "symbol": "BTC-USD",
            "side": "SELL",
            "size": "0.00012345",
            "submitted_at": datetime(2024, 5, 2, 0, 0, tzinfo=timezone.utc),
        }
    ]
    fills = [
        {
            "fill_id": "fill-hp",
            "order_id": "ord-hp",
            "market": "BTC-USD",
            "symbol": "BTC-USD",
            "fill_time": datetime(2024, 5, 2, 0, 5, tzinfo=timezone.utc),
            "price": "52345.67",
            "size": "0.00012345",
            "fee": "0.12345678",
        }
    ]
    session = RecordingSession(accounts, orders, fills)
    storage = ArtifactStorage(tmp_path)

    keys = generate_daily_pnl(
        session,
        storage,
        report_date=date(2024, 5, 2),
        output_formats=("csv", "parquet"),
    )

    assert keys == ["daily_pnl/2024-05-02.csv", "daily_pnl/2024-05-02.parquet"]

    quantity = Decimal("0.00012345")
    price = Decimal("52345.67")
    fee = Decimal("0.12345678")
    gross = quantity * price
    net = gross - fee
    reporting_scale = Decimal("0.01")

    csv_path = tmp_path / "global" / keys[0]
    rows = read_csv(csv_path)
    assert len(rows) == 1
    row = rows[0]
    assert Decimal(row["executed_quantity"]) == quantity
    assert Decimal(row["gross_pnl"]) == gross.quantize(reporting_scale)
    assert Decimal(row["fees"]) == fee
    assert Decimal(row["net_pnl"]) == net.quantize(reporting_scale)

    parquet_path = tmp_path / "global" / keys[1]
    frame = pd.read_parquet(parquet_path)
    assert len(frame) == 1
    parquet_row = frame.iloc[0]
    assert Decimal(str(parquet_row["executed_quantity"])) == quantity
    assert Decimal(str(parquet_row["gross_pnl"])) == gross.quantize(reporting_scale)
    assert Decimal(str(parquet_row["fees"])) == fee
    assert Decimal(str(parquet_row["net_pnl"])) == net.quantize(reporting_scale)


class _StubResult:
    def __init__(self, rows: Iterable[Dict[str, Any]]) -> None:
        self._rows = list(rows)

    def mappings(self) -> List[Dict[str, Any]]:
        return list(self._rows)


class _StubSession:
    def __init__(self, rows: Iterable[Dict[str, Any]]) -> None:
        self._rows = list(rows)

    def execute(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> _StubResult:
        return _StubResult(self._rows)


def _fill(
    *,
    account_id: str = "acct-1",
    order_id: str = "order-1",
    instrument: Any = "BTC-USD",
    side: str = "SELL",
    size: Any = "1",
    price: Any = "100",
    fee: Any = "0",
) -> Dict[str, Any]:
    return {
        "account_id": account_id,
        "order_id": order_id,
        "instrument": instrument,
        "side": side,
        "size": size,
        "price": price,
        "fee": fee,
    }


def test_fetch_daily_orders_filters_non_spot_symbols(caplog: pytest.LogCaptureFixture) -> None:
    session = _StubSession(
        [
            {"order_id": 1, "account_id": 42, "instrument": "btc-perp"},
            {"order_id": 2, "account_id": 42, "instrument": "eth_usd"},
            {"order_id": 3, "account_id": 99, "market": "ltcup-usdt"},
            {"order_id": 4, "account_id": 77, "symbol": "ada-usd"},
        ]
    )

    with caplog.at_level(logging.WARNING):
        instruments = fetch_daily_orders(
            session,
            start=datetime(2024, 5, 2, tzinfo=timezone.utc),
            end=datetime(2024, 5, 3, tzinfo=timezone.utc),
        )

    assert instruments == {"2": "ETH-USD", "4": "ADA-USD"}
    assert "Ignoring non-spot instrument" in caplog.text


def test_fetch_daily_orders_logs_validation_error_details(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = _StubSession(
        [
            {"order_id": 1, "account_id": 42, "instrument": "btc-perp"},
        ]
    )

    with caplog.at_level(logging.WARNING):
        fetch_daily_orders(
            session,
            start=datetime(2024, 5, 2, tzinfo=timezone.utc),
            end=datetime(2024, 5, 3, tzinfo=timezone.utc),
        )

    warning = next(
        record
        for record in caplog.records
        if "Ignoring non-spot instrument" in record.getMessage()
    )
    assert warning.instrument == "BTC-PERP"
    assert warning.error.startswith("Only spot market instruments are supported")


def test_compute_daily_pnl_normalizes_instruments() -> None:
    rows = compute_daily_pnl(
        [_fill(instrument="btc/usd", side="SELL", size="2", price="10", fee="0")],
        {},
    )

    assert rows == [
        DailyPnlRow(
            account_id="acct-1",
            instrument="BTC-USD",
            executed_quantity=Decimal("2"),
            gross_pnl=Decimal("20"),
            fees=Decimal("0"),
            net_pnl=Decimal("20"),
        )
    ]


def test_compute_daily_pnl_skips_derivative_fills(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.DEBUG):
        rows = compute_daily_pnl([_fill(instrument="btc-perp")], {})

    assert rows == []
    assert "Skipping fill without a canonical spot instrument" in caplog.text


def test_compute_daily_pnl_uses_order_mapping_for_missing_instrument() -> None:
    rows = compute_daily_pnl([_fill(order_id="order-2", instrument=None)], {"order-2": "eth-usd"})

    assert len(rows) == 1
    assert rows[0].instrument == "ETH-USD"

