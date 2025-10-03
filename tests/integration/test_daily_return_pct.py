from __future__ import annotations

import datetime as dt
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Mapping, Optional

import pytest

pytest.importorskip("fastapi")
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.common.security import require_admin_account
import services.reports.report_service as report_service


def _normalize_sql(query: str) -> str:
    return " ".join(str(query).split())


_NAV_OPEN = _normalize_sql(report_service.NAV_OPEN_QUERY)
_NAV_CLOSE = _normalize_sql(report_service.NAV_CLOSE_QUERY)
_PNL_SUMMARY = _normalize_sql(report_service.PNL_SUMMARY_QUERY)
_FEES_SUMMARY = _normalize_sql(report_service.FEES_SUMMARY_QUERY)


class _StubCursor:
    def __init__(
        self,
        *,
        nav_rows: Iterable[Mapping[str, Any]],
        pnl_rows: Iterable[Mapping[str, Any]],
        orders: Iterable[Mapping[str, Any]],
        fills: Iterable[Mapping[str, Any]],
    ) -> None:
        self._nav_rows = list(nav_rows)
        self._pnl_rows = list(pnl_rows)
        self._orders_by_id = {str(order["order_id"]): dict(order) for order in orders}
        self._fills = list(fills)
        self._last_result: Optional[Any] = None
        self._last_query: Optional[str] = None
        self._last_params: Mapping[str, Any] = {}

    def execute(self, query: str, params: Optional[Mapping[str, Any]] = None) -> None:
        normalized = _normalize_sql(query)
        params = params or {}
        self._last_query = normalized
        self._last_params = params

        if normalized == _NAV_OPEN:
            self._last_result = self._nav_open(params)
        elif normalized == _NAV_CLOSE:
            self._last_result = self._nav_close(params)
        elif normalized == _PNL_SUMMARY:
            self._last_result = self._pnl_summary(params)
        elif normalized == _FEES_SUMMARY:
            self._last_result = self._fees_summary(params)
        else:
            self._last_result = None

    def fetchone(self) -> Optional[Mapping[str, Any]]:
        if self._last_result is None:
            return None
        if isinstance(self._last_result, list):
            if not self._last_result:
                return None
            return dict(self._last_result[0])
        return dict(self._last_result)

    def fetchall(self) -> List[Mapping[str, Any]]:
        if self._last_result is None:
            return []
        if isinstance(self._last_result, list):
            return [dict(row) for row in self._last_result]
        return [dict(self._last_result)]

    def _nav_open(self, params: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
        account_id = str(params.get("account_id", ""))
        start = params.get("start")
        end = params.get("end")
        candidates = [
            row
            for row in self._nav_rows
            if str(row.get("account_id")) == account_id
            and start <= row.get("as_of") < end
        ]
        candidates.sort(key=lambda row: row.get("as_of"))
        return dict(candidates[0]) if candidates else None

    def _nav_close(self, params: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
        account_id = str(params.get("account_id", ""))
        start = params.get("start")
        end = params.get("end")
        candidates = [
            row
            for row in self._nav_rows
            if str(row.get("account_id")) == account_id
            and start <= row.get("as_of") < end
        ]
        candidates.sort(key=lambda row: row.get("as_of"), reverse=True)
        return dict(candidates[0]) if candidates else None

    def _pnl_summary(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        account_id = str(params.get("account_id", ""))
        start = params.get("start")
        end = params.get("end")
        realized_total = 0.0
        unrealized_total = 0.0
        for row in self._pnl_rows:
            if str(row.get("account_id")) != account_id:
                continue
            as_of = row.get("as_of")
            if as_of is None or as_of < start or as_of >= end:
                continue
            realized_total += float(row.get("realized", 0.0))
            unrealized_total += float(row.get("unrealized", 0.0))
        return {"realized_pnl": realized_total, "unrealized_pnl": unrealized_total}

    def _fees_summary(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        account_id = str(params.get("account_id", ""))
        start = params.get("start")
        end = params.get("end")
        total_fees = 0.0
        for fill in self._fills:
            fill_time = fill.get("fill_time")
            if fill_time is None or fill_time < start or fill_time >= end:
                continue
            order_id = str(fill.get("order_id"))
            order = self._orders_by_id.get(order_id)
            if not order or str(order.get("account_id")) != account_id:
                continue
            total_fees += float(fill.get("fee", 0.0))
        return {"fees": total_fees}


class _StubDailyReportService(report_service.DailyReportService):
    def __init__(
        self,
        *,
        default_account_id: str,
        nav_rows: Iterable[Mapping[str, Any]],
        pnl_rows: Iterable[Mapping[str, Any]],
        orders: Iterable[Mapping[str, Any]],
        fills: Iterable[Mapping[str, Any]],
    ) -> None:
        super().__init__(default_account_id=default_account_id)
        self._nav_rows = list(nav_rows)
        self._pnl_rows = list(pnl_rows)
        self._orders = list(orders)
        self._fills = list(fills)
        self.persisted_nav: Optional[Dict[str, Any]] = None

    @contextmanager
    def _session(self, config: Any) -> Iterable[_StubCursor]:  # type: ignore[override]
        del config
        yield _StubCursor(
            nav_rows=self._nav_rows,
            pnl_rows=self._pnl_rows,
            orders=self._orders,
            fills=self._fills,
        )

    def _timescale(self, account_id: str) -> Any:  # type: ignore[override]
        return {"account_id": account_id}

    def _persist_daily_nav(
        self,
        *,
        account_id: str,
        nav_date: dt.date,
        open_nav: float,
        close_nav: float,
        daily_return_pct: float,
    ) -> None:
        self.persisted_nav = {
            "account_id": account_id,
            "date": nav_date,
            "open_nav": open_nav,
            "close_nav": close_nav,
            "daily_return_pct": daily_return_pct,
        }


def test_daily_return_pct_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    report_date = dt.date(2024, 6, 15)
    start = dt.datetime.combine(report_date, dt.time(0, 0), tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(hours=8)

    nav_rows = [
        {"account_id": "alpha", "as_of": start, "nav": 100_000.0},
        {"account_id": "alpha", "as_of": end, "nav": 101_500.0},
    ]
    pnl_rows = [
        {"account_id": "alpha", "as_of": start + dt.timedelta(hours=1), "realized": 300.0, "unrealized": 120.0},
        {"account_id": "alpha", "as_of": start + dt.timedelta(hours=4), "realized": 180.0, "unrealized": 95.0},
    ]
    orders = [
        {"order_id": "ord-1", "account_id": "alpha", "submitted_at": start + dt.timedelta(minutes=5)},
        {"order_id": "ord-2", "account_id": "alpha", "submitted_at": start + dt.timedelta(hours=2)},
    ]
    fills = [
        {"order_id": "ord-1", "fill_time": start + dt.timedelta(minutes=30), "fee": 4.0},
        {"order_id": "ord-2", "fill_time": start + dt.timedelta(hours=2, minutes=15), "fee": 3.5},
    ]

    service = _StubDailyReportService(
        default_account_id="alpha",
        nav_rows=nav_rows,
        pnl_rows=pnl_rows,
        orders=orders,
        fills=fills,
    )
    monkeypatch.setattr(report_service, "_SERVICE", service, raising=False)

    app = FastAPI()
    app.include_router(report_service.router)
    app.dependency_overrides[require_admin_account] = lambda: "alpha"

    with TestClient(app) as client:
        response = client.get(
            "/reports/pnl/daily_pct",
            params={"account_id": "alpha", "date": report_date.isoformat()},
        )

    assert response.status_code == 200
    payload = response.json()

    expected_return = ((nav_rows[1]["nav"] - nav_rows[0]["nav"]) / nav_rows[0]["nav"]) * 100.0
    assert payload["account_id"] == "alpha"
    assert payload["date"] == report_date.isoformat()
    assert payload["daily_return_pct"] == pytest.approx(expected_return)
    assert payload["open_nav"] == pytest.approx(nav_rows[0]["nav"])
    assert payload["close_nav"] == pytest.approx(nav_rows[1]["nav"])
    assert payload["realized_pnl_usd"] == pytest.approx(480.0)
    assert payload["unrealized_pnl_usd"] == pytest.approx(215.0)
    assert payload["fees_usd"] == pytest.approx(7.5)

    assert service.persisted_nav is not None
    assert service.persisted_nav["account_id"] == "alpha"
    assert service.persisted_nav["date"] == report_date
    assert service.persisted_nav["open_nav"] == pytest.approx(nav_rows[0]["nav"])
    assert service.persisted_nav["close_nav"] == pytest.approx(nav_rows[1]["nav"])
    assert service.persisted_nav["daily_return_pct"] == pytest.approx(expected_return)
