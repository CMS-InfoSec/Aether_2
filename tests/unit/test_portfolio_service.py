from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

import pytest


def _connection_stub(rows: Iterable[Mapping[str, Any]]):
    class _CursorStub:
        def __init__(self, items: Iterable[Mapping[str, Any]]) -> None:
            self._rows = [dict(row) for row in items]

        def __enter__(self) -> "_CursorStub":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        def execute(self, query: str, params: Mapping[str, Any]) -> None:
            self.query = query
            self.params = params

        def fetchall(self) -> list[Mapping[str, Any]]:
            return [dict(row) for row in self._rows]

    class _ConnectionStub:
        def __init__(self, items: Iterable[Mapping[str, Any]]) -> None:
            self._rows = [dict(row) for row in items]

        def __enter__(self) -> "_ConnectionStub":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        def cursor(self) -> _CursorStub:
            return _CursorStub(self._rows)

    return _ConnectionStub(rows)


def test_compute_daily_return_pct_prefers_mid_price(monkeypatch: pytest.MonkeyPatch) -> None:
    portfolio_rows = [
        {
            "nav_open_usd_mark": 1_000_000.0,
            "nav_close_usd_mark": 1_020_000.0,
            "nav_open_usd_mid": 1_000_000.0,
            "nav_close_usd_mid": 1_050_000.0,
            "curve_ts": datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        }
    ]

    import portfolio_service

    monkeypatch.setattr(portfolio_service, "_connect", lambda: _connection_stub(portfolio_rows))

    pct = portfolio_service.compute_daily_return_pct(
        "acct-123", datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    )
    assert pct == pytest.approx(5.0)


def test_compute_daily_return_pct_zero_nav_open_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    portfolio_rows = [
        {
            "nav_open_usd": 0.0,
            "nav_close_usd": 1_000_000.0,
            "curve_ts": datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        }
    ]

    import portfolio_service

    monkeypatch.setattr(portfolio_service, "_connect", lambda: _connection_stub(portfolio_rows))

    with caplog.at_level("WARNING"):
        pct = portfolio_service.compute_daily_return_pct(
            "acct-456", datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
        )

    assert pct == 0.0
    assert any("nav_open_usd" in record.message for record in caplog.records)


def test_compute_daily_return_pct_uses_nav_series(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {
            "nav": 1_000_000.0,
            "curve_ts": datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc),
        },
        {
            "nav": 1_040_000.0,
            "curve_ts": datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        },
    ]

    import portfolio_service

    monkeypatch.setattr(portfolio_service, "_connect", lambda: _connection_stub(rows))

    pct = portfolio_service.compute_daily_return_pct(
        "acct-789", datetime(2024, 1, 1, 23, 59, tzinfo=timezone.utc)
    )
    assert pct == pytest.approx(4.0)

