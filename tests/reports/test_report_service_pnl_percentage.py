import pytest

pd = pytest.importorskip("pandas")

from datetime import datetime, timedelta, timezone

import services.reports.report_service as report_service


def test_daily_pnl_summary_includes_percentage(monkeypatch):
    report_date = datetime(2024, 5, 1, tzinfo=timezone.utc)
    start = report_date
    end = start + timedelta(days=1)

    frame = pd.DataFrame(
        [
            {
                "account_id": "acct",
                "curve_ts": datetime(2024, 5, 1, 0, 0, tzinfo=timezone.utc),
                "nav": 100_000.0,
                "net_pnl": 0.0,
                "realized_pnl": 0.0,
                "fees": 12.5,
                "gross_exposure": 100_000.0,
                "net_exposure": 100_000.0,
            },
            {
                "account_id": "acct",
                "curve_ts": datetime(2024, 5, 1, 23, 0, tzinfo=timezone.utc),
                "nav": 101_000.0,
                "net_pnl": 1_000.0,
                "realized_pnl": 1_250.0,
                "fees": 37.5,
                "gross_exposure": 101_000.0,
                "net_exposure": 101_000.0,
            },
        ]
    )

    def fake_query(conn, query, params):  # noqa: ANN001
        return frame

    monkeypatch.setattr(report_service, "_query_dataframe", fake_query)

    summary = report_service._daily_pnl_summary(
        object(),
        account_id="acct",
        start=start,
        end=end,
        report_date=report_date.date(),
    )

    assert set(["starting_nav", "ending_nav", "daily_return_pct"]).issubset(summary.columns)
    row = summary.iloc[0]
    assert row["starting_nav"] == pytest.approx(100_000.0)
    assert row["ending_nav"] == pytest.approx(101_000.0)
    assert row["daily_return_pct"] == pytest.approx(1.0)

    fills = pd.DataFrame(
        [
            {
                "session_date": report_date.date(),
                "account_id": "acct",
                "instrument": "BTC-USD",
                "trade_count": 2,
                "executed_qty": 3.0,
                "gross_notional": 12_000.0,
                "fees": 50.0,
            }
        ]
    )

    merged = report_service._merge_daily_components(fills, summary, pd.DataFrame())
    output = merged.iloc[0]
    assert output["daily_return_pct"] == pytest.approx(1.0)
    assert output["starting_nav"] == pytest.approx(100_000.0)
    assert output["ending_nav"] == pytest.approx(101_000.0)
