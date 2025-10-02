import pytest

from services.reports import report_service


class _ReportStub:
    def __init__(self, nav: float, realized: float, unrealized: float, fees: float) -> None:
        self.nav = nav
        self.realized_pnl = realized
        self.unrealized_pnl = unrealized
        self.fees = fees


class _ServiceStub:
    def __init__(self, report: _ReportStub) -> None:
        self._report = report

    def build_daily_report(self, account_id=None):  # pragma: no cover - interface shim
        return self._report


def test_compute_daily_return_pct(monkeypatch: pytest.MonkeyPatch) -> None:
    report = _ReportStub(nav=1000.0, realized=50.0, unrealized=-10.0, fees=5.0)
    service = _ServiceStub(report)
    monkeypatch.setattr(report_service, "get_daily_report_service", lambda: service)

    result = report_service.compute_daily_return_pct(account_id="alpha")
    assert result == pytest.approx(((50.0 - 10.0 - 5.0) / 1000.0) * 100.0)


def test_compute_daily_return_pct_handles_missing_nav(monkeypatch: pytest.MonkeyPatch) -> None:
    report = _ReportStub(nav=0.0, realized=10.0, unrealized=0.0, fees=0.0)
    service = _ServiceStub(report)
    monkeypatch.setattr(report_service, "get_daily_report_service", lambda: service)

    result = report_service.compute_daily_return_pct(account_id="alpha")
    assert result is None


def test_compute_daily_return_pct_handles_service_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FailingService:
        def build_daily_report(self, account_id=None):  # pragma: no cover - exercised via exception path
            raise RuntimeError("boom")

    monkeypatch.setattr(report_service, "get_daily_report_service", lambda: _FailingService())

    result = report_service.compute_daily_return_pct(account_id="alpha")
    assert result is None
