"""Regression tests for the reports service helpers."""

from __future__ import annotations

from typing import Any, Callable, Iterator, TypeVar, cast

import pytest

pytest.importorskip("fastapi")

import services.reports.report_service as report_service

F = TypeVar("F", bound=Callable[..., object])


def _fixture(*args: Any, **kwargs: Any) -> Callable[[F], F]:
    """Typed shim around :func:`pytest.fixture` for strict mypy."""

    return cast(Callable[[F], F], pytest.fixture(*args, **kwargs))


@_fixture(autouse=True)
def _clear_service_cache() -> Iterator[None]:
    """Ensure tests run with an isolated cached service instance."""

    report_service.reset_daily_report_service_cache()
    try:
        yield
    finally:
        report_service.reset_daily_report_service_cache()


def test_override_daily_report_service_restores_previous() -> None:
    original = report_service.get_daily_report_service()
    replacement = report_service.DailyReportService(default_account_id="override")

    with report_service.override_daily_report_service(replacement) as active:
        assert active is replacement
        assert report_service.get_daily_report_service() is replacement

    restored = report_service.get_daily_report_service()
    assert restored is original
    assert restored is not replacement


def test_reset_daily_report_service_cache_creates_fresh_instance() -> None:
    first = report_service.get_daily_report_service()
    report_service.reset_daily_report_service_cache()
    second = report_service.get_daily_report_service()

    assert second is not first

