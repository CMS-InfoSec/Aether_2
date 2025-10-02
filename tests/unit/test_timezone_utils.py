from datetime import datetime, timezone

import pytest

from shared.timezone import as_london_time, format_london_time


@pytest.mark.parametrize(
    "instant,expected",
    [
        (datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc), "2024-01-15 12:00:00 GMT"),
        (datetime(2024, 6, 15, 12, 0, tzinfo=timezone.utc), "2024-06-15 13:00:00 BST"),
    ],
)
def test_format_london_time_handles_seasonal_offsets(instant: datetime, expected: str) -> None:
    """Formatting respects GMT/BST offsets for winter and summer dates."""

    assert format_london_time(instant) == expected


def test_as_london_time_handles_dst_transition_forward() -> None:
    """Times immediately before and after the DST change map to correct offsets."""

    before_change = datetime(2024, 3, 31, 0, 30, tzinfo=timezone.utc)
    after_change = datetime(2024, 3, 31, 1, 30, tzinfo=timezone.utc)

    before_local = as_london_time(before_change)
    after_local = as_london_time(after_change)

    assert before_local.tzname() == "GMT"
    assert before_local.hour == 0
    assert after_local.tzname() == "BST"
    assert after_local.hour == 2


def test_naive_datetime_assumed_to_be_utc() -> None:
    """Naive datetimes are treated as UTC and converted to London time."""

    naive = datetime(2024, 10, 1, 8, 30)

    result = as_london_time(naive)

    assert result.tzname() == "BST"
    assert result.hour == 9
