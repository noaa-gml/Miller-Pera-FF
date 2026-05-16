"""Unit tests for timeutils.py — calendar-accurate time spans.

A wrong month or year length silently mis-scales emissions in the unit
conversions, so these check real calendar values: leap years, the century
rules, and the month-sum == year-span invariant the conversions rely on.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from timeutils import seconds_in_month, seconds_in_year

DAY = 86400


@pytest.mark.parametrize(
    ("year", "month", "days"),
    [
        (2023, 1, 31),   # January
        (2023, 2, 28),   # February, common year
        (2024, 2, 29),   # February, leap year (divisible by 4)
        (2000, 2, 29),   # February, leap year (divisible by 400)
        (2100, 2, 28),   # February, common year (div by 100, not 400)
        (2023, 4, 30),   # April
        (2023, 12, 31),  # December
    ],
)
def test_seconds_in_month(year, month, days):
    assert seconds_in_month(year, month) == days * DAY


@pytest.mark.parametrize(
    ("year", "days"),
    [
        (2023, 365),  # common year
        (2024, 366),  # leap year (divisible by 4)
        (2000, 366),  # leap year (divisible by 400)
        (2100, 365),  # common year (div by 100, not 400)
        (1993, 365),  # pipeline start year
    ],
)
def test_seconds_in_year(year, days):
    assert seconds_in_year(year) == days * DAY


@pytest.mark.parametrize("year", [1993, 2020, 2023, 2024, 2025, 2026, 2100])
def test_months_sum_to_year(year):
    """The unit conversions rely on this: 12 monthly spans == the year span."""
    month_sum = sum(seconds_in_month(year, m) for m in range(1, 13))
    assert month_sum == seconds_in_year(year)


@pytest.mark.parametrize("month", range(1, 13))
def test_month_lengths_are_plausible(month):
    """Every month is 28-31 days long."""
    assert 28 * DAY <= seconds_in_month(2025, month) <= 31 * DAY
