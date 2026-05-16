"""Regression tests for post_process.py.

These don't run the full pipeline (too slow); they isolate small invariants
that have caused real bugs.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from datetime import time as dtime

import pytest
from dateutil.relativedelta import relativedelta


def _build_times(n_months: int, yr_start: int = 1993) -> list[datetime]:
    """Same construction post_process.py uses."""
    first_day = date(yr_start, 1, 15)
    return [
        datetime.combine(first_day + relativedelta(months=m), dtime.min, tzinfo=UTC)
        for m in range(n_months)
    ]


@pytest.mark.parametrize("n_months", [12, 396, 400])
def test_time_axis_monotonicity_correct_zip(n_months: int):
    """Regression for the B905 zip-strict bug.

    The bug was ``zip(times, times[1:], strict=True)`` which ALWAYS fails
    because times has length N and times[1:] has length N-1 (strict=True
    requires equal lengths). The correct form is
    ``zip(times[:-1], times[1:], strict=True)`` — both length N-1.
    """
    times = _build_times(n_months)
    # The fixed form must succeed for any non-trivial n_months
    assert all(b > a for a, b in zip(times[:-1], times[1:], strict=True))


def test_time_axis_buggy_zip_form_raises():
    """Demonstrates the bug to lock in the fix: original form raises."""
    times = _build_times(12)
    with pytest.raises(ValueError, match=r"shorter|longer"):
        list(zip(times, times[1:], strict=True))


@pytest.mark.parametrize("n_months", [400])
def test_partial_year_400_months(n_months: int):
    """400 months = 33 full years (1993-2025) + 4 partial (2026 Jan-Apr)."""
    times = _build_times(n_months)
    assert times[-1].year == 2026
    assert times[-1].month == 4
    assert times[0].year == 1993
    assert times[0].month == 1


def test_all_times_on_15th():
    """Time-coordinate contract: every monthly value falls on the 15th of the month."""
    times = _build_times(48)
    assert all(t.day == 15 for t in times)
