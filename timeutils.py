"""Calendar-accurate time-span helpers.

Convert calendar spans to seconds using real month and year lengths, leap
years included. The unit conversions in ``ingest.py`` and ``post_process.py``
divide annual emission rates by these spans, so a wrong value silently
mis-scales the output.

Kept in their own module (standard library plus python-dateutil — no
geospatial stack) so they import anywhere and can be unit-tested directly.
"""
from __future__ import annotations

from datetime import UTC, datetime

from dateutil.relativedelta import relativedelta


def seconds_in_month(year: int, month: int) -> float:
    """Calendar-accurate seconds in a given month."""
    t0 = datetime(year, month, 1, tzinfo=UTC)
    t1 = t0 + relativedelta(months=1)
    return (t1 - t0).total_seconds()


def seconds_in_year(year: int) -> float:
    """Calendar-accurate seconds in a given year (leap years included)."""
    t0 = datetime(year, 1, 1, tzinfo=UTC)
    t1 = datetime(year + 1, 1, 1, tzinfo=UTC)
    return (t1 - t0).total_seconds()
