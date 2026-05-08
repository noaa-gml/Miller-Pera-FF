"""Tests for the CarbonMonitor download / loader / overwrite functions."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from download_carbon_monitor import EXPECTED_HEADER, EXPECTED_SECTORS
from ff_country_2026 import _apply_cm_monthly_overwrite

REPO = Path(__file__).parent.parent


# ── download_carbon_monitor constants ────────────────────────────────────────

def test_expected_cm_header_unchanged():
    """If CarbonMonitor changes their CSV schema, this catches it."""
    assert EXPECTED_HEADER == "country,date,sector,value,"


def test_expected_cm_sectors_unchanged():
    assert {
        "Domestic Aviation", "Ground Transport", "Industry",
        "International Aviation", "Power", "Residential",
    } == EXPECTED_SECTORS


# ── _apply_cm_monthly_overwrite ──────────────────────────────────────────────

@pytest.fixture
def overwrite_inputs():
    """Build a tiny synthetic ff_monthly + GISS map + CM YoY ratios.

    Layout: 4 years × 12 months = 48 months; 4×4 grid; 2 countries + ocean.
        codes_arr = [100, 200]   (country A occupies code 100; country B = 200)
        gissmap[0, :] = 100      (1 row of country A)
        gissmap[1, :] = 200      (1 row of country B)
        gissmap[2:, :] = 0       (ocean)

    YoY ratios for year 4 (last year):
        country A: Feb=1.10  (overwrite triggers for month=2)
        country B: Feb=0.90
        WORLD    : Feb=0.95  (used by ocean)
        Mar/Apr  : NaN      (skipped)
    """
    yr_start = 2020
    n_years = 4
    n_months = n_years * 12
    H, W = 4, 4

    # ff_monthly: fill prior-year (year 3, idx 24..35) with country-distinguishable
    # values so we can verify the multiplication.
    rng = np.random.default_rng(0)
    ff_monthly = rng.uniform(1.0, 2.0, size=(n_months, H, W))
    # Make the 2023 layer (year_idx=3) clearly different so we can spot the overwrite.
    # The overwrite year is 2023 (last); prior year is 2022 (idx 24..35).
    # Put round numbers in 2022's Feb (idx 25).
    ff_monthly[25] = np.array([
        [10.0, 10.0, 10.0, 10.0],     # row 0: country A
        [20.0, 20.0, 20.0, 20.0],     # row 1: country B
        [ 5.0,  5.0,  5.0,  5.0],     # row 2: ocean
        [ 5.0,  5.0,  5.0,  5.0],     # row 3: ocean
    ])

    gissmap = np.zeros((H, W), dtype=int)
    gissmap[0, :] = 100
    gissmap[1, :] = 200
    # rows 2,3 stay 0 → ocean

    country_names = ["A", "B"]
    codes_arr = np.array([100, 200])

    cm_yoy_ratios = pd.DataFrame(
        index=["A", "B", "WORLD"],
        columns=["2023-02", "2023-03"],
        dtype=float,
    )
    cm_yoy_ratios.loc["A",     "2023-02"] = 1.10
    cm_yoy_ratios.loc["B",     "2023-02"] = 0.90
    cm_yoy_ratios.loc["WORLD", "2023-02"] = 0.95
    cm_yoy_ratios.loc["A",     "2023-03"] = np.nan
    cm_yoy_ratios.loc["B",     "2023-03"] = np.nan
    cm_yoy_ratios.loc["WORLD", "2023-03"] = np.nan

    return ff_monthly, country_names, gissmap, codes_arr, cm_yoy_ratios, yr_start


def test_overwrite_applies_per_country_yoy(overwrite_inputs):
    ff_monthly, names, gissmap, codes_arr, cm, yr_start = overwrite_inputs
    # Snapshot prior-year Feb (for assertions later).
    prior_feb = ff_monthly[25].copy()

    _apply_cm_monthly_overwrite(
        ff_monthly,
        country_names=names,
        gissmap=gissmap,
        codes_arr=codes_arr,
        cm_yoy_ratios=cm,
        yr_start=yr_start,
        cm_year=2023,
        overwrite_months=(2, 3),
    )

    # 2023-Feb at month index 3*12 + 1 = 37
    feb_2023 = ff_monthly[37]
    # Row 0 (country A): prior × 1.10
    np.testing.assert_allclose(feb_2023[0, :], prior_feb[0, :] * 1.10, rtol=1e-12)
    # Row 1 (country B): prior × 0.90
    np.testing.assert_allclose(feb_2023[1, :], prior_feb[1, :] * 0.90, rtol=1e-12)
    # Rows 2,3 (ocean): prior × WORLD = 0.95
    np.testing.assert_allclose(feb_2023[2:, :], prior_feb[2:, :] * 0.95, rtol=1e-12)


def test_overwrite_skips_months_with_nan_world_ratio(overwrite_inputs):
    """Mar 2023 has NaN ratios → that month is left unchanged."""
    ff_monthly, names, gissmap, codes_arr, cm, yr_start = overwrite_inputs
    mar_2023_before = ff_monthly[38].copy()  # year_idx=3, month_idx=2

    _apply_cm_monthly_overwrite(
        ff_monthly, country_names=names, gissmap=gissmap, codes_arr=codes_arr,
        cm_yoy_ratios=cm, yr_start=yr_start, cm_year=2023, overwrite_months=(3,),
    )

    # All rows: country ratios are NaN → loop skips. WORLD is also NaN → ocean
    # branch skipped. Result: month is unchanged from before.
    np.testing.assert_array_equal(ff_monthly[38], mar_2023_before)


def test_overwrite_skips_month_not_in_csv(overwrite_inputs):
    """Apr 2023 doesn't have a column at all → overwrite skips silently."""
    ff_monthly, names, gissmap, codes_arr, cm, yr_start = overwrite_inputs
    apr_2023_before = ff_monthly[39].copy()

    _apply_cm_monthly_overwrite(
        ff_monthly, country_names=names, gissmap=gissmap, codes_arr=codes_arr,
        cm_yoy_ratios=cm, yr_start=yr_start, cm_year=2023, overwrite_months=(4,),
    )

    np.testing.assert_array_equal(ff_monthly[39], apr_2023_before)


def test_overwrite_uses_world_for_ocean_not_country_ratio(overwrite_inputs):
    """Ocean cells must use WORLD ratio, NOT inherit country A's or B's."""
    ff_monthly, names, gissmap, codes_arr, cm, yr_start = overwrite_inputs
    prior_ocean = ff_monthly[25, 2:, :].copy()  # rows 2,3

    _apply_cm_monthly_overwrite(
        ff_monthly, country_names=names, gissmap=gissmap, codes_arr=codes_arr,
        cm_yoy_ratios=cm, yr_start=yr_start, cm_year=2023, overwrite_months=(2,),
    )

    feb_2023_ocean = ff_monthly[37, 2:, :]
    # Should be prior × 0.95, NOT × 1.10 (A) or × 0.90 (B)
    np.testing.assert_allclose(feb_2023_ocean, prior_ocean * 0.95, rtol=1e-12)
    # And definitely not the country ratios:
    assert not np.allclose(feb_2023_ocean, prior_ocean * 1.10)
    assert not np.allclose(feb_2023_ocean, prior_ocean * 0.90)
