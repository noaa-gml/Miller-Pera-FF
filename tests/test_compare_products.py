"""Unit tests for the metric helpers in compare_products.py.

The reference products (GridFED, OCO-2 FF) are gitignored bulk data, so the
end-to-end comparison isn't run here; these exercise the pure numeric
helpers on synthetic fields.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

sys.path.insert(0, str(Path(__file__).parent.parent))
from compare_products import (
    annual_mean_flux,
    annual_pgc,
    band_seasonal_cycle,
    per_month_pgc,
    spatial_stats,
)
from constants import C_MOLAR_MASS


def _flux(n_months: int, start: str = "2015-01-01", value: float = 1e-8) -> xr.DataArray:
    times = pd.date_range(start, periods=n_months, freq="MS") + pd.Timedelta(days=14)
    lat = np.array([-45.0, 45.0])
    lon = np.array([0.0, 180.0])
    data = np.full((n_months, 2, 2), value)
    return xr.DataArray(
        data, coords={"time": times, "lat": lat, "lon": lon}, dims=("time", "lat", "lon")
    )


def test_per_month_pgc_matches_hand_calculation() -> None:
    flux = _flux(1, value=1e-8)
    area = np.full((2, 2), 1e12)  # m²
    pgc = per_month_pgc(flux, area)
    expected = 1e-8 * 4 * 1e12 * 31 * 86400 * C_MOLAR_MASS * 1e-15
    assert pgc.iloc[0] == pytest.approx(expected, rel=1e-12)


def test_annual_pgc_drops_partial_years() -> None:
    s = pd.Series(1.0, index=pd.date_range("2015-01-01", periods=15, freq="MS"))
    ann = annual_pgc(s)
    assert list(ann.index) == [2015]
    assert ann.loc[2015] == 12.0


def test_annual_mean_flux_keeps_full_years_only() -> None:
    flux = _flux(15)  # 2015 full + Jan–Mar 2016
    ann = annual_mean_flux(flux)
    assert list(ann.year.values) == [2015]
    np.testing.assert_allclose(ann.values, 1e-8)


def test_spatial_stats_identity_and_scaling() -> None:
    rng = np.random.default_rng(0)
    a = rng.lognormal(size=(10, 20))
    r, r_log, rel = spatial_stats(a, a)
    assert r == pytest.approx(1.0)
    assert r_log == pytest.approx(1.0)
    assert rel == pytest.approx(0.0)
    # A uniform 10 % scaling keeps r = 1 and gives rel-RMS = 10 %.
    r, r_log, rel = spatial_stats(a, 1.1 * a)
    assert r == pytest.approx(1.0)
    assert r_log == pytest.approx(1.0)
    assert rel == pytest.approx(10.0)


def test_band_seasonal_cycle_normalised_and_windowed() -> None:
    # Jan–Dec 2015–2019 with a known cycle: Jan doubled everywhere in the band.
    flux = _flux(12 * 5)
    jan = flux.time.dt.month == 1
    flux = flux.where(~jan, 2e-8)
    area = np.ones((2, 2))
    cyc = band_seasonal_cycle(flux, area)  # 45°N row is inside 30–60°N
    assert cyc.shape == (12,)
    assert cyc.mean() == pytest.approx(1.0)
    assert cyc[0] == pytest.approx(2 * cyc[1])
