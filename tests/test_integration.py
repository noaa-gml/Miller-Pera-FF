"""Integration tests — stage-to-stage contracts in the pipeline.

Where the unit tests exercise one function in isolation, these check that
the *output of one pipeline stage is consumed correctly by the next*.

Group A — post_process → split_ct
    `split_ct.build_carbontracker_dataset()` consumes the monolithic
    netCDF that `post_process.py` writes. We feed it a small synthetic
    dataset in exactly that format and verify the CarbonTracker transform.
    split_ct imports only numpy/pandas/xarray, so this group runs in CI.

Group B — ff_country → post_process (unit-conversion round-trip)
    The Gg C → mol m⁻² s⁻¹ forward conversion and the PgC back-computation
    must be mutually consistent. post_process imports the geospatial
    stack (xesmf/xcdat/…), so this group skips gracefully in CI and runs
    locally in the p312 env.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

sys.path.insert(0, str(Path(__file__).parent.parent))
from constants import C_MOLAR_MASS
from split_ct import build_carbontracker_dataset
from timeutils import seconds_in_year

# ── post_process import probe (geospatial stack may be absent in CI) ─────────
try:
    import post_process
    _HAVE_POST_PROCESS = True
except ImportError:
    _HAVE_POST_PROCESS = False

requires_post_process = pytest.mark.skipif(
    not _HAVE_POST_PROCESS,
    reason="post_process import needs the geospatial stack (xesmf/xcdat/cf_xarray)",
)


# ═════════════════════════════════════════════════════════════════════════════
# Group A — post_process → split_ct
# ═════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def synthetic_monolithic() -> xr.Dataset:
    """A minimal stand-in for post_process.py's monolithic netCDF.

    Two years (24 months) on a 2×2 grid — just enough structure for
    `build_carbontracker_dataset` to chew on: `fossil_imp`, `time_bnds`
    (month start/end), and lat/lon bounds.
    """
    n_months = 24
    starts = pd.date_range("1993-01-01", periods=n_months, freq="MS")
    ends = starts + pd.offsets.MonthBegin(1)
    mid = starts + (ends - starts) / 2
    lat = np.array([-45.0, 45.0])
    lon = np.array([0.0, 180.0])
    rng = np.random.default_rng(0)
    fossil = rng.uniform(1e-10, 1e-8, size=(n_months, 2, 2))
    return xr.Dataset(
        data_vars={
            "fossil_imp": (("time", "lat", "lon"), fossil),
            "time_bnds": (("time", "bnds"),
                          np.stack([starts.values, ends.values], axis=1)),
            "lat_bounds": (("lat", "bounds"), np.array([[-90.0, 0.0], [0.0, 90.0]])),
            "lon_bounds": (("lon", "bounds"), np.array([[-90.0, 90.0], [90.0, 270.0]])),
        },
        coords={"time": mid.values, "lat": lat, "lon": lon},
    )


def test_build_ct_renames_time_to_date(synthetic_monolithic: xr.Dataset) -> None:
    ct = build_carbontracker_dataset(synthetic_monolithic)
    assert "date" in ct.dims
    assert "time" not in ct.dims


def test_build_ct_has_required_variables(synthetic_monolithic: xr.Dataset) -> None:
    """CarbonTracker delivery format needs these alongside fossil_imp."""
    ct = build_carbontracker_dataset(synthetic_monolithic)
    for v in ["fossil_imp", "date_bounds", "decimal_date",
              "date_components", "calendar_components"]:
        assert v in ct, f"missing {v}"


def test_build_ct_preserves_emission_values(synthetic_monolithic: xr.Dataset) -> None:
    """The CT transform repackages metadata — it must not touch the fluxes."""
    ct = build_carbontracker_dataset(synthetic_monolithic)
    np.testing.assert_array_equal(
        ct["fossil_imp"].values, synthetic_monolithic["fossil_imp"].values,
    )


def test_build_ct_date_midpoints_within_month(synthetic_monolithic: xr.Dataset) -> None:
    """Each `date` value is the month midpoint — strictly inside its bounds."""
    ct = build_carbontracker_dataset(synthetic_monolithic)
    tb = synthetic_monolithic["time_bnds"].values
    dates = ct["date"].values
    for i in range(len(dates)):
        assert tb[i, 0] <= dates[i] < tb[i, 1]


def test_build_ct_decimal_date_monotonic_in_range(synthetic_monolithic: xr.Dataset) -> None:
    ct = build_carbontracker_dataset(synthetic_monolithic)
    dd = ct["decimal_date"].values
    assert np.all(np.diff(dd) > 0), "decimal_date not monotonically increasing"
    assert dd[0] >= 1993.0
    assert dd[-1] < 1995.0


def test_build_ct_carbontracker_attrs(synthetic_monolithic: xr.Dataset) -> None:
    ct = build_carbontracker_dataset(synthetic_monolithic)
    for attr in ["Notes", "disclaimer", "Conventions", "institution"]:
        assert attr in ct.attrs, f"missing global attribute {attr}"
    assert ct.attrs["Conventions"] == "CF-1.9"


def test_build_ct_drops_diagnostic_variables(synthetic_monolithic: xr.Dataset) -> None:
    """fossil_imp_cell / cell_areas are post_process diagnostics — CT files
    carry only fossil_imp + coordinate bounds."""
    ds = synthetic_monolithic.copy()
    ds["fossil_imp_cell"] = (("time", "lat", "lon"), np.ones((24, 2, 2)))
    ds["cell_areas"] = (("lat", "lon"), np.ones((2, 2)))
    ct = build_carbontracker_dataset(ds)
    assert "fossil_imp_cell" not in ct
    assert "cell_areas" not in ct


def test_build_ct_date_count_unchanged(synthetic_monolithic: xr.Dataset) -> None:
    """All 24 months survive the transform — no months dropped or duplicated."""
    ct = build_carbontracker_dataset(synthetic_monolithic)
    assert ct.sizes["date"] == synthetic_monolithic.sizes["time"] == 24


# ═════════════════════════════════════════════════════════════════════════════
# Group B — ff_country → post_process unit-conversion round-trip
# ═════════════════════════════════════════════════════════════════════════════

@requires_post_process
def test_gg_to_mol_to_pgc_roundtrip() -> None:
    """Forward Gg C → mol m⁻² s⁻¹, then `_annual_pgc` back, recovers the input.

    post_process converts Gg C/cell/yr → mol/m²/s with
    ``GgC · 1e9/C_MOLAR_MASS / area_m2 / sec_yr``; `_annual_pgc` integrates
    ``mol/m²/s · area · sec_month`` back to PgC. The two must compose to the
    identity (1 Gg C = 1e-6 Pg C).
    """
    gg_per_cell = 1000.0          # Gg C / cell / yr
    yr = 2010
    areas_m2 = np.full((4, 4), 5.0e10)
    sec_yr = seconds_in_year(yr)

    mol_per_m2_s = gg_per_cell * 1e9 / C_MOLAR_MASS / areas_m2 / sec_yr
    data_yr = np.broadcast_to(mol_per_m2_s, (12, 4, 4)).copy()  # 12 identical months

    pgc = post_process._annual_pgc(data_yr, areas_m2, yr)
    expected = gg_per_cell * 16 * 1e-6   # 16 cells, Gg → Pg
    assert abs(pgc - expected) <= 1e-9 * expected
