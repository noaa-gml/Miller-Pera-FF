#!/usr/bin/env python
"""post_process_2026.py — Convert gridded emissions to NetCDF for CarbonTracker.

For the v2026b NRT extension this script is run twice, once per
``ff_country_2026.py`` method:

    python post_process_2026.py --method assumed
    python post_process_2026.py --method cm_yearly

Input :  outputs/ff_monthly_2026b_<method>_py.npz   (from ff_country_2026.py)
Outputs: outputs/gml_ff_co2_2026b_<method>.nc       (monolithic, partial-year)
         outputs/yearly/gml_ff_co2_2026b_<method>.{YYYY}.nc
                                                    (per-year, full years only)

Workflow:
  1. Load .npz, convert Gg C → mol/m²/s
  2. Reorder to (time, lat, lon) and build one canonical xarray Dataset
  3. Write the monolithic file (with diagnostic variables)
  4. Slice per-year from the SAME dataset and write per-year files

The dimension order is set ONCE, BEFORE any files are written:

    NetCDF on disk (C / row-major):  time  ×  lat  ×  lon
    Fortran in memory (col-major):   lon   ×  lat  ×  time
    → matches  ff_input(nlon360, nlat180, 12)  ✓

Variable name: 'fossil_imp'  (Fortran rc key  ff.ncfile.varname = fossil_imp)
Units:         mol m-2 s-1   (Fortran multiplies by 12e-3 * spm → kgC/m²/month)
"""

import argparse
import os
import subprocess
import sys
from datetime import UTC, date, datetime
from datetime import time as dtime
from typing import Literal

import cf_xarray.units
import netCDF4
import numpy as np

# pint / CF-aware xarray — import order matters
import pint_xarray  # noqa: F401  # registers the .pint accessor on xarray
import xarray as xr
import xcdat  # noqa: F401  # monkey-patches .bounds.add_time_bounds onto xarray
import xesmf as xe
from dateutil.relativedelta import relativedelta  # type: ignore[import-untyped]

xr.set_options(keep_attrs=True)  # type: ignore[no-untyped-call]


# ═══════════════════════════════════════════════════════════════════════════════
# Configuration — edit these for each new year
# ═══════════════════════════════════════════════════════════════════════════════
YEARLY_DIR    = "outputs/yearly"
VAR_NAME      = "fossil_imp"           # main variable (matches Fortran rc)
YR1           = 1993                   # first year in data
YR3           = 2026                   # last year in data (partial — through April only)
EARTH_RADIUS  = 6371.009              # km  (John Miller's value)
C_MOLAR_MASS  = 12.011                # g/mol
SOURCE_STRING = ("Miller-Pera FF 2026b, 1993 country bounds. "
                 "CDIAC-AppState 2022; EI 2025; EDGAR 2025 GHG; "
                 "USGS MCS Cement 2026; CarbonMonitor NRT through "
                 "April 2026 (per Andy Jacobson's request).")
CM_METHODS = ("assumed", "cm_yearly")
CMMethod = Literal["assumed", "cm_yearly"]


# ═══════════════════════════════════════════════════════════════════════════════
# Helper functions
# ═══════════════════════════════════════════════════════════════════════════════

def seconds_in_month(year: int, month: int) -> float:
    """Calendar-accurate seconds in a given month."""
    t0 = datetime(year, month, 1, tzinfo=UTC)
    t1 = t0 + relativedelta(months=1)
    return (t1 - t0).total_seconds()


def seconds_in_year(year: int) -> float:
    """Calendar-accurate seconds in a given year."""
    return (datetime(year + 1, 1, 1, tzinfo=UTC) - datetime(year, 1, 1, tzinfo=UTC)).total_seconds()


def cell_areas_m2(earth_radius_km: float = EARTH_RADIUS) -> np.ndarray:
    """Compute 1x1 cell areas in m2 using xESMF, shape (180, 360)."""
    grid = xe.util.grid_global(1, 1, cf=True).drop_vars("latitude_longitude")
    areas_km2 = xe.util.cell_area(grid, earth_radius=earth_radius_km).values
    return areas_km2 * 1e6  # km2 -> m2


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main(method: CMMethod = "assumed") -> None:
    if method not in CM_METHODS:
        raise ValueError(f"Unknown method {method!r}; expected one of {CM_METHODS}")
    npz_file = f"outputs/ff_monthly_2026b_{method}_py.npz"
    monolithic = f"outputs/gml_ff_co2_2026b_{method}.nc"
    file_prefix = f"gml_ff_co2_2026b_{method}"

    nyears_full = YR3 - YR1                       # 33 (1993..2025 inclusive)
    nyears_in_npz = nyears_full + 1               # 34 (npz carries Dec 2026 too)
    nmonths_full = nyears_full * 12               # 396
    provenance = (f"Post-processed {datetime.now(UTC).isoformat()} "
                  f"by {os.path.basename(__file__)} "
                  f"(method={method})")

    global_attrs = {
        "title":       "Miller-Pera Fossil Fuel CO2 prior estimates for CarbonTracker",
        "source":      SOURCE_STRING,
        "history":     f"Created by ff_country_2026.py + {os.path.basename(__file__)}\n{provenance}",
        "institution": "NOAA Global Monitoring Laboratory",
        "Conventions": "CF-1.8",
        "v2026b_annual_method": method,
    }

    # ------------------------------------------------------------------
    # 1. Load Python .npz (produced by ff_country_2026.py)
    # ------------------------------------------------------------------
    print(f"Loading {npz_file} ...")
    npz = np.load(npz_file)
    assert "ff_monthly" in npz, f"Expected key 'ff_monthly' in {npz_file}, found: {list(npz.keys())}"
    last_cm_year = int(npz["last_cm_year"])
    last_output_month = int(npz["last_output_month"])
    last_output_idx = (last_cm_year - YR1) * 12 + last_output_month
    nmonths_npz = nyears_in_npz * 12              # 408
    raw_tll = npz["ff_monthly"].astype(np.float64)  # (time, lon, lat) from Python
    assert raw_tll.shape == (nmonths_npz, 360, 180), (
        f"Expected ff_monthly shape ({nmonths_npz}, 360, 180), got {raw_tll.shape}")
    # Truncate to LAST_OUTPUT_MONTH of LAST_CM_YEAR (e.g. April 2026)
    raw_tll = raw_tll[:last_output_idx]
    nmonths = last_output_idx                     # 400
    n_partial_months = last_output_idx - nmonths_full  # 4 (Jan..Apr 2026)
    print(f"  trimmed to {nmonths} months: {nyears_full} full years + "
          f"{n_partial_months} months in {last_cm_year}")
    # Transpose to (lat, lon, time) to match the rest of the pipeline
    raw_llt = raw_tll.transpose(2, 1, 0)  # (time, lon, lat) -> (lat, lon, time)
    assert raw_llt.shape == (180, 360, nmonths)
    print(f"  ff_monthly shape = {raw_llt.shape}  (lat, lon, time)")

    assert not np.isnan(raw_llt).any(), "NaN in input data"
    assert not np.isinf(raw_llt).any(), "Inf in input data"
    assert (raw_llt >= 0).all(), "Negative values in input data (Gg C should be non-negative)"

    # ------------------------------------------------------------------
    # 2. Build coordinates
    # ------------------------------------------------------------------
    lat = np.linspace(-89.5, 89.5, 180)
    lon = np.linspace(-179.5, 179.5, 360)

    first_day = date(YR1, 1, 15)
    times = [datetime.combine(first_day + relativedelta(months=m), dtime.min)
             for m in range(nmonths)]

    assert all(t2 > t1 for t1, t2 in zip(times[:-1], times[1:], strict=True)), "Time coordinates not monotonically increasing"
    assert all(t.day == 15 for t in times), "Not all time coordinates fall on the 15th"

    # ------------------------------------------------------------------
    # 3. Convert units and reorder to (time, lat, lon)
    #
    #    Gg C / cell / yr  ->  mol C / m2 / s
    #      * 1e9 / 12.011        Gg -> g -> mol
    #      / cell_area_m2        cell -> m2
    #      / seconds_in_year     yr -> s
    #
    #    Then transpose from (lat,lon,time) to (time,lat,lon).
    #    This reorder happens ONCE, BEFORE any file is written.
    # ------------------------------------------------------------------
    print("Computing cell areas ...")
    areas = cell_areas_m2()  # (180, 360), m2

    expected_area = 4 * np.pi * (EARTH_RADIUS * 1e3) ** 2  # m2
    actual_area = areas.sum()
    area_relerr = abs(actual_area - expected_area) / expected_area
    assert area_relerr < 1e-3, (
        f"Cell areas sum {actual_area:.6e} m2 vs 4piR2 {expected_area:.6e} m2, "
        f"rel err {area_relerr:.2e}")
    print(f"  Cell areas sum OK (rel err {area_relerr:.2e} vs 4piR2)")

    print("Converting Gg C -> mol/m2/s and reordering to (time, lat, lon) ...")
    Gg_to_mol = 1e9 / C_MOLAR_MASS

    fossil_llt = np.empty_like(raw_llt)
    # Full years 1993..(YR3-1) get a complete 12-month slice each.
    for yr_idx in range(nyears_full):
        yr = YR1 + yr_idx
        sec_yr = seconds_in_year(yr)
        t0, t1 = yr_idx * 12, (yr_idx + 1) * 12
        fossil_llt[:, :, t0:t1] = raw_llt[:, :, t0:t1] * Gg_to_mol / areas[:, :, np.newaxis] / sec_yr
    # Partial year (YR3, e.g. 2026): only the first n_partial_months are populated.
    # Same Gg-C/yr→mol/m²/s factor — sec_yr is the divisor for the *yearly rate*
    # the npz carries, regardless of how many months we keep.
    if n_partial_months > 0:
        sec_yr = seconds_in_year(YR3)
        t0 = nmonths_full
        t1 = nmonths_full + n_partial_months
        fossil_llt[:, :, t0:t1] = raw_llt[:, :, t0:t1] * Gg_to_mol / areas[:, :, np.newaxis] / sec_yr

    # *** THE REORDER -- everything downstream uses this ***
    fossil = fossil_llt.transpose(2, 0, 1)   # (time, lat, lon)
    raw_tlat = raw_llt.transpose(2, 0, 1)    # raw in (time, lat, lon) for diagnostics
    assert fossil.shape == (nmonths, 180, 360)
    print(f"  Shape after reorder: {fossil.shape}  (time, lat, lon)")

    assert (fossil >= 0).all(), "Negative values in converted fossil_imp (unit conversion error?)"
    for t in range(nmonths):
        assert fossil[t].sum() > 0, f"Month {t} ({times[t]:%Y-%m}) is all zeros"

    # ------------------------------------------------------------------
    # 4. Cross-validate against independent pint-based computation
    # ------------------------------------------------------------------
    _cross_validate_with_pint(raw_llt, fossil, times, lat, lon, areas)

    # ------------------------------------------------------------------
    # 5. Build the canonical xarray Dataset -- (time, lat, lon) throughout
    #
    #    This single Dataset is the source of truth for both the
    #    monolithic file and the per-year files.
    # ------------------------------------------------------------------
    print("\nBuilding canonical dataset ...")

    fossil_imp = xr.DataArray(
        fossil, dims=["time", "lat", "lon"],
        coords={"time": times, "lat": lat, "lon": lon},
        name=VAR_NAME,
        attrs={
            "long_name": "fossil fuel CO2 emissions as mol carbon per unit area per second",
            "units": "mol m-2 s-1",
            "cell_methods": "time: point",
        },
    )

    # Diagnostic: mol C per cell per year, for cross-checking in verify-sums.
    # raw is Gg C/cell/yr; * 1e9 / 12.011 = mol C/cell/yr
    fossil_imp_cell = xr.DataArray(
        raw_tlat * (1e9 / C_MOLAR_MASS), dims=["time", "lat", "lon"],
        coords={"time": times, "lat": lat, "lon": lon},
        name="fossil_imp_cell",
        attrs={"long_name": "mol carbon emitted per cell per year", "units": "mol"},
    )

    cell_areas_da = xr.DataArray(
        areas * 1e-6, dims=["lat", "lon"], coords={"lat": lat, "lon": lon},
    ).pint.quantify("km**2")  # m2 -> km2 for pint metadata

    year_lengths = xr.DataArray(
        [seconds_in_year(t.year) for t in times], dims=["time"],
    ).pint.quantify("s")
    month_lengths = xr.DataArray(
        [seconds_in_month(t.year, t.month) for t in times], dims=["time"],
    ).pint.quantify("s")

    # month-lengths should sum to year-lengths for each FULL year only.
    ml_raw = month_lengths.pint.dequantify().values
    yl_raw = year_lengths.pint.dequantify().values
    for yr_idx in range(nyears_full):
        t0, t1 = yr_idx * 12, (yr_idx + 1) * 12
        ml_sum = ml_raw[t0:t1].sum()
        yr_val = yl_raw[t0]
        assert abs(ml_sum - yr_val) < 1.0, (
            f"Year {YR1 + yr_idx}: month-lengths sum {ml_sum:.0f}s != year-length {yr_val:.0f}s")

    ds = fossil_imp.to_dataset()
    ds["fossil_imp_cell"] = fossil_imp_cell
    ds["cell_areas"] = cell_areas_da
    ds["year_lengths"] = year_lengths
    ds["month_lengths"] = month_lengths
    ds["earth_radius"] = EARTH_RADIUS
    ds["earth_radius"].attrs["units"] = "km"
    ds.attrs = global_attrs

    ds["lat"].attrs  = {"units": "degrees_north", "long_name": "latitude",  "axis": "Y"}
    ds["lon"].attrs  = {"units": "degrees_east",  "long_name": "longitude", "axis": "X"}
    ds["time"].attrs = {"long_name": "time", "axis": "T"}

    # Add bounds
    ds = ds.cf.add_bounds(["lat", "lon"])
    ds = ds.bounds.add_time_bounds("freq", "month")
    ds["time_bnds"].encoding["units"] = "days since 1970-01-01"
    ds["time"].encoding["units"] = "days since 1970-01-01"

    # ------------------------------------------------------------------
    # 6. Write monolithic file
    # ------------------------------------------------------------------
    print(f"Writing monolithic file {monolithic} ...")
    ds.pint.dequantify().to_netcdf(monolithic)
    print(f"  Wrote {monolithic}  {VAR_NAME} shape={ds[VAR_NAME].shape}  dims={ds[VAR_NAME].dims}")
    _assert_dim_order(monolithic, VAR_NAME)

    # Round-trip: read back and verify values survive encoding
    ds_rt = xr.open_dataset(monolithic)
    max_rt_err = float(np.max(np.abs(ds_rt[VAR_NAME].values - fossil)))
    ds_rt.close()
    assert max_rt_err < 1e-20, f"Monolithic round-trip error: max abs diff = {max_rt_err:.2e}"
    print(f"  Round-trip OK (max abs diff {max_rt_err:.2e})")

    # ------------------------------------------------------------------
    # 7. Split per-year files FROM the same dataset (full years only —
    #    skip the partial last year, which is delivered via per-month CT
    #    files instead)
    # ------------------------------------------------------------------
    os.makedirs(YEARLY_DIR, exist_ok=True)
    print(f"\nWriting per-year files to {YEARLY_DIR}/ ...")

    # Dequantify once for the variables that have pint units
    ds_dq = ds.pint.dequantify()

    # Per-year files use float32 (TM5 reads single precision); ~6e-8 relative error
    encoding_template = {
        VAR_NAME: {"dtype": "float32", "zlib": True, "complevel": 4},
        "time":   {"units": "days since 1970-01-01", "calendar": "standard"},
        "lat":    {"dtype": "float64"},
        "lon":    {"dtype": "float64"},
    }

    pgc_per_year = []
    for yr_idx in range(nyears_full):
        yr = YR1 + yr_idx
        t0, t1 = yr_idx * 12, (yr_idx + 1) * 12

        # Slice directly from the canonical dataset
        ds_yr = ds_dq[[VAR_NAME]].isel(time=slice(t0, t1))
        fname = os.path.join(YEARLY_DIR, f"{file_prefix}.{yr:04d}.nc")
        ds_yr.to_netcdf(fname, encoding=encoding_template, unlimited_dims=["time"])

        # Sanity: back-compute annual PgC
        yr_pgc = _annual_pgc(fossil[t0:t1], areas, yr)
        pgc_per_year.append(yr_pgc)
        print(f"  {fname}  {yr_pgc:.4f} PgC")

    if n_partial_months > 0:
        # Diagnostic only — no per-year file for the partial last year.
        t0 = nmonths_full
        t1 = nmonths_full + n_partial_months
        partial_pgc = _annual_pgc_partial(fossil[t0:t1], areas, YR3, n_partial_months)
        print(f"  (partial {YR3} Jan..month {n_partial_months}: {partial_pgc:.4f} PgC — "
              "no per-year file written; CT per-month files cover this range)")

    pgc_total = sum(pgc_per_year)
    print(f"\n  Total full years: {pgc_total:.2f} PgC  "
          f"(mean {pgc_total / nyears_full:.2f} PgC/yr)")

    # Global total bounds: each FULL year should be 5-15 PgC
    for yr_idx, yr_pgc in enumerate(pgc_per_year):
        assert 5.0 < yr_pgc < 15.0, (
            f"Year {YR1 + yr_idx}: {yr_pgc:.4f} PgC outside plausible range [5, 15]")

    # Year-over-year changes should not exceed 20%
    for i in range(1, len(pgc_per_year)):
        change = abs(pgc_per_year[i] - pgc_per_year[i - 1]) / pgc_per_year[i - 1]
        assert change < 0.20, (
            f"Year {YR1 + i}: {change:.1%} change from previous year exceeds 20% "
            f"({pgc_per_year[i - 1]:.4f} -> {pgc_per_year[i]:.4f} PgC)")
    print("  Global totals OK (5-15 PgC/yr, <20% year-over-year change)")

    # Per-year file count (full years only)
    yearly_files = [f for f in os.listdir(YEARLY_DIR)
                    if f.startswith(file_prefix) and f.endswith(".nc")]
    assert len(yearly_files) == nyears_full, (
        f"Expected {nyears_full} yearly files for {file_prefix}, found {len(yearly_files)}")

    # Per-year round-trip: spot-check first year
    first_yr_file = os.path.join(YEARLY_DIR, f"{file_prefix}.{YR1:04d}.nc")
    ds_yr_rt = xr.open_dataset(first_yr_file)
    assert len(ds_yr_rt.time) == 12, f"First-year file has {len(ds_yr_rt.time)} time steps, expected 12"
    orig_slice = fossil[:12]
    # per-year files use float32, so allow some tolerance
    max_rel_err = float(np.max(
        np.abs(ds_yr_rt[VAR_NAME].values - orig_slice) /
        np.where(orig_slice > 0, orig_slice, 1.0)))
    ds_yr_rt.close()
    assert max_rel_err < 1e-6, (
        f"Per-year round-trip error: max rel diff = {max_rel_err:.2e} (float32 encoding?)")
    print(f"  Per-year round-trip OK (max rel diff {max_rel_err:.2e})")

    # ------------------------------------------------------------------
    # 8. Final dimension-order assertion on a per-year file
    # ------------------------------------------------------------------
    _assert_dim_order(os.path.join(YEARLY_DIR, f"{file_prefix}.{YR1:04d}.nc"), VAR_NAME)

    # ------------------------------------------------------------------
    # 9. Write CarbonTracker-format per-year & per-month files
    # ------------------------------------------------------------------
    print(f"\nRunning split_ct_2026.py --method {method} for CT-format output ...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    subprocess.check_call(
        [sys.executable, os.path.join(script_dir, "split_ct_2026.py"),
         "--method", method],
        cwd=script_dir,
    )

    print("\n  Done.  Per-year files ready for TM5.")
    print("  Fortran rc settings:")
    print(f"    ff.input.dir      = <path to {YEARLY_DIR}>")
    print(f"    ff.ncfile.prefix  = {file_prefix}")
    print(f"    ff.ncfile.varname = {VAR_NAME}")


# ═══════════════════════════════════════════════════════════════════════════════
# Validation helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _cross_validate_with_pint(
    raw_llt: np.ndarray,
    fossil_tll: np.ndarray,
    times: list[datetime],
    lat: np.ndarray,
    lon: np.ndarray,
    areas_m2: np.ndarray,
) -> None:
    """Re-derive using pint (same algorithm as the original notebook) and compare."""
    print("Cross-validating against pint-based computation ...")
    c_molar = C_MOLAR_MASS * cf_xarray.units.units("g / mol")

    da = xr.DataArray(
        raw_llt, coords={"lat": lat, "lon": lon, "time": times},
        dims=["lat", "lon", "time"],
    ).pint.quantify("Gg")

    areas_pint = xr.DataArray(
        areas_m2 * 1e-6, dims=["lat", "lon"], coords={"lat": lat, "lon": lon},
    ).pint.quantify("km**2")  # m2 -> km2 to match pint conversion chain

    year_lengths = xr.DataArray(
        [seconds_in_year(t.year) for t in times], dims=["time"],
    ).pint.quantify("s")

    pint_result = (da / c_molar / areas_pint / year_lengths).pint.to("mol / m**2 / s")
    pint_vals = pint_result.pint.dequantify().values  # (lat, lon, time)
    pint_tll = pint_vals.transpose(2, 0, 1)           # (time, lat, lon)

    max_reldiff = np.max(np.abs(fossil_tll - pint_tll) /
                         np.where(np.abs(pint_tll) > 1e-30, np.abs(pint_tll), 1.0))
    print(f"  Max relative difference vs pint: {max_reldiff:.2e}")
    assert max_reldiff < 1e-10, f"Cross-validation failed: max rel diff = {max_reldiff}"
    print("  Cross-validation passed")


def _annual_pgc(data_yr: np.ndarray, areas_m2: np.ndarray, yr: int) -> float:
    """Back-compute PgC/yr from mol/m2/s data with shape (12, 180, 360)."""
    total_mol = 0.0
    for m in range(12):
        spm = seconds_in_month(yr, m + 1)
        total_mol += float(np.sum(data_yr[m] * areas_m2)) * spm
    return total_mol * C_MOLAR_MASS * 1e-15  # mol -> g -> Pg


def _annual_pgc_partial(
    data_partial: np.ndarray, areas_m2: np.ndarray, yr: int, n_months: int,
) -> float:
    """Sum PgC over the first *n_months* of *yr* (used for the partial last year)."""
    total_mol = 0.0
    for m in range(n_months):
        spm = seconds_in_month(yr, m + 1)
        total_mol += float(np.sum(data_partial[m] * areas_m2)) * spm
    return total_mol * C_MOLAR_MASS * 1e-15


def _assert_dim_order(filepath: str, varname: str) -> None:
    """Open a netCDF and assert dimension order is (time, lat, lon)."""
    nc = netCDF4.Dataset(filepath, "r")
    dims = nc.variables[varname].dimensions
    nc.close()

    assert dims == ("time", "lat", "lon"), (
        f"DIMENSION ORDER ERROR in {filepath}: "
        f"expected ('time', 'lat', 'lon'), got {dims}")
    print(f"  Dims verified: {dims}  -> Fortran reads as (lon=360, lat=180, time)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--method", choices=CM_METHODS, default="assumed",
        help="Which v2026b annual-baseline method to post-process.",
    )
    main(method=parser.parse_args().method)
