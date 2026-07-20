#!/usr/bin/env python3
"""split_ct.py — Split the monolithic netCDF into CarbonTracker-format files.

Input:   outputs/{OUTPUT_PREFIX}_<method>.nc   (from post_process.py)
Outputs: outputs/ct/flux1x1_ff_<method>.{YYYY}.nc    (per-year; final year may be partial)
         outputs/ct/flux1x1_ff_<method>.{YYYYMM}.nc  (per-month, 1 month each)

CarbonTracker conventions (matching split.py):
  - time dim named 'date', values are midpoint of each month
  - 'date_bounds' with bounds dim 'bounds'
  - 'decimal_date' variable (fractional years)
  - 'date_components' / 'calendar_components' variables
  - CarbonTracker global attributes (Notes, disclaimer, etc.)
  - Only 'fossil_imp' variable (no diagnostics)
  - Encoding: days since 1900-01-01, float64 for dates, float32 for data
  - Unlimited 'date' dimension
"""

import argparse
import os
import sys
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

from config import CM_METHODS, OUTPUT_PREFIX, SOURCE_STRING, CMMethod
from provenance import provenance_attrs

# ═══════════════════════════════════════════════════════════════════════════════
# Configuration — provenance / methods come from config.py
# ═══════════════════════════════════════════════════════════════════════════════
CT_DIR        = "outputs/ct"
VAR_NAME      = "fossil_imp"


# ═══════════════════════════════════════════════════════════════════════════════
# Build CarbonTracker dataset
# ═══════════════════════════════════════════════════════════════════════════════

def build_carbontracker_dataset(ds_in: xr.Dataset) -> xr.Dataset:
    """Transform the monolithic dataset into CarbonTracker delivery format:
    - time → 'date' (midpoint of each month)
    - date_bounds, decimal_date, date_components, calendar_components
    - CarbonTracker global attributes
    - Only fossil_imp (no diagnostics)
    """
    # Keep fossil_imp plus lat/lon bounds so bounds attributes aren't dangling
    keep_vars = [VAR_NAME]
    for bv in ("lat_bounds", "lon_bounds"):
        if bv in ds_in:
            keep_vars.append(bv)
    ds = ds_in[keep_vars].copy(deep=True)

    # Compute date as midpoint of each month from time_bnds
    bnds = ds_in["time_bnds"].values.astype("datetime64[ns]")  # ensure datetime64
    midpoints = bnds[:, 0] + (bnds[:, 1] - bnds[:, 0]) / 2
    ds["time"] = midpoints
    ds["date_bounds"] = (("time", "bounds"), bnds)

    # Rename time -> date
    ds = ds.rename({"time": "date"})
    ds["date"].attrs["bounds"] = "date_bounds"

    # decimal_date — leap-year aware
    def _decimal_year(dt64: np.datetime64) -> float:
        """Convert datetime64 to decimal year, accounting for leap years."""
        dt = pd.Timestamp(dt64).to_pydatetime().replace(tzinfo=UTC)
        year_start = datetime(dt.year, 1, 1, tzinfo=UTC)
        year_end = datetime(dt.year + 1, 1, 1, tzinfo=UTC)
        frac = (dt - year_start).total_seconds() / (year_end - year_start).total_seconds()
        return dt.year + frac

    ds["decimal_date"] = (
        ("date",),
        np.array([_decimal_year(d) for d in ds["date"].values]),
    )
    ds["decimal_date"].attrs["units"] = "years"

    # date_components
    ds["calendar_components"] = [1, 2, 3, 4, 5, 6]
    ds["date_components"] = (
        ("calendar_components", "date"),
        np.stack([
            ds["date"].dt.year.values,
            ds["date"].dt.month.values,
            ds["date"].dt.day.values,
            ds["date"].dt.hour.values,
            ds["date"].dt.minute.values,
            ds["date"].dt.second.values,
        ]),
    )
    ds["date_components"].attrs["long_name"] = "integer components of UTC date"
    ds["date_components"].attrs["comment"] = (
        "Calendar date components as integers.  Times and dates are UTC."
    )
    ds["date_components"].attrs["order"] = "year, month, day, hour, minute, second"

    # CarbonTracker global attributes
    ds.attrs = {
        "Notes": (
            "This file contains CarbonTracker surface CO2 fluxes averaged "
            "over each time interval.  The times on the date axis are the "
            "centers of each averaging period."
        ),
        "disclaimer": (
            "CarbonTracker is an open product of the NOAA Earth System Research \n"
            "Laboratory using data from the Global Monitoring Division greenhouse \n"
            "gas observational network and collaborating institutions.  Model results \n"
            "including figures and tabular material found on the CarbonTracker \n"
            "website may be used for non-commercial purposes without restriction,\n"
            "but we request that the following acknowledgement text be included \n"
            "in documents or publications made using CarbonTracker results: \n"
            "\n"
            "     CarbonTracker results provided by NOAA/ESRL,\n"
            "     Boulder, Colorado, USA, http://carbontracker.noaa.gov\n"
            "\n"
            "Since we expect to continuously update the CarbonTracker product, it\n"
            "is important to identify which version you are using.  To provide\n"
            "accurate citation, please include the version of the CarbonTracker\n"
            "release in any use of these results.\n"
            "\n"
            "The CarbonTracker team welcomes special requests for data products not\n"
            "offered by default on this website, and encourages proposals for\n"
            "collaborative activities.  Contact us at carbontracker.team@noaa.gov.\n"
        ),
        "email": "carbontracker.team@noaa.gov",
        "url": "http://carbontracker.noaa.gov",
        "institution": "NOAA Earth System Research Laboratory",
        "Conventions": "CF-1.9",
        "history": (
            f"Created on {datetime.now(UTC).isoformat()}\n"
            f"by script {os.path.basename(__file__)}"
        ),
        "Source": f"Miller-Pera fossil fuel emissions estimate — {SOURCE_STRING}",
    }

    return ds


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def write_ct_files(
    ds_ct: xr.Dataset, ct_dir: str, ct_prefix: str, n_months: int,
) -> tuple[list[str], list[str]]:
    """Write per-year + per-month CarbonTracker files to *ct_dir* and validate.

    One per-year file per calendar year present; the final year may be
    partial (< 12 months — the NRT tail) and still gets a per-year file.
    Returns ``(yr_files, mon_files)`` as sorted basenames.
    """
    os.makedirs(ct_dir, exist_ok=True)

    ct_encoding: dict[str, dict[str, Any]] = {
        "date": {"units": "days since 1900-01-01", "dtype": "float64"},
        "calendar_components": {"dtype": "int32"},
        "date_components": {"dtype": "int32"},
        VAR_NAME: {"dtype": "float32", "zlib": True, "complevel": 4},
    }

    print(f"Writing per-year and per-month files to {ct_dir}/ ...")

    n_partial_year_files = 0
    for year_key, ds_yr in ds_ct.groupby("date.year"):
        year = int(year_key)
        n_in_year = int(ds_yr.sizes["date"])
        # One per-year file for every calendar year present. The final year
        # may be partial (< 12 months, the NRT tail) — it still gets a
        # per-year file so consumers see a flux1x1_ff.<year>.nc for it.
        yr_fname = os.path.join(ct_dir, f"{ct_prefix}.{year}.nc")
        ds_yr.to_netcdf(yr_fname, encoding=ct_encoding, unlimited_dims=["date"])
        if n_in_year < 12:
            n_partial_year_files += 1
        tag = "" if n_in_year == 12 else f" (partial, {n_in_year} mo)"
        print(f"  {year}{tag}:", end="")

        for month_key, ds_mon in ds_yr.groupby("date.month"):
            month = int(month_key)
            mon_fname = os.path.join(ct_dir, f"{ct_prefix}.{year}{month:02d}.nc")
            ds_mon.to_netcdf(mon_fname, encoding=ct_encoding, unlimited_dims=["date"])
            print(f" {month}", end="", flush=True)
        print()

    # --- output validation ---
    all_ct = [f for f in os.listdir(ct_dir)
              if f.startswith(ct_prefix + ".") and f.endswith(".nc")]

    def stem(f: str) -> str:
        return f.replace(ct_prefix + ".", "").replace(".nc", "")

    yr_files = sorted(f for f in all_ct if len(stem(f)) == 4 and stem(f).isdigit())
    mon_files = sorted(f for f in all_ct if len(stem(f)) == 6 and stem(f).isdigit())

    n_full_years = n_months // 12
    has_partial = (n_months % 12) != 0
    expected_yr_files = n_full_years + (1 if has_partial else 0)
    assert len(mon_files) == n_months, (
        f"Expected {n_months} per-month files, found {len(mon_files)}")
    # One per-year file per calendar year present (final year may be partial).
    assert len(yr_files) == expected_yr_files, (
        f"Expected {expected_yr_files} per-year files "
        f"({n_full_years} full + {int(has_partial)} partial), "
        f"found {len(yr_files)}")

    # spot-check: months per year-file sum to n_months, each in [1, 12], and
    # at most one partial (< 12) year — the NRT tail.
    total_yr_months = 0
    n_partial_seen = 0
    for yf in yr_files:
        ds_check = xr.open_dataset(os.path.join(ct_dir, yf))
        n = int(ds_check.sizes["date"])
        ds_check.close()
        assert 1 <= n <= 12, f"{yf}: expected 1..12 months, got {n}"
        total_yr_months += n
        if n < 12:
            n_partial_seen += 1
    assert total_yr_months == n_months, (
        f"per-year files cover {total_yr_months} months, expected {n_months}")
    assert n_partial_seen == (1 if has_partial else 0), (
        f"expected {int(has_partial)} partial per-year file, found {n_partial_seen}")

    print(f"\nDone. CarbonTracker-format files written to {ct_dir}/")
    print(f"  {len(yr_files)} per-year files "
          f"({n_full_years} full + {n_partial_year_files} partial), "
          f"{len(mon_files)} per-month files")
    print(f"  Pattern: {ct_prefix}.YYYY.nc  and  {ct_prefix}.YYYYMM.nc")

    return yr_files, mon_files


def main(method: CMMethod = "assumed") -> None:
    if method not in CM_METHODS:
        raise ValueError(f"Unknown method {method!r}; expected one of {CM_METHODS}")
    monolithic = f"outputs/{OUTPUT_PREFIX}_{method}.nc"
    ct_prefix = f"flux1x1_ff_{method}"

    if not os.path.exists(monolithic):
        print(f"ERROR: {monolithic} not found. Run "
              f"post_process --method {method} first.", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {monolithic} ...")
    ds_in = xr.open_dataset(monolithic)

    # --- input validation ---
    for required in [VAR_NAME, "time_bnds"]:
        assert required in ds_in, (
            f"Required variable '{required}' not found in {monolithic}; "
            f"got {list(ds_in.data_vars)}")
    assert "time" in ds_in.dims, (
        f"Required dimension 'time' not found in {monolithic}; "
        f"got {list(ds_in.dims)}")
    n_months = ds_in.sizes["time"]
    assert n_months > 0, "Input dataset has zero time steps"
    # The v2026b product is partial-year — last year may have fewer than 12 months.

    print(f"  {VAR_NAME} shape={ds_in[VAR_NAME].shape}  dims={ds_in[VAR_NAME].dims}")

    print("Building CarbonTracker-format dataset ...")
    ds_ct = build_carbontracker_dataset(ds_in)
    # Run provenance (code commit, package versions, input fingerprints) —
    # propagates to every per-year / per-month CT file sliced from ds_ct.
    ds_ct.attrs.update(provenance_attrs(method=method))

    write_ct_files(ds_ct, CT_DIR, ct_prefix, n_months)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--method", choices=CM_METHODS, default="assumed",
        help="Which v2026b annual-baseline method's monolithic to split.",
    )
    main(method=parser.parse_args().method)
