#!/usr/bin/env python3
"""extrapolate_edgar.py — Extrapolate EDGAR gridded CO₂ fluxes beyond the last
available year using an empirically-derived annual growth rate.

EDGAR releases lag real-time by ~2 years.  This script creates "FAKE" files
that extend coverage to the years needed by the pipeline (ingest_2026.py).

Runs over all three sector directories (TOTALS, NMM, PRO_FFF).  For each
sector the growth rate is estimated from the last N_YEARS_FOR_RATE real EDGAR
files rather than being hardcoded, so the extrapolation tracks recent trends
automatically.  A fallback rate is used if fewer than 2 real years exist.

Source: https://edgar.jrc.ec.europa.eu/dataset_ghg2025
"""

import os
import re
import sys
from datetime import UTC, datetime
from glob import glob

import numpy as np
import xarray as xr

# ═══════════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════════

LAST_PIPELINE_YEAR = 2025   # last year needed by ingest_2026.py
N_YEARS_FOR_RATE   = 3      # how many recent real years to use for rate estimate
FALLBACK_RATE      = 0.01   # fallback if too few real years exist (~recent trend)

# All three sector directories the pipeline needs, with their sector tag and
# the NetCDF variable name used inside each file.
SECTORS = [
    {"dir": "inputs/TOTALS_flx_nc_2025_GHG",   "tag": "TOTALS",  "var": "fluxes"},
    {"dir": "inputs/NMM_flx_nc_2025_GHG",      "tag": "NMM",     "var": "fluxes"},
    {"dir": "inputs/PRO_FFF_flx_nc_2025_GHG",  "tag": "PRO_FFF", "var": "fluxes"},
]


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _find_latest_real_file(edgar_dir, sector_tag):
    """Return (path, year) for the most recent non-FAKE file in edgar_dir."""
    pattern = os.path.join(edgar_dir, f"EDGAR_*_CO2_*_{sector_tag}_flx.nc")
    real_files = [f for f in sorted(glob(pattern)) if "FAKE" not in f]
    if not real_files:
        print(f"ERROR: no real EDGAR files found in {edgar_dir}", file=sys.stderr)
        sys.exit(1)
    latest = real_files[-1]
    m = re.search(r"_CO2_(\d{4})_", latest)
    if not m:
        print(f"ERROR: cannot parse year from {latest}", file=sys.stderr)
        sys.exit(1)
    return latest, int(m.group(1))


def _output_path(edgar_dir, base_name, year):
    """Build the output path for a FAKE extrapolated year.

    Replaces the year in the filename and inserts FAKE before _flx.nc.
    Works for all sector tags (TOTALS, NMM, PRO_FFF).
    Example: EDGAR_2025_GHG_CO2_2024_NMM_flx.nc
          -> EDGAR_2025_GHG_CO2_2025_NMM_FAKE_flx.nc
    """
    name = re.sub(r"(?<=_CO2_)\d{4}(?=_)", str(year), base_name)
    name = re.sub(r"(?<!FAKE)_flx\.nc$", "_FAKE_flx.nc", name)
    return os.path.join(edgar_dir, name)


def _empirical_growth_rate(edgar_dir, sector_tag, var_name, n_years):
    """Estimate annual growth rate from the last n_years real EDGAR files.

    Returns the geometric-mean year-over-year growth rate as a fraction
    (e.g. 0.012 for 1.2 %/yr), or None if fewer than 2 real files exist.
    """
    pattern = os.path.join(edgar_dir, f"EDGAR_*_CO2_*_{sector_tag}_flx.nc")
    real_files = [f for f in sorted(glob(pattern)) if "FAKE" not in f]
    if len(real_files) < 2:
        return None

    files_to_use = real_files[-n_years:]   # at most n_years files
    totals = []
    for f in files_to_use:
        ds = xr.open_dataset(f)
        assert var_name in ds, f"Variable '{var_name}' not found in {f}; got {list(ds.data_vars)}"
        totals.append(float(ds[var_name].sum()))
        ds.close()

    # Geometric mean of consecutive ratios
    ratios = [totals[i] / totals[i - 1] for i in range(1, len(totals))]
    geo_mean_ratio = float(np.prod(ratios) ** (1.0 / len(ratios)))
    return geo_mean_ratio - 1.0


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    timestamp = datetime.now(UTC).isoformat()
    any_written = False

    for sector in SECTORS:
        edgar_dir  = sector["dir"]
        sector_tag = sector["tag"]
        var_name   = sector["var"]

        print(f"\n{'='*60}")
        print(f"Sector: {sector_tag}  ({edgar_dir})")

        base_path, base_year = _find_latest_real_file(edgar_dir, sector_tag)
        base_name = os.path.basename(base_path)
        print(f"Base file: {base_name}  (year {base_year})")

        n_extrap = LAST_PIPELINE_YEAR - base_year
        if n_extrap <= 0:
            print("No extrapolation needed.")
            continue

        # --- Empirical growth rate ---
        rate = _empirical_growth_rate(edgar_dir, sector_tag, var_name, N_YEARS_FOR_RATE)
        if rate is None:
            rate = FALLBACK_RATE
            print(f"Growth rate: {rate:.2%}/yr  (fallback — insufficient history)")
        else:
            print(f"Growth rate: {rate:.2%}/yr  (empirical, last {N_YEARS_FOR_RATE} real years)")

        print(f"Extrapolating {n_extrap} year(s): {base_year + 1} .. {LAST_PIPELINE_YEAR}")

        # --- Load base data once ---
        base = xr.open_dataset(base_path)
        assert var_name in base, (
            f"Expected variable '{var_name}' in {base_path}; got {list(base.data_vars)}"
        )

        # --- Write each extrapolated year ---
        written = []
        for i in range(1, n_extrap + 1):
            target_year = base_year + i
            factor = (1 + rate) ** i
            out_path = _output_path(edgar_dir, base_name, target_year)

            if os.path.exists(out_path):
                print(f"  {target_year}: SKIPPING — {os.path.basename(out_path)} already exists")
                continue

            ds = base.copy(deep=True)
            ds[var_name] = base[var_name] * factor
            ds[var_name].attrs["year"] = str(target_year)  # match real-file string format
            # Scale global_total if present (real files carry e.g. '39.63Gt')
            if "global_total" in ds[var_name].attrs:
                raw = ds[var_name].attrs["global_total"]  # e.g. '39.63Gt'
                base_gt = float(re.sub(r"[^\d.]", "", raw))
                ds[var_name].attrs["global_total"] = f"{base_gt * factor:.2f}Gt"
            ds.attrs["history"] = (
                f"extrapolate_edgar.py {timestamp}  "
                f"base={base_name}  year={target_year}  "
                f"factor={factor:.6f} ({rate:.2%}/yr^{i})"
            )
            ds.to_netcdf(out_path)
            written.append((target_year, factor, out_path))
            print(f"  {target_year}: factor={factor:.4f}  → {os.path.basename(out_path)}")

        base.close()
        any_written = True

        # --- Verification ---
        print("Verifying ...")
        base_check = xr.open_dataset(base_path)
        base_total = float(base_check[var_name].sum())
        base_check.close()

        for target_year, expected_factor, out_path in written:
            assert os.path.exists(out_path), f"Output file missing: {out_path}"
            ds_check = xr.open_dataset(out_path)
            assert var_name in ds_check, f"Variable '{var_name}' missing in {out_path}"
            assert ds_check[var_name].attrs.get("year") == str(target_year), (
                f"Year attribute mismatch in {out_path}"
            )
            actual_ratio = float(ds_check[var_name].sum()) / base_total
            ds_check.close()
            assert abs(actual_ratio - expected_factor) < 1e-5, (
                f"Ratio mismatch for {target_year}: "
                f"expected {expected_factor:.6f}, got {actual_ratio:.6f}"
            )
            print(f"  {target_year}: OK  (ratio={actual_ratio:.6f}, expected={expected_factor:.6f})")

    if not any_written:
        print(f"\nAll sectors already covered through {LAST_PIPELINE_YEAR}. Nothing to do.")
    print("\nDone.")


if __name__ == "__main__":
    main()
