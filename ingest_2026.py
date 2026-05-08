#!/usr/bin/env python3
"""ingest_2026.py — Ingest and process raw input data for the Miller-Pera FF pipeline.

Reads CDIAC, EI, USGS, and EDGAR source files and writes processed CSVs and
NetCDF to processed_inputs/ for consumption by ff_country_2026.py.

Outputs:
  processed_inputs/CDIAC_global_2020.csv
  processed_inputs/CDIAC_national_2020.csv
  processed_inputs/CDIAC_countries.csv
  processed_inputs/EI_frac_changes_2020-2024_global_{oil,gas,coal}.csv
  processed_inputs/EI_national_2024.csv
  processed_inputs/EI_frac_changes_2020-2024_{gas,coal,oil}.csv
  processed_inputs/EI_flaring_bcm.csv
  processed_inputs/USGS_cement_2026.csv
  processed_inputs/USGS_cement_ratios_2020-2026.csv
  processed_inputs/EDGAR_fluxes.nc
  processed_inputs/edgar_patterns.npz
"""

import json
import warnings
from datetime import UTC, datetime
from glob import glob
from itertools import product

import cf_xarray as cfxr  # noqa: F401 — not side-effect free
import cf_xarray.units
import numpy as np
import pandas as pd
import pint  # noqa: F401 — must be imported before pint_xarray
import pint_xarray  # noqa: F401
import xarray as xr
import xcdat  # noqa: F401
import xesmf as xe
from xarray import open_mfdataset

from country_names import load_aliases, load_canonical, validate_names

pd.set_option("future.no_silent_downcasting", True)
xr.set_options(keep_attrs=True)  # type: ignore[no-untyped-call]


# =============================================================================
# Configuration
# =============================================================================

STARTING_YEAR    = 1993
LAST_CDIAC_YEAR  = 2022
LAST_EI_YEAR     = 2024
LAST_CM_YEAR     = 2026  # CarbonMonitor partial-year coverage for v2026b NRT extension
EARTH_RADIUS     = 6371.009  # km, John Miller's value

CDIAC_GLOBAL_XLSX   = "inputs/CDIAC/global.1750_2022.xlsx"
CDIAC_NATIONAL_XLSX = "inputs/CDIAC/nation.1750_2022.xlsx"
EI_XLSX             = "inputs/EI-Stats-Review-ALL-data-2025.xlsx"
EDGAR_NCS           = "inputs/TOTALS_flx_nc_2025_GHG/*.nc"
EDGAR_NMM_NCS       = "inputs/NMM_flx_nc_2025_GHG/*.nc"
EDGAR_PRO_NCS       = "inputs/PRO_FFF_flx_nc_2025_GHG/*.nc"
USGS_CEMENT_CSVS    = "./inputs/USGS_cement/mcs????-cement.csv"
CM_CSV_GLOB         = "inputs/carbon_monitor/carbonmonitor-global_datas_*.csv"
# CarbonMonitor sectors to drop before aggregating (the IDL pipeline excludes
# both aviation channels — they're a small fraction of total and aren't well
# matched to country-of-emission, which is what we want here).
CM_EXCLUDED_SECTORS = {"Domestic Aviation", "International Aviation"}
CM_AGGREGATE_ROWS   = {"EU27", "ROW", "WORLD"}

EI_YEARS       = list(range(STARTING_YEAR, LAST_EI_YEAR + 1))
EI_EXTRAP_YEARS = list(range(LAST_CDIAC_YEAR, LAST_EI_YEAR + 1))

OUTPUT_COLS = [
    "total (Gg C)", "gas_fuel (Gg C)", "liquid_fuel (Gg C)",
    "solid_fuel (Gg C)", "flaring (Gg C)", "cement (Gg C)",
]


# =============================================================================
# Lookup tables
# =============================================================================

# Country name aliases — loaded from inputs/country_aliases.json
# (see also inputs/canonical_countries.csv for the 189 canonical names)
CANONICAL_COUNTRIES = load_canonical()
CANONICAL_SET = set(CANONICAL_COUNTRIES)

CDIAC_RENAMING = load_aliases("CDIAC_2022")

# These countries use "REPUBLIC OF ..." names which would change list alphabetization
# if renamed: REPUBLIC OF CAMEROON, REPUBLIC OF MOLDOVA, UNITED REPUBLIC OF TANZANIA

AGGREGATING_LIST = {
    "ETHIOPIA": ["ETHIOPIA", "ERITREA"],
    "ISRAEL": ["ISRAEL", "OCCUPIED PALESTINIAN TERRITORY"],
    "INDONESIA": ["INDONESIA", "TIMOR-LESTE (FORMERLY EAST TIMOR)"],
    "CANADA": ["CANADA", "ST. PIERRE & MIQUELON"],
    "SPAIN": ["SPAIN", "GIBRALTAR", "ANDORRA"],
    "VENEZUELA": ["VENEZUELA", "ARUBA"],
    "CHINA": ["CHINA", "MACAU"],
    "YUGOSLAVIA": ["YUGOSLAVIA", "MACEDONIA", "CROATIA", "BOSNIA & HERZEGOVINA",
                    "SLOVENIA", "KOSOVO", "SERBIA", "MONTENEGRO"],
    "SOUTH AFRICA": ["SOUTH AFRICA", "LESOTHO"],
    "UNITED KINGDOM": ["UNITED KINGDOM", "ISLE OF MAN"],
    "ST. KITTS-NEVIS": ["ST. KITTS-NEVIS", "ANGUILLA"],
    # Note: Liechtenstein is now combined with Switzerland in CDIAC 2022
    # (was merged with Germany in earlier versions). Separate LIECHTENSTEIN entry is deleted.
    "SUDAN": ["REPUBLIC OF SUDAN", "REPUBLIC OF SOUTH SUDAN"],
}

DELETING_LIST = [
    "CAYMAN ISLANDS", "NIUE", "MONTSERRAT", "PALAU", "BRITISH VIRGIN ISLANDS",
    "ANTARCTIC FISHERIES", "SAINT HELENA", "WALLIS AND FUTUNA ISLANDS",
    "MARSHALL ISLANDS", "FEDERATED STATES OF MICRONESIA", "TURKS AND CAICOS ISLANDS",
    "BONAIRE, SAINT EUSTATIUS, AND SABA", "CURACAO", "NETHERLAND ANTILLES",
    "SAINT MARTIN (DUTCH PORTION)", "TUVALU", "MAYOTTE",
    # Added for CDIAC 2022 (new entries or separate Liechtenstein now in Switzerland)
    "AMERICAN SAMOA", "NORTHERN MARIANA ISLANDS", "LIECHTENSTEIN",
    "SAINT BARTHÉLEMY", "SAINT MARTIN (FRENCH PART)",
]

# Expanded set: canonical names + aggregation members + deleted countries (all Title Case).
# Used to validate EI region JSONs, which reference pre-aggregation country names.
CANONICAL_EXPANDED = CANONICAL_SET | {
    m.title() for members in AGGREGATING_LIST.values() for m in members
} | {d.title() for d in DELETING_LIST}

CDIAC_NATIONAL_COL_RENAMES = {
    "Emissions from fossil fuels and cement production (thousand metric tons of C)": "total (Gg C)",
    "Emissions from solid fuel consumption": "solid_fuel (Gg C)",
    "Emissions from liquid fuel consumption": "liquid_fuel (Gg C)",
    "Emissions from gas fuel consumption": "gas_fuel (Gg C)",
    "Emissions from cement production": "cement (Gg C)",
    "Emissions from gas flaring": "flaring (Gg C)",
    "Emissions per capita (metric tons of carbon)": "per_capita",
    "Emissions from bunker fuels (not included in the totals)": "bunker (Gg C)",
}

# CDIAC version notes:
# - 2013: added Bonaire/St. Eustatius/Saba, Curacao, Neth. Antilles,
#   St. Martin (Dutch) — incomplete records from 1992–2013.
# - 2014: added Tuvalu.
# - 2022: Liechtenstein combined with Switzerland; many country names changed
#   (see CDIAC_RENAMING); Sudan split into pre/post-2011 entries.
# - Neth. Antilles, Turks & Caicos, Tuvalu have GISS codes but are
#   in DELETING_LIST (too small / incomplete records).

EI_RENAMING = load_aliases("EI_2024")

USGS_RENAMES = load_aliases("USGS_2026")


# =============================================================================
# Helper functions
# =============================================================================

def aggregate_countries(
    data: pd.DataFrame, little_names: list[str], big_name: str,
) -> pd.DataFrame:
    """Merge small countries into a single big-country entry."""
    aggregated = data.xs(key=little_names[0], level="Nation").add(
        data.xs(key=little_names[1], level="Nation"), fill_value=0)
    for little_name in little_names[2:]:
        aggregated = aggregated.add(data.xs(key=little_name, level="Nation"), fill_value=0)
    # per_capita is intensive — just take the first country's value
    aggregated["per_capita"] = data.xs(key=little_names[0], level="Nation")["per_capita"]
    aggregated = pd.concat({big_name: aggregated}, names=["Nation"])
    return pd.concat([data.drop(little_names, level="Nation"), aggregated])


def _read_ei_global(sheet: str, skipfooter: int) -> pd.Series:
    """Read EI global row, keeping only integer-year columns."""
    df = pd.read_excel(EI_XLSX, sheet_name=sheet, header=2, index_col=0, skipfooter=skipfooter)
    assert "Total World" in df.index, f"'Total World' not found in sheet '{sheet}' -- check skipfooter"
    row = df.loc["Total World"]
    year_cols = [c for c in row.index if isinstance(c, (int, float)) and float(c) == int(c)]
    return row[year_cols].rename(lambda c: int(c))


def seconds_in_year(year: int) -> float:
    """Seconds in a calendar year (doesn't handle leap seconds — unix smears them)."""
    return (
        datetime(year + 1, 1, 1, tzinfo=UTC).timestamp()
        - datetime(year, 1, 1, tzinfo=UTC).timestamp()
    )


def _load_and_regrid_edgar(
    file_glob: str,
    target_year: int,
    grid_01x01_areas: xr.DataArray,
    label: str,
) -> np.ndarray:
    """Load EDGAR sector files, regrid 0.1° → 1°, and return normalized patterns.

    Returns shape ``(n_years, 180, 360)`` — normalized mass tendency from
    STARTING_YEAR through *target_year*, where each year sums to 1.0.
    """
    files = sorted(glob(file_glob))
    print(f"  {label}: {len(files)} input files")

    def add_id(ds: xr.Dataset) -> xr.Dataset:
        ds.coords["year"] = int(ds.variables["fluxes"].attrs["year"])
        return ds

    ds = open_mfdataset(files, preprocess=add_id, combine="nested", concat_dim="year")
    fluxes_q = ds["fluxes"].pint.quantify(ds["fluxes"].attrs["units"])

    starting_index = list(ds["year"]).index(STARTING_YEAR)

    # Extend to target_year by duplicating last available year
    last_year = int(ds["year"].values[-1])
    if last_year < target_year:
        extras = []
        for yr in range(last_year + 1, target_year + 1):
            extra = fluxes_q[-1:, :, :].copy()
            extra["year"] = [yr]
            extras.append(extra)
        fluxes_q = xr.concat([fluxes_q] + extras, dim="year")

    fluxes_q = fluxes_q[starting_index:]

    # Compute seconds-per-year for these years
    spy = xr.DataArray(
        np.array([seconds_in_year(int(y)) for y in fluxes_q["year"]]) * cf_xarray.units.units("s"),
        dims="year")

    # Compute emissions at 0.1°
    emissions_01 = fluxes_q * grid_01x01_areas * spy

    # Aggregate 0.1° → 1° (manual 10×10 binning)
    num_years = len(fluxes_q["year"])
    agg_1x1 = np.zeros((num_years, 180, 360))
    for yi in range(num_years):
        ems = emissions_01[yi, :, :].pint.to("Mg").to_numpy()
        agg_1x1[yi] = ems.reshape(1800, 360, 10).sum(axis=2).T.reshape(360, 180, 10).sum(axis=2).T

    # Normalize each year to sum to 1.0
    year_sums = agg_1x1.sum(axis=(1, 2), keepdims=True)
    # Avoid division by zero for years with no emissions in this sector
    year_sums = np.where(year_sums > 0, year_sums, 1.0)
    normalized = agg_1x1 / year_sums

    print(f"  {label}: {num_years} years, shape {normalized.shape}")
    return normalized


def _load_carbon_monitor(
    canonical: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load CarbonMonitor near-real-time emissions for v2026b NRT extrapolation.

    Reads the most recent CM CSV from ``inputs/carbon_monitor/``, drops the
    aviation sectors and the EU27 aggregate row, harmonises CM country names
    to the canonical CDIAC list via ``country_aliases.json``, and produces
    four DataFrames keyed on the 189 canonical country names:

    * **monthly totals**  — ``MtCO2`` per country per month for the union of
      months CM covers in 2025 and ``LAST_CM_YEAR`` (e.g., 2026-01..2026-03
      with the May-2026 download). The raw aggregate from which the ratios
      below are derived.
    * **monthly intra-year ratios**  — each month ÷ January of the same
      year, per country. *Diagnostic only* — these were used by the
      original "option 2" overwrite scheme but produced spurious YoY drops
      in non-seasonal regions. Kept in the processed_inputs for inspection.
    * **monthly YoY ratios** — CM[Year, Month] / CM[Year-1, Month], per
      country, for each month in ``LAST_CM_YEAR``. *This is what the
      pipeline actually uses.* Anchoring on the same month a year prior
      preserves whatever seasonal pattern is in our 2025 output and only
      applies a per-country YoY scalar (matches the IDL semantics in
      ``ff_country_new2023a.pro``).
    * **yearly proxy ratio**  — ``sum(CM[Q1 ``LAST_CM_YEAR``]) / sum(CM[Q1
      ``LAST_CM_YEAR - 1``])`` per country. The annual baseline scalar for
      Method B (``cm_yearly``); ignored by Method A (``assumed``).

    Returns ``(monthly_totals, monthly_intra_ratios, monthly_yoy_ratios,
    yearly_ratio)``.
    """
    files = sorted(glob(CM_CSV_GLOB))
    if not files:
        raise FileNotFoundError(
            f"No CarbonMonitor CSV found at {CM_CSV_GLOB}. "
            "Run download_carbon_monitor.py first.",
        )
    csv_path = files[-1]
    print(f"  reading {csv_path}")

    raw = pd.read_csv(csv_path)
    raw = raw.loc[:, ~raw.columns.str.match(r"Unnamed")]
    raw["date"] = pd.to_datetime(raw["date"], format="%d/%m/%Y")
    raw["value"] = pd.to_numeric(raw["value"], errors="coerce")

    # Drop aviation sectors and the EU27 aggregate (we have all 27 members
    # individually). Keep ROW and WORLD as special pseudo-countries.
    raw = raw[~raw["sector"].isin(CM_EXCLUDED_SECTORS)]
    raw = raw[raw["country"] != "EU27"]

    # Map CM names → canonical CDIAC names. Anything not aliased is taken
    # at face value (most CM names match canonical directly).
    aliases = load_aliases("CarbonMonitor_2026")
    raw["canonical"] = raw["country"].replace(aliases)

    # Aggregate sector → country and day → month, then group by canonical
    # so multi-CM-name → single-canonical (e.g., Croatia + Slovenia both
    # → Yugoslavia) sum up.
    raw["yearmonth"] = raw["date"].dt.to_period("M")
    monthly = (
        raw.groupby(["canonical", "yearmonth"], observed=True)["value"]
        .sum()
        .unstack("yearmonth")
        .sort_index()
    )

    # Restrict to LAST_CM_YEAR-1 (full) and LAST_CM_YEAR (partial). We need
    # both to derive the yearly-proxy ratio.
    keep_cols = [c for c in monthly.columns if c.year in {LAST_CM_YEAR - 1, LAST_CM_YEAR}]
    monthly = monthly[keep_cols]
    print(f"  CM coverage: {keep_cols[0]} .. {keep_cols[-1]}  "
          f"({len(keep_cols)} months across {LAST_CM_YEAR - 1}-{LAST_CM_YEAR})")

    # Build the canonical-keyed DataFrames. Countries not directly covered
    # by CM (after aggregation) get the ROW row; the WORLD aggregate is
    # also preserved as its own row for use by the bunker/global total.
    if "ROW" not in monthly.index:
        raise ValueError("ROW row missing from CarbonMonitor data — "
                         "cannot fall back for non-tracked countries")
    if "WORLD" not in monthly.index:
        raise ValueError("WORLD row missing from CarbonMonitor data — "
                         "needed for the bunker/global ratio")

    # Identify direct vs ROW-fallback for each canonical country.
    direct = set(monthly.index) - {"ROW", "WORLD"}
    direct_canonical = direct & set(canonical)
    fallback_canonical = set(canonical) - direct_canonical
    n_direct = len(direct_canonical)
    n_fallback = len(fallback_canonical)
    print(f"  direct CM coverage  : {n_direct} canonical countries")
    print(f"  ROW fallback        : {n_fallback} canonical countries")

    # Validate that every direct canonical name is actually canonical.
    unknown_after_alias = direct - set(canonical)
    if unknown_after_alias:
        raise ValueError(
            f"After aliasing, {len(unknown_after_alias)} CM countries do not "
            f"map to canonical names — extend CarbonMonitor_2026 in "
            f"country_aliases.json: {sorted(unknown_after_alias)}",
        )

    # monthly_totals: 189 canonical rows + WORLD, columns = months.
    monthly_totals = pd.DataFrame(
        index=[*canonical, "WORLD"], columns=keep_cols, dtype=float,
    )
    row_row = monthly.loc["ROW"]
    for name in canonical:
        if name in direct_canonical:
            monthly_totals.loc[name] = monthly.loc[name]
        else:
            monthly_totals.loc[name] = row_row
    monthly_totals.loc["WORLD"] = monthly.loc["WORLD"]
    monthly_totals.columns.name = "yearmonth"

    # monthly_intra_ratios: each month's value / January of the same year.
    # Diagnostic only — see docstring; the YoY ratios below are what the
    # pipeline actually uses for the Feb..Apr 2026 overwrite.
    monthly_intra_ratios = monthly_totals.copy()
    for yr in {LAST_CM_YEAR - 1, LAST_CM_YEAR}:
        cols_yr = [c for c in keep_cols if c.year == yr]
        if not cols_yr:
            continue
        jan = pd.Period(year=yr, month=1, freq="M")
        if jan not in cols_yr:
            continue
        anchor = monthly_totals[jan]
        anchor_safe = anchor.where(anchor > 0)
        for c in cols_yr:
            monthly_intra_ratios[c] = monthly_totals[c] / anchor_safe

    # monthly_yoy_ratios: CM[year=LAST_CM_YEAR, month=M] / CM[year=LAST_CM_YEAR-1, month=M]
    # per row, for every month in LAST_CM_YEAR. NaN where the prior-year
    # value is missing or zero. This is the per-country YoY scalar the
    # pipeline applies as `Feb_2026[cell] = Feb_2025[cell] × YoY_ratio`.
    monthly_yoy_ratios = pd.DataFrame(
        index=monthly_totals.index, dtype=float,
    )
    for c in keep_cols:
        if c.year != LAST_CM_YEAR:
            continue
        prev = pd.Period(year=LAST_CM_YEAR - 1, month=c.month, freq="M")
        if prev not in keep_cols:
            monthly_yoy_ratios[str(c)] = np.nan
            continue
        denom = monthly_totals[prev]
        monthly_yoy_ratios[str(c)] = monthly_totals[c] / denom.where(denom > 0)

    # yearly_ratio (proxy): sum(months in LAST_CM_YEAR) / sum(same months in LAST_CM_YEAR-1).
    cy = [c for c in keep_cols if c.year == LAST_CM_YEAR]
    cy_prev = [pd.Period(year=LAST_CM_YEAR - 1, month=p.month, freq="M") for p in cy]
    cy_prev = [p for p in cy_prev if p in keep_cols]
    if cy and cy_prev:
        num = monthly_totals[cy].sum(axis="columns")
        den = monthly_totals[cy_prev].sum(axis="columns")
        ratio = (num / den.where(den > 0)).rename("yearly_ratio_proxy")
    else:
        ratio = pd.Series(np.nan, index=monthly_totals.index, name="yearly_ratio_proxy")
    months_str = ", ".join(str(p) for p in cy)
    yearly_ratio = pd.DataFrame({
        "yearly_ratio_proxy": ratio,
        "covered_months": months_str,
        "is_direct": [n in direct_canonical or n == "WORLD" for n in monthly_totals.index],
    })

    return monthly_totals, monthly_intra_ratios, monthly_yoy_ratios, yearly_ratio


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    # ─────────────────────────────────────────────────────────────────────────
    # 1. CDIAC global
    # Source: https://rieee.appstate.edu/projects-programs/cdiac/
    # Based on read_cdiac_nation_csv.pro
    # ─────────────────────────────────────────────────────────────────────────
    print("1. CDIAC global ...")
    CDIAC_global = pd.read_excel(CDIAC_GLOBAL_XLSX, sheet_name="Sheet1")
    CDIAC_global = CDIAC_global[CDIAC_global["Year"] >= STARTING_YEAR].set_index("Year")
    CDIAC_global_col_renames = {
        "Emissions from fossil fuels and cement production (million metric tons of C)": "total (Tg C)",
        "Emissions from solid fuel consumption": "solid_fuel (Tg C)",
        "Emissions from liquid fuel consumption": "liquid_fuel (Tg C)",
        "Emissions from gas fuel consumption": "gas_fuel (Tg C)",
        "Emissions from cement production": "cement (Tg C)",
        "Emissions from gas flaring": "flaring (Tg C)",
        "Emissions per capita (metric tons of carbon)": "per_capita (Mg C)",
    }
    CDIAC_global.rename(CDIAC_global_col_renames, axis="columns", inplace=True)

    gg_cols = ["total (Gg C)", "solid_fuel (Gg C)", "liquid_fuel (Gg C)", "gas_fuel (Gg C)", "cement (Gg C)", "flaring (Gg C)"]
    tg_cols = ["total (Tg C)", "solid_fuel (Tg C)", "liquid_fuel (Tg C)", "gas_fuel (Tg C)", "cement (Tg C)", "flaring (Tg C)"]
    CDIAC_global[gg_cols] = 1000 * CDIAC_global[tg_cols]

    CDIAC_global.to_csv("processed_inputs/CDIAC_global_2020.csv", columns=OUTPUT_COLS)

    assert len(CDIAC_global) == LAST_CDIAC_YEAR - STARTING_YEAR + 1, \
        f"Expected {LAST_CDIAC_YEAR - STARTING_YEAR + 1} years, got {len(CDIAC_global)}"
    assert not CDIAC_global[OUTPUT_COLS].isna().any().any(), "NaN in CDIAC global data"
    print(f"  {len(CDIAC_global)} years, {STARTING_YEAR}-{LAST_CDIAC_YEAR}")

    # ─────────────────────────────────────────────────────────────────────────
    # 2. CDIAC national
    # ─────────────────────────────────────────────────────────────────────────
    print("2. CDIAC national ...")
    CDIAC_national = pd.read_excel(CDIAC_NATIONAL_XLSX, sheet_name="Sheet1")
    CDIAC_national["Nation"] = CDIAC_national["Nation"].str.upper()
    CDIAC_national = CDIAC_national.set_index(["Nation", "Year"])

    # give countries+cols more standard names
    CDIAC_national.rename(CDIAC_RENAMING, level=0, inplace=True)
    CDIAC_national.rename(CDIAC_NATIONAL_COL_RENAMES, axis="columns", inplace=True)

    # merge small countries onto neighboring big countries
    for big, littles in AGGREGATING_LIST.items():
        CDIAC_national = aggregate_countries(CDIAC_national, littles, big)
    CDIAC_national.rename({"YUGOSLAVIA2": "YUGOSLAVIA"}, level=0, inplace=True)

    # delete even smaller countries
    CDIAC_national.drop(DELETING_LIST, level=0, inplace=True)

    # only keep data after year countries stabilize, fix indices
    CDIAC_national.reset_index(inplace=True)
    CDIAC_national["Nation"] = CDIAC_national["Nation"].str.title()
    CDIAC_national = CDIAC_national[CDIAC_national["Year"] >= STARTING_YEAR]
    CDIAC_national.set_index(["Nation", "Year"], inplace=True)

    # French departments have no CDIAC data after 2010 — insert NaN rows so
    # the per-nation interpolation can fill them (extrapolate from 2010 trend)
    for department, year in product(
        ["French Guiana", "Guadeloupe", "Martinique", "Reunion"],
        range(2011, LAST_CDIAC_YEAR + 1),
    ):
        CDIAC_national.loc[(department, year), :] = np.nan
    CDIAC_national.sort_index(axis="index", inplace=True)

    CDIAC_countries = CDIAC_national.index.get_level_values("Nation").unique().to_series()

    # Sector columns used for the total-recomputation fix (and NaN audit below)
    _sector_cols_fix = ["gas_fuel (Gg C)", "liquid_fuel (Gg C)", "solid_fuel (Gg C)",
                        "flaring (Gg C)", "cement (Gg C)"]

    # Audit NaN counts before interpolation (logged for transparency)
    _nan_before = CDIAC_national[_sector_cols_fix].isna().sum()
    _nan_nations = {
        col: CDIAC_national[CDIAC_national[col].isna()].index.get_level_values("Nation").unique().tolist()
        for col in _sector_cols_fix if CDIAC_national[col].isna().any()
    }
    if _nan_before.any():
        print("  NaN sector values before interpolation (will be filled linearly within each nation):")
        for col, n in _nan_before[_nan_before > 0].items():
            print(f"    {col}: {n} rows — {_nan_nations[col]}")

    # interpolate within each nation only (ffill handles trailing NaN, e.g. French
    # departments that only have CDIAC data through 2010)
    for nation in CDIAC_national.index.get_level_values("Nation").unique():
        CDIAC_national.loc[[nation]] = pd.concat(
            {nation: CDIAC_national.loc[nation].interpolate().ffill()}, names=["Nation"])

    # Fix rows where the reported total doesn't match the sector sum.
    # This arises when a sector (commonly flaring) was NaN in the raw xlsx and
    # was filled by interpolation above, but the total column was not updated.
    # Recompute total as sector sum wherever the discrepancy exceeds 1 Gg C.
    _sector_sum = CDIAC_national[_sector_cols_fix].sum(axis=1)
    _mismatch = (_sector_sum - CDIAC_national["total (Gg C)"]).abs()
    _bad = _mismatch > 1.0
    if _bad.any():
        print(f"  Fixing {_bad.sum()} rows where total != sector sum (recomputing total):")
        print(f"    {CDIAC_national[_bad].index.get_level_values('Nation').unique().tolist()}")
        CDIAC_national.loc[_bad, "total (Gg C)"] = _sector_sum[_bad]

    # Post-fix assertion: no discrepancy should remain
    _remaining = (_sector_sum - CDIAC_national["total (Gg C)"]).abs()
    assert not (_remaining > 1.0).any(), \
        f"Sector/total discrepancy still present after fix: " \
        f"{CDIAC_national[_remaining > 1.0].index.tolist()}"

    # output — fill extrapolated missing values with zero
    CDIAC_national_filled = CDIAC_national.fillna(0)
    CDIAC_national_filled.to_csv("processed_inputs/CDIAC_national_2020.csv", columns=OUTPUT_COLS)
    CDIAC_countries.to_csv("processed_inputs/CDIAC_countries.csv", columns=[], header=False)

    # --- validate against canonical country list ---
    validate_names("CDIAC", set(CDIAC_countries), CANONICAL_SET, strict=True)
    assert CDIAC_countries.tolist() == CANONICAL_COUNTRIES, \
        "CDIAC country order differs from canonical list"

    # --- validation ---
    n_nations = CDIAC_national.index.get_level_values("Nation").nunique()
    n_years = CDIAC_national.index.get_level_values("Year").nunique()
    assert n_years == LAST_CDIAC_YEAR - STARTING_YEAR + 1, \
        f"Expected {LAST_CDIAC_YEAR - STARTING_YEAR + 1} years, got {n_years}"

    rows_per_nation = CDIAC_national_filled.groupby("Nation").size()
    incomplete = rows_per_nation[rows_per_nation != n_years]
    assert incomplete.empty, f"Countries with incomplete year coverage:\n{incomplete}"

    expected_years = list(range(STARTING_YEAR, LAST_CDIAC_YEAR + 1))
    for nation in CDIAC_national_filled.index.get_level_values("Nation").unique()[:5]:
        nation_years = sorted(CDIAC_national_filled.loc[nation].index.get_level_values("Year"))
        assert nation_years == expected_years, \
            f"{nation}: years are {nation_years[:5]}..., expected {expected_years[:5]}..."
    assert not CDIAC_national_filled.index.duplicated().any(), "Duplicate (Nation, Year) rows in CDIAC national"

    sector_cols = ["gas_fuel (Gg C)", "liquid_fuel (Gg C)", "solid_fuel (Gg C)", "flaring (Gg C)", "cement (Gg C)"]
    sector_sum = CDIAC_national_filled[sector_cols].sum(axis=1)
    reported_total = CDIAC_national_filled["total (Gg C)"]
    sector_mismatch = (sector_sum - reported_total).abs()
    worst_mismatch = sector_mismatch.max()
    if worst_mismatch > 1.0:
        print(f"  WARNING: sector sum != reported total, max mismatch = {worst_mismatch:.1f} Gg C")
    else:
        print(f"  Sector sum check OK (max mismatch {worst_mismatch:.2f} Gg C)")

    for col in OUTPUT_COLS:
        neg_rows = CDIAC_national_filled[CDIAC_national_filled[col] < 0]
        if len(neg_rows) > 0:
            nations = neg_rows.index.get_level_values("Nation").unique().tolist()
            print(f"  WARNING: {len(neg_rows)} rows with negative {col}: {nations}")

    country_sums = CDIAC_national_filled.groupby("Year")["total (Gg C)"].sum()
    global_totals = CDIAC_global["total (Gg C)"]
    bunker_frac = (global_totals - country_sums) / global_totals
    assert (global_totals >= country_sums).all(), "Country totals exceed global total (bunker fuels would be negative)"
    if (bunker_frac > 0.10).any():
        print(f"  WARNING: bunker fraction exceeds 10% in some years: max = {bunker_frac.max():.1%}")
    print(f"  {n_nations} nations, {n_years} years, bunker fraction {bunker_frac.min():.1%}--{bunker_frac.max():.1%}")

    # ─────────────────────────────────────────────────────────────────────────
    # 3. EI global (for extrapolation)
    # Source: https://www.energyinst.org/statistical-review
    # Oil, coal, and gas all in energy units — exajoules.
    # ─────────────────────────────────────────────────────────────────────────
    print("3. EI global ...")
    global_oil     = _read_ei_global("Primary Energy Cons (old meth)", 10)
    global_gas     = _read_ei_global("Gas Consumption - EJ", 14)
    global_coal    = _read_ei_global("Coal Consumption - EJ", 13)
    global_flaring = _read_ei_global("CO2 from Flaring", 13)

    global_EI = pd.concat(
        [global_oil, global_gas, global_coal, global_flaring],
        axis=1, keys=["oil", "gas", "coal", "flaring"],
    ).stack().unstack(0)

    (global_oil.pct_change() + 1).dropna().to_csv("processed_inputs/EI_frac_changes_2020-2024_global_oil.csv", index=False)
    (global_gas.pct_change() + 1).dropna().to_csv("processed_inputs/EI_frac_changes_2020-2024_global_gas.csv", index=False)
    (global_coal.pct_change() + 1).dropna().to_csv("processed_inputs/EI_frac_changes_2020-2024_global_coal.csv", index=False)

    # Global flaring volumes in BCM (billions of cubic metres) — used by
    # ff_country_2026.py for flaring-sector extrapolation ratios.
    global_flaring_bcm = _read_ei_global("Natural Gas Flaring", 13)
    global_flaring_bcm.to_csv("processed_inputs/EI_flaring_bcm.csv",
                              header=["BCM"], index_label="Year")
    print(f"  Flaring BCM: {len(global_flaring_bcm)} years written")

    print(f"  {len(global_EI)} fuel-years")

    # ─────────────────────────────────────────────────────────────────────────
    # 4. EI flaring (different country set, treated separately)
    # ─────────────────────────────────────────────────────────────────────────
    print("4. EI flaring ...")
    EI_flaring = pd.read_excel(
        EI_XLSX, sheet_name="CO2 from Flaring", index_col=0, header=2, skipfooter=13,
    ).dropna(axis="index", how="all").dropna(axis="columns", how="all")
    EI_flaring.index.names = ["Nation"]
    EI_flaring.rename(EI_RENAMING, level="Nation", inplace=True)
    EI_flaring.reset_index(inplace=True)
    EI_flaring = EI_flaring[
        ~EI_flaring["Nation"].str.startswith("Total")
        & ~EI_flaring["Nation"].str.lower().str.startswith("of which")
    ].set_index("Nation")

    with open("inputs/EI_2024_flaring_regions.json") as f:
        EI_flaring_regions = json.load(f)
    for members in EI_flaring_regions.values():
        validate_names("EI flaring regions JSON", set(members), CANONICAL_EXPANDED)

    for EI_label, region_countries in EI_flaring_regions.items():
        for country in region_countries:
            EI_flaring.loc[country, :] = EI_flaring.loc[EI_label, :]
    EI_flaring.drop(EI_flaring_regions.keys(), inplace=True)

    EI_flaring.reset_index(inplace=True)
    EI_flaring["Nation"] = EI_flaring["Nation"].str.title()
    EI_flaring.set_index("Nation", inplace=True)
    EI_flaring.sort_index(inplace=True)
    print(f"  {len(EI_flaring)} nations")

    # ─────────────────────────────────────────────────────────────────────────
    # 5. EI fuels (oil, gas, coal)
    # ─────────────────────────────────────────────────────────────────────────
    print("5. EI fuels ...")
    EI_oil = pd.read_excel(EI_XLSX, sheet_name="Primary Energy Cons (old meth)", header=2, index_col=0, skipfooter=10).dropna(axis="index", how="all").dropna(axis="columns", how="all")
    EI_gas = pd.read_excel(EI_XLSX, sheet_name="Gas Consumption - EJ", header=2, index_col=0, skipfooter=14).dropna(axis="index", how="all").dropna(axis="columns", how="all")
    EI_coal = pd.read_excel(EI_XLSX, sheet_name="Coal Consumption - EJ", header=2, index_col=0, skipfooter=13).dropna(axis="index", how="all").dropna(axis="columns", how="all")

    for label, df in [("oil", EI_oil), ("gas", EI_gas), ("coal", EI_coal)]:
        assert "Total World" in df.index, f"'Total World' missing from EI {label} -- check skipfooter"

    EI_fuels = pd.concat({"oil": EI_oil, "coal": EI_coal, "gas": EI_gas}, names=["Fuel_type"])
    EI_fuels.index.names = ["Fuel_type", "Nation"]
    EI_fuels.rename(EI_RENAMING, level="Nation", inplace=True)
    EI_fuels = EI_fuels[
        ~EI_fuels.index.get_level_values("Nation").str.startswith("Total")
        & ~EI_fuels.index.get_level_values("Nation").str.lower().str.startswith("of which")
    ]
    EI_fuels.reset_index("Nation", inplace=True)

    # Macedonia, Slovenia, and Croatia go into Yugoslavia, which is in 'Other Europe'
    yugoslavia_extras = EI_fuels[EI_fuels["Nation"] == "Macedonia"].add(
        EI_fuels[EI_fuels["Nation"] == "Slovenia"].add(EI_fuels[EI_fuels["Nation"] == "Croatia"]))
    yugoslavia_extras["Nation"] = ""
    EI_fuels[EI_fuels["Nation"] == "Other Europe"] = EI_fuels[EI_fuels["Nation"] == "Other Europe"].add(yugoslavia_extras)
    EI_fuels = EI_fuels.reset_index("Fuel_type").set_index("Nation")
    EI_fuels.drop(["Macedonia", "Slovenia", "Croatia", "USSR"], axis="index", inplace=True)

    EI_fuels = EI_fuels.reset_index().set_index(["Nation", "Fuel_type"])
    EI_fuels.sort_index(inplace=True)

    # NOTE: Afghanistan moved to 'Other Asia Pacific', in accordance with EI Definitions tab
    with open("inputs/EI_2024_fuel_regions.json") as f:
        EI_fuel_regions = json.load(f)
    for members in EI_fuel_regions.values():
        validate_names("EI fuel regions JSON", set(members), CANONICAL_EXPANDED)

    # checks that countries are appropriately represented in CDIAC, EI, and regions
    all_extras = []
    for nations in EI_fuel_regions.values():
        all_extras += nations
    Extra_countries = set(all_extras)
    CDIAC_countries_set = set(CDIAC_national.index.get_level_values(0).str.title().unique())
    EI_countries = set(EI_fuels.index.get_level_values(0).str.title().unique())

    missing_countries = (CDIAC_countries_set - EI_countries) - Extra_countries
    duplicate_countries = Extra_countries - (CDIAC_countries_set - EI_countries)
    assert not missing_countries, f"CDIAC countries missing from EI + regions: {missing_countries}"
    assert not duplicate_countries, f"Region countries duplicating EI entries: {duplicate_countries}"
    print(f"  Country check passed: {len(CDIAC_countries_set)} CDIAC, {len(EI_countries)} EI, {len(Extra_countries)} from regions")

    # expand regions to individual countries
    for EI_label, CDIAC_nations in EI_fuel_regions.items():
        for CDIAC_country in CDIAC_nations:
            dupe = pd.concat({CDIAC_country: EI_fuels.loc[EI_label]}, names=["Nation"])
            EI_fuels = pd.concat([EI_fuels, dupe])
    EI_fuels.sort_index(inplace=True)
    EI_fuels.drop(EI_fuel_regions.keys(), inplace=True)

    EI_fuels.reset_index(inplace=True)
    EI_fuels["Nation"] = EI_fuels["Nation"].str.title()
    EI_fuels.set_index(["Nation", "Fuel_type"], inplace=True)

    # ─────────────────────────────────────────────────────────────────────────
    # 6. EI combined + ratios for extrapolation
    # ─────────────────────────────────────────────────────────────────────────
    print("6. EI combined + ratios ...")
    EI_combined = EI_fuels.reset_index().set_index(["Fuel_type", "Nation"]).sort_index()
    x = pd.concat([EI_flaring], keys=["flaring"], names=["Fuel_type"])
    EI_combined = pd.concat([EI_combined, x]).reset_index()
    EI_combined = EI_combined.set_index(["Nation", "Fuel_type"]).sort_index()
    # merge Gibraltar (only flaring) onto Spain
    EI_combined.loc["Spain", "flaring"] += EI_combined.loc["Gibraltar", "flaring"]
    EI_combined.drop("Gibraltar", axis="index", level="Nation", inplace=True)

    # every CDIAC country should have all 4 fuel types
    expected_fuels = {"coal", "flaring", "gas", "oil"}
    nations_in_combined = EI_combined.index.get_level_values("Nation").str.title().unique()
    for nation in CDIAC_countries:
        if nation not in nations_in_combined:
            print(f"  WARNING: CDIAC country '{nation}' missing entirely from EI combined")
        else:
            fuels_present = set(EI_combined.loc[nation].index.get_level_values("Fuel_type"))
            missing_fuels = expected_fuels - fuels_present
            assert not missing_fuels, f"Country '{nation}' missing fuel types: {missing_fuels}"

    # EI per-country totals should approximately match the global 'Total World' row
    for fuel, global_series in [("oil", global_oil), ("gas", global_gas), ("coal", global_coal)]:
        national_sum = EI_combined.xs(fuel, level="Fuel_type")[EI_YEARS].sum(axis=0)
        global_vals = global_series[EI_YEARS]
        rel_err = ((national_sum - global_vals) / global_vals).abs()
        worst_year = rel_err.idxmax()
        if rel_err.max() > 0.05:
            print(f"  WARNING: EI {fuel} national sum vs global differs by {rel_err.max():.1%} in {worst_year}")
        else:
            print(f"  EI {fuel} national vs global OK (max {rel_err.max():.1%} in {worst_year})")

    EI_combined_ranged = EI_combined[EI_YEARS]
    EI_csv = EI_combined_ranged.stack().unstack(level="Fuel_type").rename(
        columns={"coal": "coal (EJ)", "flaring": "flaring (TG CO2)", "oil": "oil (EJ)", "gas": "gas (EJ)"})
    EI_csv.to_csv("processed_inputs/EI_national_2024.csv")

    # validate EI country names against canonical list
    # (Ussr is a historical EI entry dropped in ratio calculations)
    ei_nations = set(EI_csv.index.get_level_values("Nation").unique()) - {"Ussr"}
    validate_names("EI", ei_nations, CANONICAL_SET)

    # ratios for extrapolation
    fractional_changes = (
        EI_combined[[LAST_CDIAC_YEAR - 1] + EI_EXTRAP_YEARS]
        .stack().unstack(level=1)
        .groupby(level="Nation").pct_change(fill_method=None)
        .drop(labels=LAST_CDIAC_YEAR - 1, level=1)
        .replace([np.inf, -np.inf], np.nan) + 1
    ).astype(float)
    global_fractional_changes = global_EI.T.pct_change(fill_method=None).T[EI_EXTRAP_YEARS].T + 1

    for nation in fractional_changes.index.get_level_values("Nation").unique():
        filled = fractional_changes.loc[nation].fillna(global_fractional_changes)
        fractional_changes.loc[nation] = filled.values

    fractional_changes["gas"].unstack().drop(["Ussr"]).to_csv("processed_inputs/EI_frac_changes_2020-2024_gas.csv")
    fractional_changes["coal"].unstack().drop(["Ussr"]).to_csv("processed_inputs/EI_frac_changes_2020-2024_coal.csv")
    fractional_changes["oil"].unstack().drop(["Ussr"]).to_csv("processed_inputs/EI_frac_changes_2020-2024_oil.csv")
    fractional_changes["flaring"].unstack().drop(["Ussr"], errors="ignore").to_csv("processed_inputs/EI_frac_changes_2020-2024_flaring.csv")

    n_expected = CDIAC_national.index.get_level_values("Nation").nunique()
    for fuel in ["gas", "coal", "oil", "flaring"]:
        fc = fractional_changes[fuel].unstack().drop(["Ussr"], errors="ignore")
        assert len(fc) == n_expected, f"EI {fuel} ratios: {len(fc)} nations, expected {n_expected}"
        assert fc.notna().all().all(), f"NaN in EI {fuel} fractional changes after fill"

    for fuel in ["gas", "coal", "oil", "flaring"]:
        fc = fractional_changes[fuel].unstack().drop(["Ussr"], errors="ignore")
        extreme = (fc < 0.5) | (fc > 2.0)
        n_extreme = extreme.sum().sum()
        if n_extreme > 0:
            worst_nations = fc[extreme.any(axis=1)].index.tolist()
            print(f"  WARNING: {n_extreme} extreme EI {fuel} ratios (outside [0.5, 2.0]) in: {worst_nations[:5]}{'...' if len(worst_nations) > 5 else ''}")

    # ─────────────────────────────────────────────────────────────────────────
    # 7. USGS Cement
    # Source: https://www.usgs.gov/centers/national-minerals-information-center/cement-statistics-and-information
    # ─────────────────────────────────────────────────────────────────────────
    print("7. USGS cement ...")
    # these CSVs are constructed by manually reading the PDF
    cement_datas = []
    for cement_datafile in sorted(glob(USGS_CEMENT_CSVS)):
        cement_data = pd.read_csv(cement_datafile)
        cement_data.columns = cement_data.columns.str.replace(" (Gg)", "", regex=False)
        cement_data = pd.wide_to_long(cement_data, ["Cement", "Clinker"], i="Nation", j="Year", sep=" ")
        cement_datas.append(cement_data)

    total_cement = pd.concat(cement_datas).sort_index().rename(USGS_RENAMES, axis="index")
    total_cement = total_cement.reset_index().drop_duplicates(
        subset=["Nation", "Year"], keep="last").set_index(["Nation", "Year"]).sort_index()
    total_cement.to_csv("./processed_inputs/USGS_cement_2026.csv")

    # validate USGS country names against canonical list
    usgs_skip = {"Other countries", "Other countries (rounded)", "World total (rounded)"}
    usgs_nations = set(total_cement.index.get_level_values("Nation").unique()) - usgs_skip
    validate_names("USGS", usgs_nations, CANONICAL_SET)

    cement_ratios = total_cement.drop(columns="Clinker").drop(labels="World total (rounded)").stack().unstack(level="Year")

    zero_base = cement_ratios[cement_ratios[2020] == 0]
    if len(zero_base) > 0:
        print(f"  WARNING: {len(zero_base)} countries have zero cement production in base year 2020 (will be dropped by dropna):")
        print(f"    {zero_base.index.get_level_values('Nation').unique().tolist()}")

    cement_ratios = cement_ratios.div(cement_ratios[2020], axis=0).dropna(how="all")
    cement_other_nations = cement_ratios.xs("Other countries (rounded)").reset_index()
    cement_ratios = cement_ratios.reset_index().drop(columns=["level_1"])

    new_rows = []
    for CDIAC_country in CDIAC_countries:
        if CDIAC_country not in list(cement_ratios["Nation"]):
            temp = cement_other_nations.copy()
            temp["Nation"] = CDIAC_country
            new_rows.append(temp)

    cement_ratios = pd.concat([cement_ratios, *new_rows]).set_index("Nation").sort_index()
    cement_ratios = cement_ratios.drop("Other countries (rounded)").drop(
        columns=[c for c in ["index", 2018, 2019] if c in cement_ratios.columns])
    cement_ratios = cement_ratios.stack("Year").to_frame()
    cement_ratios.columns = ["cement"]

    assert (cement_ratios["cement"] > 0).all(), \
        f"Non-positive cement ratios found:\n{cement_ratios[cement_ratios['cement'] <= 0]}"
    extreme_cement = cement_ratios[(cement_ratios["cement"] < 0.1) | (cement_ratios["cement"] > 10)]
    if len(extreme_cement) > 0:
        print(f"  WARNING: {len(extreme_cement)} extreme cement ratios (outside [0.1, 10x of 2020]):")
        print(extreme_cement.head(10).to_string())

    cement_ratios.to_csv("processed_inputs/USGS_cement_ratios_2020-2026.csv")
    print(f"  {len(total_cement)} nation-years, {len(cement_ratios)} ratios")

    # ─────────────────────────────────────────────────────────────────────────
    # 7b. CarbonMonitor near-real-time monthly data (for v2026b NRT extension)
    # Source: https://carbonmonitor.org/  (download via download_carbon_monitor.py)
    # Used to fill in Feb..Apr 2026 in the partial-year extension. Two
    # outputs: a per-country yearly-ratio proxy (Q1 2026 / Q1 2025) for
    # Method B's annual baseline, and per-country monthly ratios within
    # 2026 (each month / Jan 2026) for the within-year shape.
    # ─────────────────────────────────────────────────────────────────────────
    print("7b. CarbonMonitor (NRT for v2026b extension) ...")
    cm_canonical = CDIAC_countries.tolist()
    cm_totals, cm_intra, cm_yoy, cm_yearly = _load_carbon_monitor(cm_canonical)
    cm_totals.to_csv("processed_inputs/CM_monthly_totals_2025-2026.csv")
    # _intra is diagnostic only; the pipeline uses _yoy.
    cm_intra.to_csv("processed_inputs/CM_monthly_intra_ratios_2025-2026.csv")
    cm_yoy.to_csv("processed_inputs/CM_monthly_yoy_ratios_2026.csv")
    cm_yearly.to_csv("processed_inputs/CM_yearly_ratio_proxy_2026.csv")

    # ─────────────────────────────────────────────────────────────────────────
    # 8. EDGAR emissions for spatial patterning (sector-specific)
    # Source: https://edgar.jrc.ec.europa.eu/dataset_ghg2025
    # Three patterns: combustion (gas/oil/coal), flaring (PRO_FFF), cement (NMM)
    # Combustion = TOTALS − NMM − PRO_FFF
    # ─────────────────────────────────────────────────────────────────────────
    print("8. EDGAR (sector-specific spatial patterns) ...")

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*cannot be divided by.*", category=UserWarning)
        grid_01x01 = xe.util.grid_global(.1, .1, cf=True)
    grid_01x01_areas = xr.DataArray(
        xe.util.cell_area(grid_01x01, earth_radius=EARTH_RADIUS).to_numpy(),
        dims=["lat", "lon"]).pint.quantify("km^2")

    target_year = LAST_EI_YEAR + 1  # = 2025

    pat_totals = _load_and_regrid_edgar(EDGAR_NCS, target_year, grid_01x01_areas, "TOTALS")
    pat_nmm    = _load_and_regrid_edgar(EDGAR_NMM_NCS, target_year, grid_01x01_areas, "NMM (cement)")
    pat_pro    = _load_and_regrid_edgar(EDGAR_PRO_NCS, target_year, grid_01x01_areas, "PRO_FFF (flaring)")

    # Combustion = TOTALS − NMM − PRO_FFF (clamp negatives, renormalize)
    pat_combust = pat_totals - pat_nmm - pat_pro
    pat_combust = np.maximum(pat_combust, 0.0)
    combust_sums = pat_combust.sum(axis=(1, 2), keepdims=True)
    combust_sums = np.where(combust_sums > 0, combust_sums, 1.0)
    pat_combust = pat_combust / combust_sums

    num_years = pat_totals.shape[0]
    expected_n_years = target_year - STARTING_YEAR + 1
    assert num_years == expected_n_years, \
        f"Expected {expected_n_years} years, got {num_years}"

    # Validate all patterns
    for name, pat in [("TOTALS", pat_totals), ("NMM", pat_nmm),
                      ("PRO_FFF", pat_pro), ("combustion", pat_combust)]:
        for yi in range(num_years):
            s = pat[yi].sum()
            assert abs(s - 1.0) < 1e-6, \
                f"{name} year {yi}: normalized sum = {s}, expected 1.0"
        assert (pat >= 0).all(), f"{name}: has negative values"

    print(f"  All patterns OK: {num_years} years, "
          f"non-negative, normalized to 1.0")

    # Save sector-specific patterns: (180, 360, n_years, 3)
    # axis 3: 0=combustion, 1=flaring, 2=cement
    # Also save TOTALS for bunker-fuel distribution
    fracarr_sectors = np.stack(
        [pat_combust, pat_pro, pat_nmm], axis=-1)           # (n_years, 180, 360, 3)
    fracarr_sectors = fracarr_sectors.transpose((1, 2, 0, 3))  # (180, 360, n_years, 3)
    np.savez_compressed("processed_inputs/edgar_patterns.npz",
                        fracarr=fracarr_sectors,
                        totals=pat_totals.transpose((1, 2, 0)))
    print(f"  Saved edgar_patterns.npz: fracarr={fracarr_sectors.shape} "
          f"(combustion/flaring/cement), totals={(180, 360, num_years)}")

    # ─────────────────────────────────────────────────────────────────────────
    # 9. Country map
    # ─────────────────────────────────────────────────────────────────────────
    print("9. Country map ...")
    country_map_raw = np.loadtxt("inputs/COUNTRY1X1.1993.mod.txt", skiprows=3).reshape(180, 360)
    country_map = xr.DataArray(country_map_raw)

    codes = pd.read_csv("inputs/COUNTRY1X1.CODE.mod2.2013.csv", header=None, names=["code", "nation"])

    assert len(CDIAC_countries) == len(codes), \
        f"CDIAC countries ({len(CDIAC_countries)}) != codes file ({len(codes)}) -- positional mapping will be wrong"

    codes_in_map = set(np.unique(country_map.values).astype(int)) - {0}
    missing_from_map = []
    for i in range(len(CDIAC_countries)):
        code = codes.iloc[i]["code"]
        has_base = code in codes_in_map
        has_sub = any(c // 100 == code // 100 and c % 100 != 0 for c in codes_in_map) if not has_base else False
        if not has_base and not has_sub:
            missing_from_map.append((CDIAC_countries.iloc[i], code))

    if missing_from_map:
        print(f"  WARNING: {len(missing_from_map)} CDIAC countries have no cells in GISS map: {missing_from_map[:10]}")
    else:
        print(f"  Country-code coverage OK: all {len(CDIAC_countries)} CDIAC countries found in GISS map")

    print("\nDone.")


if __name__ == "__main__":
    main()
