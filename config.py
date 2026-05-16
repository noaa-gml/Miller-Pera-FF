"""Central configuration for the Miller-Pera FF pipeline.

Everything that changes when the pipeline is re-run for a new production
year lives here — the year span, input-data paths, the product version,
and the output provenance string. Updating for a new year is a single-file
edit (this file), not a copy-and-hand-edit of every pipeline script.

Before this module existed the year span was duplicated under different
names in every script — ``STARTING_YEAR`` / ``yr_start`` / ``YR1`` (all
1993), ``LAST_CDIAC_YEAR`` / ``yr_cdiac``, ``LAST_EI_YEAR`` / ``yr_ei``,
``LAST_CM_YEAR`` / ``yr_final`` / ``YR3`` — a real drift risk.

Physical constants (Earth radius, carbon molar mass) are *not* here — they
never change year to year; see ``constants.py``.
"""

from __future__ import annotations

from typing import Literal

# ── Pipeline year span ───────────────────────────────────────────────────────
STARTING_YEAR     = 1993   # first year of the gridded product
LAST_CDIAC_YEAR   = 2022   # last year covered by CDIAC-at-AppState
LAST_EI_YEAR      = 2024   # last year covered by the Energy Institute review
LAST_CM_YEAR      = 2026   # CarbonMonitor NRT extension year (partial final year)
LAST_OUTPUT_MONTH = 4      # last month of LAST_CM_YEAR present in the output
# Months of LAST_CM_YEAR overwritten from CarbonMonitor year-over-year ratios.
CM_OVERWRITE_MONTHS = (2, 3, 4)
# extrapolate_edgar.py writes "FAKE" placeholder EDGAR files through this year.
EDGAR_EXTRAP_TO_YEAR = 2025

# ── Product version / output naming ──────────────────────────────────────────
PRODUCT_VERSION = "2026b"                          # frozen v2026 + NRT extension
OUTPUT_PREFIX   = f"gml_ff_co2_{PRODUCT_VERSION}"  # → gml_ff_co2_2026b_<method>.nc

# ── v2026b NRT annual-baseline methods ───────────────────────────────────────
# Two ways to set the 2025→2026 annual baseline; the pipeline runs once per
# method (see ff_country / README). The output filename is tagged with it.
CM_METHODS = ("assumed", "cm_yearly")
CMMethod = Literal["assumed", "cm_yearly"]

# ── Input data paths (update when a new data release lands) ──────────────────
# The years embedded in these filenames are the data providers' own release
# conventions, not pipeline config — replace the whole path for a new release.
CDIAC_GLOBAL_XLSX   = "inputs/CDIAC/global.1750_2022.xlsx"
CDIAC_NATIONAL_XLSX = "inputs/CDIAC/nation.1750_2022.xlsx"
EI_XLSX             = "inputs/EI-Stats-Review-ALL-data-2025.xlsx"
EDGAR_TOTALS_DIR    = "inputs/TOTALS_flx_nc_2025_GHG"
EDGAR_NMM_DIR       = "inputs/NMM_flx_nc_2025_GHG"
EDGAR_PRO_FFF_DIR   = "inputs/PRO_FFF_flx_nc_2025_GHG"
USGS_CEMENT_GLOB    = "./inputs/USGS_cement/mcs????-cement.csv"
CM_CSV_GLOB         = "inputs/carbon_monitor/carbonmonitor-global_datas_*.csv"

# ── Output provenance (written into every netCDF `source` attribute) ─────────
SOURCE_STRING = (
    "Miller-Pera FF 2026b, 1993 country bounds. "
    "CDIAC-AppState 2022; EI 2025; EDGAR 2025 GHG; "
    "USGS MCS Cement 2026; CarbonMonitor NRT through April 2026."
)
