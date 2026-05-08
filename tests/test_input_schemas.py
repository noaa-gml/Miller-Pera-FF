"""Schema-snapshot tests for input files.

Catches upstream format drift early — when CDIAC, EI, USGS, EDGAR, or
CarbonMonitor change a column header, sheet name, or expected sectors,
these tests fail loudly *before* the pipeline silently produces wrong
output.

Tests skip gracefully when the input file isn't present locally (the
xlsx files are gitignored and not always available in CI), so this
suite is safe to run anywhere.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).parent.parent
INPUTS = REPO / "inputs"


def _skip_if_missing(p: Path) -> None:
    if not p.exists():
        pytest.skip(f"{p} not present locally")


# ── CDIAC ─────────────────────────────────────────────────────────────────────

CDIAC_GLOBAL = INPUTS / "CDIAC" / "global.1750_2022.xlsx"
CDIAC_NATION = INPUTS / "CDIAC" / "nation.1750_2022.xlsx"
EXPECTED_CDIAC_GLOBAL_COLS = {
    "Year",
    "Emissions from fossil fuels and cement production (million metric tons of C)",
    "Emissions from solid fuel consumption",
    "Emissions from liquid fuel consumption",
    "Emissions from gas fuel consumption",
    "Emissions from cement production",
    "Emissions from gas flaring",
    "Emissions per capita (metric tons of carbon)",
}


def test_cdiac_global_xlsx_schema():
    _skip_if_missing(CDIAC_GLOBAL)
    df = pd.read_excel(CDIAC_GLOBAL, sheet_name="Sheet1")
    missing = EXPECTED_CDIAC_GLOBAL_COLS - set(df.columns)
    assert not missing, f"CDIAC global missing columns: {missing}"
    # Should cover at least 1993..2022 (the years the pipeline depends on)
    years = set(df["Year"].dropna().astype(int))
    for needed in (1993, 2000, 2010, 2022):
        assert needed in years, f"CDIAC global missing year {needed}"


def test_cdiac_nation_xlsx_schema():
    _skip_if_missing(CDIAC_NATION)
    df = pd.read_excel(CDIAC_NATION, sheet_name="Sheet1")
    expected = {"Nation", "Year"}
    assert expected <= set(df.columns), \
        f"CDIAC nation missing required columns; have {list(df.columns)[:10]}…"


# ── Energy Institute ─────────────────────────────────────────────────────────

EI_XLSX = INPUTS / "EI-Stats-Review-ALL-data-2025.xlsx"
EXPECTED_EI_SHEETS = {
    "Primary Energy Cons (old meth)",   # oil
    "Gas Consumption - EJ",
    "Coal Consumption - EJ",
    "CO2 from Flaring",
}


def test_ei_xlsx_has_all_required_sheets():
    _skip_if_missing(EI_XLSX)
    xlsx = pd.ExcelFile(EI_XLSX)
    missing = EXPECTED_EI_SHEETS - set(xlsx.sheet_names)
    assert not missing, f"EI xlsx missing sheets: {missing}"


def test_ei_total_world_row_present():
    _skip_if_missing(EI_XLSX)
    df = pd.read_excel(EI_XLSX, sheet_name="Primary Energy Cons (old meth)",
                       header=2, index_col=0, skipfooter=10)
    assert "Total World" in df.index, \
        "'Total World' row missing from EI primary energy sheet"


# ── USGS Cement ──────────────────────────────────────────────────────────────

def test_usgs_cement_2025_schema():
    """The 2026-published mcs file (covers 2024-2025 data)."""
    p = INPUTS / "USGS_cement" / "mcs2026-cement.csv"
    _skip_if_missing(p)
    df = pd.read_csv(p)
    expected = {"Nation", "Cement 2024 (Gg)", "Cement 2025 (Gg)"}
    missing = expected - set(df.columns)
    assert not missing, f"USGS mcs2026 missing columns: {missing}"
    # Sentinel rows the ingest depends on (string-matched in ingest_2026.py)
    nations = set(df["Nation"].dropna())
    assert any("World total" in n for n in nations), \
        "Expected 'World total (rounded)' or similar sentinel row"
    assert any("Other countries" in n for n in nations), \
        "Expected 'Other countries (rounded)' fallback row"


# ── GISS country grid ─────────────────────────────────────────────────────────

def test_giss_country_grid_shape():
    p = INPUTS / "COUNTRY1X1.1993.mod.txt"
    _skip_if_missing(p)
    import numpy as np
    arr = np.loadtxt(p, skiprows=3, dtype=int)
    assert arr.size == 180 * 360, \
        f"GISS grid has {arr.size} cells, expected {180 * 360}"
    # Reshapes cleanly to (180, 360)
    assert arr.reshape(180, 360).shape == (180, 360)


def test_giss_country_codes_csv():
    p = INPUTS / "COUNTRY1X1.CODE.mod2.2013.csv"
    _skip_if_missing(p)
    df = pd.read_csv(p, header=None, names=["code", "name"])
    assert (df["code"] >= 0).all(), "negative country codes in GISS code table"
    assert df["code"].nunique() == len(df), "duplicate codes in GISS code table"


# ── CarbonMonitor (cached CSV) ───────────────────────────────────────────────

def test_carbon_monitor_csv_schema():
    """Validate the most recent downloaded CM CSV against download_carbon_monitor's expectations."""
    cm_files = sorted((INPUTS / "carbon_monitor").glob("carbonmonitor-global_datas_*.csv"))
    if not cm_files:
        pytest.skip("no CarbonMonitor CSV downloaded yet")

    import sys
    sys.path.insert(0, str(REPO))
    from download_carbon_monitor import EXPECTED_HEADER, EXPECTED_SECTORS

    csv_path = cm_files[-1]
    header = csv_path.read_text(encoding="utf-8").splitlines()[0]
    assert header.strip() == EXPECTED_HEADER, \
        f"CM CSV header changed: {header!r}"

    df = pd.read_csv(csv_path)
    df = df.loc[:, ~df.columns.str.match(r"Unnamed")]
    assert {"country", "date", "sector", "value"} <= set(df.columns)

    sectors = set(df["sector"].unique())
    assert sectors == EXPECTED_SECTORS, \
        f"CM sectors changed: extra={sectors - EXPECTED_SECTORS}, " \
        f"missing={EXPECTED_SECTORS - sectors}"

    # EU27 / ROW / WORLD aggregates must be present
    assert {"EU27", "ROW", "WORLD"} <= set(df["country"].unique())


# ── EDGAR ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("sector", ["TOTALS", "NMM", "PRO_FFF"])
def test_edgar_sector_dirs_exist(sector: str):
    p = INPUTS / f"{sector}_flx_nc_2025_GHG"
    _skip_if_missing(p)
    files = sorted(p.glob("*.nc"))
    assert files, f"EDGAR {sector} dir empty"
    # Should have at least one real file
    real = [f for f in files if "FAKE" not in f.name]
    assert real, f"EDGAR {sector} has only FAKE files"
