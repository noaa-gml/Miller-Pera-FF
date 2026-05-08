"""CDIAC ↔ GISS grid coverage tests.

Catches the silent-data-loss case where a CDIAC country has no matching
grid cells in the GISS country map (its emissions would be silently
dropped). Already done in verify_2026.ipynb check 1e, but here it runs
fast as a unit test rather than during a manual notebook session.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO = Path(__file__).parent.parent
INPUTS = REPO / "inputs"
GISS_GRID = INPUTS / "COUNTRY1X1.1993.mod.txt"
GISS_CODES = INPUTS / "COUNTRY1X1.CODE.mod2.2013.csv"
PROCESSED = REPO / "processed_inputs" / "CDIAC_countries.csv"


def _skip_if_missing(p: Path) -> None:
    if not p.exists():
        pytest.skip(f"{p} not present locally")


@pytest.fixture(scope="module")
def giss_map() -> np.ndarray:
    _skip_if_missing(GISS_GRID)
    return np.loadtxt(GISS_GRID, skiprows=3, dtype=int).reshape(180, 360)


@pytest.fixture(scope="module")
def codes_table() -> pd.DataFrame:
    _skip_if_missing(GISS_CODES)
    return pd.read_csv(GISS_CODES, header=None, names=["code", "name"])


@pytest.fixture(scope="module")
def cdiac_countries() -> list[str]:
    _skip_if_missing(PROCESSED)
    return pd.read_csv(PROCESSED, header=None, names=["Nation"])["Nation"].tolist()


def test_cdiac_country_count_matches_codes(cdiac_countries, codes_table):
    """ff_country_2026 relies on POSITIONAL alignment between CDIAC list and code table."""
    assert len(cdiac_countries) == len(codes_table), (
        f"CDIAC list ({len(cdiac_countries)}) != code table ({len(codes_table)}) — "
        "ff_country's positional matching will be wrong"
    )


def test_every_cdiac_country_has_grid_cells(cdiac_countries, codes_table, giss_map):
    """Every CDIAC country code must have ≥1 matching cell in the GISS map.

    Cells are matched at 3-digit prefix unless the code is a known subdivision
    (CSSR, USSR, St Kitts & Nevis, Yemen).
    """
    SUBDIV_PREFIXES = {41, 137, 172, 179}
    giss_coarse = (giss_map // 100) * 100

    missing: list[str] = []
    for i, name in enumerate(cdiac_countries):
        code = int(codes_table.iloc[i]["code"])
        is_subdiv = (code // 100) in SUBDIV_PREFIXES
        match_map = giss_map if is_subdiv else giss_coarse
        n_cells = int(np.sum(match_map == code))
        if n_cells == 0:
            # Acceptable if it's a "subdivision" parent (e.g. code 17200 with
            # only 17201/17202 in the map) — check the other branch too.
            n_cells_other = int(np.sum((giss_coarse if is_subdiv else giss_map) == code))
            if n_cells_other == 0:
                # Last resort: check if any cell shares the 3-digit prefix
                prefix = (code // 100) * 100
                n_prefix = int(np.sum(giss_coarse == prefix))
                if n_prefix == 0:
                    missing.append(f"  [{i}] {name!r} (code {code}) — 0 cells")

    assert not missing, "Countries with no grid cells:\n" + "\n".join(missing)


def test_orphan_giss_codes_are_known_zero_emission_regions(giss_map, codes_table):
    """Codes in the GISS map that don't appear in the code table should only
    cover regions with negligible fossil-fuel emissions (Antarctica, tiny
    territories). The pipeline silently drops their emissions, so we want
    the orphan total to be small relative to the global land area.
    """
    map_codes = set(np.unique(giss_map).astype(int)) - {0}
    table_codes = set(codes_table["code"].astype(int))
    table_prefixes = {(c // 100) * 100 for c in table_codes}

    orphan_cell_counts: dict[int, int] = {}
    for code in map_codes:
        if code in table_codes:
            continue
        if (code // 100) * 100 in table_prefixes:
            continue
        orphan_cell_counts[code] = int(np.sum(giss_map == code))

    # Total non-ocean cells = where map != 0
    n_land = int(np.sum(giss_map != 0))
    n_orphan = sum(orphan_cell_counts.values())
    orphan_frac = n_orphan / n_land if n_land > 0 else 0

    # Known: Antarctica (~6600 cells, ~22% of land, but zero FF emissions);
    # plus a handful of small territories (single-digit cell counts).
    # If non-Antarctic orphans exceed 100 cells, we're losing real countries.
    big_orphans = {c: n for c, n in orphan_cell_counts.items() if n > 1000}
    small_orphans = {c: n for c, n in orphan_cell_counts.items() if n <= 1000}
    n_small = sum(small_orphans.values())

    # Big orphan(s) must be Antarctica-like (cell count > 5000 → continental).
    for code, n in big_orphans.items():
        assert n > 5000, (
            f"Code {code} has {n} cells — too small to be Antarctica, too "
            "big to ignore. Likely a real country dropping out of the code table."
        )

    # Small orphans together must be < 1% of land area.
    small_frac = n_small / n_land if n_land > 0 else 0
    assert small_frac < 0.01, (
        f"Small-orphan codes account for {small_frac:.2%} of land cells "
        f"(threshold: 1%); details: {small_orphans}"
    )
    # Antarctica alone is ~30% of GISS land; tolerate up to 35% in total.
    assert orphan_frac < 0.35, (
        f"Total orphan fraction {orphan_frac:.1%} exceeds 35% of land — "
        "more than just Antarctica + tiny territories is being dropped"
    )
