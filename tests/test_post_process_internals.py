"""Unit tests for post_process.py internals — unit conversion + cell areas.

These import post_process, which pulls the conda-only geospatial stack, so
they sit behind importorskip: they run in the conda-based CI job and skip in
the lightweight one.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from constants import C_MOLAR_MASS, EARTH_RADIUS
from timeutils import seconds_in_year


def _post_process():
    """Import post_process behind the geospatial-stack guard."""
    pytest.importorskip("xesmf")
    pytest.importorskip("xcdat")
    import post_process
    return post_process


def test_cell_areas_cover_the_sphere():
    """xESMF 1x1 cell areas: shape (180, 360), positive, summing to 4 pi R^2."""
    pp = _post_process()
    areas = pp.cell_areas_m2()
    assert areas.shape == (180, 360)
    assert (areas > 0).all()
    expected = 4 * np.pi * (EARTH_RADIUS * 1e3) ** 2  # m^2
    assert abs(areas.sum() - expected) / expected < 1e-3


def test_annual_pgc_zero_field():
    pp = _post_process()
    areas = np.ones((180, 360))
    assert pp._annual_pgc(np.zeros((12, 180, 360)), areas, 2023) == 0.0


def test_annual_pgc_recovers_a_known_total():
    """A uniform mol/m2/s field that integrates to P PgC must read back as P."""
    pp = _post_process()
    areas = np.ones((180, 360))                       # 1 m^2 per cell
    yr, target = 2023, 8.0
    total_mol = target / 1e-15 / C_MOLAR_MASS
    rate = total_mol / (areas.sum() * seconds_in_year(yr))   # mol m-2 s-1
    data = np.full((12, 180, 360), rate)
    assert abs(pp._annual_pgc(data, areas, yr) - target) / target < 1e-12


def test_annual_pgc_is_linear():
    pp = _post_process()
    areas = np.ones((180, 360))
    data = np.full((12, 180, 360), 3e-9)
    base = pp._annual_pgc(data, areas, 2023)
    assert base > 0
    assert abs(pp._annual_pgc(2 * data, areas, 2023) - 2 * base) / base < 1e-12


def test_annual_pgc_partial_zero_field():
    pp = _post_process()
    areas = np.ones((180, 360))
    assert pp._annual_pgc_partial(np.zeros((4, 180, 360)), areas, 2026, 4) == 0.0


def test_annual_pgc_partial_full_year_matches_annual():
    """_annual_pgc_partial over all 12 months equals _annual_pgc."""
    pp = _post_process()
    areas = np.ones((180, 360))
    data = np.full((12, 180, 360), 2e-9)
    full = pp._annual_pgc(data, areas, 2024)
    partial = pp._annual_pgc_partial(data, areas, 2024, 12)
    assert abs(full - partial) / full < 1e-12
