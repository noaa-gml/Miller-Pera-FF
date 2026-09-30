"""Tests for download_gridfed.py — the GridFED 1° reference-product builder.

The pure grid helpers are exercised on synthetic arrays (runs everywhere);
the schema test checks whatever `inputs/GridFED_<version>/` files are present
locally and skips otherwise (they are gitignored bulk data).
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import GRIDFED_DIR, GRIDFED_VERSION
from constants import CO2_MOLAR_MASS, EARTH_RADIUS
from download_gridfed import SECTORS, cell_areas_m2, coarsen_sum, mass_to_flux, parse_years
from timeutils import seconds_in_month


def test_cell_areas_sum_to_sphere_and_are_symmetric() -> None:
    a = cell_areas_m2()
    assert a.shape == (180, 360)
    assert a.sum() == pytest.approx(4 * np.pi * (EARTH_RADIUS * 1e3) ** 2, rel=1e-12)
    np.testing.assert_allclose(a, a[::-1, :])  # N–S symmetric
    assert a[90, 0] > a[0, 0]  # equator cell larger than polar cell


def test_coarsen_sum_conserves_mass_and_places_blocks() -> None:
    rng = np.random.default_rng(0)
    fine = rng.uniform(size=(2, 1800, 3600)).astype(np.float32)
    coarse = coarsen_sum(fine)
    assert coarse.shape == (2, 180, 360)
    assert coarse.sum() == pytest.approx(fine.sum(dtype=np.float64), rel=1e-9)
    # Block (i, j) is exactly the sum of fine rows 10i..10i+9, cols 10j..10j+9.
    assert coarse[1, 17, 250] == pytest.approx(
        fine[1, 170:180, 2500:2510].sum(dtype=np.float64), rel=1e-9
    )


def test_coarsen_sum_rejects_wrong_grid() -> None:
    with pytest.raises(ValueError, match="expected a"):
        coarsen_sum(np.zeros((12, 180, 360)))


def test_mass_to_flux_round_trip() -> None:
    area = cell_areas_m2()
    mass = np.full((12, 180, 360), 1e6)  # kg CO2 per cell per month
    flux = mass_to_flux(mass, 2023, area)
    secs = np.array([seconds_in_month(2023, m) for m in range(1, 13)])
    back = flux * area[np.newaxis] * secs[:, np.newaxis, np.newaxis] * CO2_MOLAR_MASS / 1000.0
    np.testing.assert_allclose(back, mass, rtol=1e-12)
    # February (shortest) carries the highest flux for equal monthly mass.
    assert flux[1].mean() > flux[0].mean()


def test_parse_years() -> None:
    assert parse_years("2020-2024") == [2020, 2021, 2022, 2023, 2024]
    assert parse_years("2019,2021,2019") == [2019, 2021]
    assert parse_years("1993-1994,2024") == [1993, 1994, 2024]


# ── whatever product files are present locally ───────────────────────────────

_PRODUCT_FILES = sorted(Path(GRIDFED_DIR).glob(f"GCP-GridFED{GRIDFED_VERSION}_1x1_*.nc"))


@pytest.mark.parametrize("path", _PRODUCT_FILES or [None], ids=lambda p: p.name if p else "none")
def test_gridfed_product_schema(path: Path | None) -> None:
    if path is None:
        pytest.skip(f"no GridFED product files under {GRIDFED_DIR} (run download_gridfed.py)")
    with xr.open_dataset(path) as ds:
        assert set(SECTORS) <= set(ds.data_vars)
        assert ds.sizes == {"time": 12, "lat": 180, "lon": 360, "bnds": 2}
        assert ds["TOTAL"].attrs["units"] == "mol m-2 s-1"
        for key in ("source_url", "source_zip_md5", "gridfed_version", "processing"):
            assert ds.attrs.get(key), f"{path.name}: missing global attribute {key}"
        # TOTAL is the published sum of the seven sectors (incl. the carbonation sink).
        parts = np.sum([ds[s].values for s in SECTORS if s != "TOTAL"], axis=0)
        np.testing.assert_allclose(parts, ds["TOTAL"].values, rtol=1e-5, atol=1e-13)
        # Flux × area × seconds must reproduce the recorded annual PgC.
        secs = ds["month_seconds"].values
        pgc = float((ds["TOTAL"] * ds["cell_area"]).sum(("lat", "lon")).values @ secs) * 12.011e-15
        assert pgc == pytest.approx(ds["TOTAL"].attrs["annual_global_total_PgC"], rel=1e-4)
