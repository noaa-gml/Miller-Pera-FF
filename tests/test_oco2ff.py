"""Tests for the OCO-2 FF reference product: the Orion-side builder's pure
helpers (orion/make_oco2ff_monthly.py, importable locally — numpy + netCDF4
only) and the schema of whatever product files download_oco2ff.py has
fetched into inputs/OCO2FF_<version>/ (skips when absent)."""

from __future__ import annotations

import sys
from pathlib import Path

import netCDF4
import numpy as np
import pytest
import xarray as xr

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "orion"))
from make_oco2ff_monthly import (
    cell_areas_m2,
    list_year,
    month_bounds,
    month_mean,
    parse_years,
    read_day,
)

from config import OCO2FF_DIR, OCO2FF_VERSION
from download_gridfed import cell_areas_m2 as gridfed_cell_areas


def _write_day(path: Path, value: float, *, history: str = "") -> None:
    with netCDF4.Dataset(path, "w") as nc:
        nc.createDimension("time", None)
        nc.createDimension("latitude", 180)
        nc.createDimension("longitude", 360)
        v = nc.createVariable("emission", "f8", ("time", "latitude", "longitude"), fill_value=-1e34)
        v[:] = np.full((24, 180, 360), value)
        nc.history = history


def test_cell_areas_match_gridfed_builder() -> None:
    np.testing.assert_allclose(cell_areas_m2(), gridfed_cell_areas(), rtol=1e-12)


def test_month_bounds_and_midpoint() -> None:
    b0, b1 = month_bounds(2024, 2)
    assert b1 - b0 == 29.0  # leap February
    b0, b1 = month_bounds(2025, 12)
    assert b1 - b0 == 31.0


def test_list_year_groups_by_month(tmp_path: Path) -> None:
    for name in (
        "fossil_fuel_1x1_20250131.nc",
        "fossil_fuel_1x1_20250201.nc",
        "fossil_fuel_1x1_20250115.nc",
        "fossil_fuel_1x1_20240115.nc",
        "other.nc",
    ):
        (tmp_path / name).touch()
    by_month = list_year(tmp_path, 2025)
    assert list(by_month) == [1, 2]
    assert [p.name for p in by_month[1]] == [
        "fossil_fuel_1x1_20250115.nc",
        "fossil_fuel_1x1_20250131.nc",
    ]


def test_read_day_and_month_mean(tmp_path: Path) -> None:
    a, b = tmp_path / "a.nc", tmp_path / "b.nc"
    _write_day(a, 1.0)
    _write_day(b, 3.0, history="ncbo x y -o b.nc")
    day_sum, extrap, attrs = read_day(a)
    assert day_sum.shape == (180, 360)
    np.testing.assert_allclose(day_sum, 24.0)
    assert extrap is False
    mean, n_hours, any_extrap, attrs = month_mean([a, b])
    np.testing.assert_allclose(mean, 2.0)  # equal-weight hours: (24·1 + 24·3) / 48
    assert n_hours == 48
    assert any_extrap is True
    assert "ncbo" in attrs["history"]


def test_read_day_rejects_fill(tmp_path: Path) -> None:
    p = tmp_path / "f.nc"
    _write_day(p, 1.0)
    with netCDF4.Dataset(p, "a") as nc:
        nc["emission"][0, 0, 0] = -1e34
    with pytest.raises(ValueError, match="fill-value"):
        read_day(p)


def test_parse_years() -> None:
    assert parse_years("2000-2002,2026") == [2000, 2001, 2002, 2026]


# ── whatever product files are present locally ───────────────────────────────

_PRODUCT_FILES = sorted(Path(OCO2FF_DIR).glob(f"oco2ff_{OCO2FF_VERSION}_1x1_*.nc"))


@pytest.mark.parametrize("path", _PRODUCT_FILES or [None], ids=lambda p: p.name if p else "none")
def test_oco2ff_product_schema(path: Path | None) -> None:
    if path is None:
        pytest.skip(f"no OCO-2 FF product files under {OCO2FF_DIR} (run download_oco2ff.py)")
    with xr.open_dataset(path) as ds:
        for v in ("emission", "extrapolated", "n_source_hours", "cell_area", "month_seconds"):
            assert v in ds.variables, f"{path.name}: missing {v}"
        assert ds.sizes["lat"] == 180 and ds.sizes["lon"] == 360
        assert 1 <= ds.sizes["time"] <= 12
        assert ds.attrs["oco2ff_version"] == OCO2FF_VERSION
        # Every month averaged the full complement of hours.
        expected = ds["month_seconds"].values / 3600.0
        np.testing.assert_array_equal(ds["n_source_hours"].values, expected)
        # Extrapolated months only ever occur in the final (partial) year.
        if int(ds["extrapolated"].values.sum()):
            assert ds.sizes["time"] < 12
        # Flux × area × seconds reproduces the recorded annual PgC.
        secs = ds["month_seconds"].values
        pgc = (
            float((ds["emission"] * ds["cell_area"]).sum(("lat", "lon")).values @ secs) * 12.011e-15
        )
        assert pgc == pytest.approx(ds["emission"].attrs["annual_global_total_PgC"], rel=1e-4)
