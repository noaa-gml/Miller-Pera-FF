"""Tests for extrapolate_edgar.py helpers."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import xarray as xr

sys.path.insert(0, str(Path(__file__).parent.parent))
from extrapolate_edgar import _empirical_growth_rate, _output_path


def test_output_path_real_to_fake():
    """Replaces year and inserts FAKE before _flx.nc."""
    out = _output_path(
        edgar_dir="inputs/NMM_flx_nc_2025_GHG",
        base_name="EDGAR_2025_GHG_CO2_2024_NMM_flx.nc",
        year=2025,
    )
    assert out.endswith("EDGAR_2025_GHG_CO2_2025_NMM_FAKE_flx.nc")


def test_output_path_idempotent_on_fake_input():
    """If the base file is already FAKE, don't double-insert FAKE."""
    out = _output_path(
        edgar_dir="inputs/NMM_flx_nc_2025_GHG",
        base_name="EDGAR_2025_GHG_CO2_2024_NMM_FAKE_flx.nc",
        year=2025,
    )
    # Year is updated, but FAKE is not duplicated (the regex has a negative lookbehind).
    assert "FAKE_FAKE" not in out
    assert "_2025_NMM_FAKE_flx.nc" in out


def test_output_path_works_for_all_sectors():
    for tag in ("TOTALS", "NMM", "PRO_FFF"):
        out = _output_path(
            edgar_dir=f"inputs/{tag}_flx_nc_2025_GHG",
            base_name=f"EDGAR_2025_GHG_CO2_2024_{tag}_flx.nc",
            year=2026,
        )
        assert f"_2026_{tag}_FAKE_flx.nc" in out


def test_empirical_growth_rate_geometric_mean(tmp_path: Path):
    """3 synthetic EDGAR files with totals 100, 110, 121 → geometric ratio 1.10 → +10%."""
    edgar_dir = tmp_path / "TOTALS_flx_nc_2025_GHG"
    edgar_dir.mkdir()
    totals = [100.0, 110.0, 121.0]   # consecutive ratios = 1.10, 1.10
    for i, total in enumerate(totals):
        year = 2020 + i
        fname = edgar_dir / f"EDGAR_2025_GHG_CO2_{year}_TOTALS_flx.nc"
        # Single 1×1 grid cell; 'fluxes' variable summing to `total`.
        ds = xr.Dataset({"fluxes": (["lat", "lon"], np.array([[total]]))})
        ds.to_netcdf(fname)

    rate = _empirical_growth_rate(
        edgar_dir=str(edgar_dir),
        sector_tag="TOTALS",
        var_name="fluxes",
        n_years=3,
    )
    assert rate is not None
    assert abs(rate - 0.10) < 1e-9


def test_empirical_growth_rate_returns_none_with_one_file(tmp_path: Path):
    edgar_dir = tmp_path / "TOTALS_flx_nc_2025_GHG"
    edgar_dir.mkdir()
    fname = edgar_dir / "EDGAR_2025_GHG_CO2_2024_TOTALS_flx.nc"
    xr.Dataset({"fluxes": (["lat", "lon"], np.array([[100.0]]))}).to_netcdf(fname)

    rate = _empirical_growth_rate(
        edgar_dir=str(edgar_dir), sector_tag="TOTALS", var_name="fluxes", n_years=3,
    )
    assert rate is None


def test_empirical_growth_rate_excludes_fake_files(tmp_path: Path):
    """FAKE files in the directory should be ignored when computing the rate."""
    edgar_dir = tmp_path / "TOTALS_flx_nc_2025_GHG"
    edgar_dir.mkdir()
    # Two real files (no FAKE in name)
    for year, total in [(2023, 100.0), (2024, 105.0)]:
        fname = edgar_dir / f"EDGAR_2025_GHG_CO2_{year}_TOTALS_flx.nc"
        xr.Dataset({"fluxes": (["lat", "lon"], np.array([[total]]))}).to_netcdf(fname)
    # One FAKE file with absurd value — should be ignored.
    fake = edgar_dir / "EDGAR_2025_GHG_CO2_2025_TOTALS_FAKE_flx.nc"
    xr.Dataset({"fluxes": (["lat", "lon"], np.array([[10000.0]]))}).to_netcdf(fake)

    rate = _empirical_growth_rate(
        edgar_dir=str(edgar_dir), sector_tag="TOTALS", var_name="fluxes", n_years=3,
    )
    assert rate is not None
    # 105/100 = 1.05 → +5%
    assert abs(rate - 0.05) < 1e-9
