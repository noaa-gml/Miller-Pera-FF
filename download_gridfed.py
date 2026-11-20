#!/usr/bin/env python3
"""download_gridfed.py — Fetch GCB GridFED and build the 1° monthly reference product.

GridFED (Jones et al., 2021) is the Global Carbon Budget's gridded fossil
CO₂ emissions dataset: GCB national totals distributed with EDGAR v4.3.2
spatial patterns, 0.1° × 0.1° monthly, one ~8.7 GB netCDF (zipped to
~1.3 GB) per year. It is a *reference* product — used only by
``compare_products.py`` — and never feeds the pipeline.

Per year this script:

1. downloads ``GCP-GridFED<version>_<year>.zip`` from UEA's mirror of the
   Zenodo record (``config.GRIDFED_URL``), recording its MD5;
2. unzips it and reads every sector of the ``CO2`` group
   (OIL, GAS, COAL, BUNKER_AVIATION, BUNKER_SHIPPING, CEMENT,
   CEMENT_CARBONATION, TOTAL — kg CO₂ cell⁻¹ month⁻¹);
3. sums each 10 × 10 block of 0.1° cells into the 1° cell (exactly mass
   conserving), converts to a flux in mol C m⁻² s⁻¹ with the pipeline's
   spherical-Earth cell areas, and writes
   ``inputs/GridFED_<version>/GCP-GridFED<version>_1x1_<year>.nc`` with
   the source URL, MD5, and original metadata as global attributes;
4. deletes the raw zip + netCDF (≈ 10 GB per year) unless ``--keep-raw``.

Note ``TOTAL`` as published *includes* the (negative) cement-carbonation
sink; the gross emissions comparable to Miller-Pera / CDIAC are
``TOTAL − CEMENT_CARBONATION``.

Usage:
    python download_gridfed.py                    # all years 1993..GRIDFED_LAST_YEAR
    python download_gridfed.py --years 2020-2024  # a subset
    python download_gridfed.py --force            # rebuild existing years
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import sys
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import netCDF4
import numpy as np
import pandas as pd
import xarray as xr

from config import (
    GRIDFED_DIR,
    GRIDFED_DOI,
    GRIDFED_FILE_TEMPLATE,
    GRIDFED_LAST_YEAR,
    GRIDFED_URL,
    GRIDFED_VERSION,
    STARTING_YEAR,
)
from constants import C_MOLAR_MASS, CO2_MOLAR_MASS, EARTH_RADIUS
from provenance import code_commit
from timeutils import seconds_in_month

SECTORS = (
    "OIL",
    "GAS",
    "COAL",
    "BUNKER_AVIATION",
    "BUNKER_SHIPPING",
    "CEMENT",
    "CEMENT_CARBONATION",
    "TOTAL",
)
FINE_RES = 0.1
BLOCK = 10  # 0.1° cells per 1° cell
N_LAT, N_LON = 180, 360
RAW_SUBDIR = "raw"
CHECKSUM_FILE = "checksums.md5"
CHUNK = 1024 * 1024
PROGRESS_EVERY = 200 * CHUNK


# ═══════════════════════════════════════════════════════════════════════════════
# Grid helpers (pure numpy — unit-tested)
# ═══════════════════════════════════════════════════════════════════════════════


def cell_areas_m2(earth_radius_km: float = EARTH_RADIUS) -> np.ndarray:
    """Exact spherical 1° × 1° cell areas (m²), shape (180, 360), lat ascending.

    ``A = R² · Δλ · (sin φ₂ − sin φ₁)``. Agrees with ``post_process.cell_areas_m2``
    (xESMF, great-circle polygons) to 5 × 10⁻⁵ — same Earth radius.
    """
    lat_edges = np.deg2rad(np.arange(-90.0, 91.0, 1.0))
    band = (earth_radius_km * 1e3) ** 2 * np.deg2rad(1.0) * np.diff(np.sin(lat_edges))
    return np.repeat(band[:, np.newaxis], N_LON, axis=1)


def coarsen_sum(fine: np.ndarray) -> np.ndarray:
    """Sum each 10 × 10 block of a ``(…, 1800, 3600)`` array into ``(…, 180, 360)``.

    The 0.1° grid is cell-centred at −89.95 … 89.95 / −179.95 … 179.95, so
    block *k* of ten cells is exactly the 1° cell centred at −89.5 + k.
    Accumulates in float64 so the sum is order-independent to rounding.
    """
    *lead, ny, nx = fine.shape
    if (ny, nx) != (N_LAT * BLOCK, N_LON * BLOCK):
        msg = f"expected a (…, {N_LAT * BLOCK}, {N_LON * BLOCK}) array, got {fine.shape}"
        raise ValueError(msg)
    blocked = fine.astype(np.float64).reshape(*lead, N_LAT, BLOCK, N_LON, BLOCK)
    return blocked.sum(axis=(-3, -1))


def mass_to_flux(mass_kg_co2: np.ndarray, year: int, area_m2: np.ndarray) -> np.ndarray:
    """Convert kg CO₂ cell⁻¹ month⁻¹ ``(12, lat, lon)`` to mol C m⁻² s⁻¹."""
    secs = np.array([seconds_in_month(year, m) for m in range(1, 13)])
    mol = mass_kg_co2 * (1000.0 / CO2_MOLAR_MASS)
    return mol / area_m2[np.newaxis, :, :] / secs[:, np.newaxis, np.newaxis]


# ═══════════════════════════════════════════════════════════════════════════════
# Download / unpack
# ═══════════════════════════════════════════════════════════════════════════════


def download(url: str, out_path: Path) -> str:
    """Stream *url* to *out_path* (atomic via .part + rename); return its MD5."""
    tmp = out_path.with_suffix(out_path.suffix + ".part")
    print(f"  downloading {url}")
    md5 = hashlib.md5()  # noqa: S324 — integrity record, not security
    done = 0
    with urllib.request.urlopen(url, timeout=300) as resp, tmp.open("wb") as fh:  # noqa: S310
        total = int(resp.headers.get("Content-Length") or 0)
        while True:
            chunk = resp.read(CHUNK)
            if not chunk:
                break
            fh.write(chunk)
            md5.update(chunk)
            done += len(chunk)
            if done % PROGRESS_EVERY == 0:
                pct = f" ({100 * done / total:.0f}%)" if total else ""
                print(f"    {done / 1e6:,.0f} MB{pct}", flush=True)
    tmp.rename(out_path)
    print(f"  saved {out_path.name}: {done:,} bytes, md5 {md5.hexdigest()}")
    return md5.hexdigest()


def md5_of(path: Path) -> str:
    md5 = hashlib.md5()  # noqa: S324
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            md5.update(chunk)
    return md5.hexdigest()


def unzip_single(zip_path: Path, dest_dir: Path) -> Path:
    """Extract the one netCDF inside *zip_path* into *dest_dir*; return its path."""
    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist() if m.endswith(".nc")]
        if len(members) != 1:
            msg = f"{zip_path.name}: expected exactly one .nc member, found {members}"
            raise ValueError(msg)
        print(f"  extracting {members[0]} ({zf.getinfo(members[0]).file_size / 1e9:.1f} GB)")
        zf.extract(members[0], dest_dir)
    return dest_dir / members[0]


# ═══════════════════════════════════════════════════════════════════════════════
# Build the 1° product for one year
# ═══════════════════════════════════════════════════════════════════════════════


def build_year(raw_nc: Path, year: int, out_path: Path, source: dict[str, str]) -> None:
    """Coarsen every CO₂ sector of *raw_nc* to 1° flux and write *out_path*."""
    area = cell_areas_m2()
    times = pd.to_datetime([f"{year}-{m:02d}-15" for m in range(1, 13)])
    starts = pd.date_range(f"{year}-01-01", periods=12, freq="MS")
    bounds = np.stack([starts.values, (starts + pd.offsets.MonthBegin(1)).values], axis=1)

    ds = xr.Dataset(
        coords={
            "time": times.values,
            "lat": np.arange(-89.5, 90.0, 1.0),
            "lon": np.arange(-179.5, 180.0, 1.0),
        },
    )
    ds["time"].attrs["bounds"] = "time_bnds"
    ds["time_bnds"] = (("time", "bnds"), bounds)
    ds["lat"].attrs.update(units="degrees_north", long_name="latitude (cell centre)")
    ds["lon"].attrs.update(units="degrees_east", long_name="longitude (cell centre)")
    ds["cell_area"] = (("lat", "lon"), area)
    ds["cell_area"].attrs.update(
        units="m2", long_name=f"spherical cell area, R = {EARTH_RADIUS} km"
    )
    ds["month_seconds"] = ("time", np.array([seconds_in_month(year, m) for m in range(1, 13)]))
    ds["month_seconds"].attrs.update(units="s", long_name="seconds in month")

    with netCDF4.Dataset(raw_nc) as nc:
        fine_lat = nc["lat"][:]
        fine_lon = nc["lon"][:]
        if not (
            np.allclose(fine_lat[[0, -1]], [-89.95, 89.95], atol=1e-3)
            and np.allclose(fine_lon[[0, -1]], [-179.95, 179.95], atol=1e-3)
        ):
            msg = f"{raw_nc.name}: unexpected 0.1° grid {fine_lat[[0, -1]]} / {fine_lon[[0, -1]]}"
            raise ValueError(msg)
        if nc["time"].shape != (12,):
            msg = f"{raw_nc.name}: expected 12 months, got {nc['time'].shape}"
            raise ValueError(msg)
        group = nc["CO2"]
        src_attrs = {k: str(nc.getncattr(k)) for k in nc.ncattrs()}

        for sector in SECTORS:
            fine = np.asarray(group[sector][:], dtype=np.float32)  # (12, 1800, 3600)
            fine = np.nan_to_num(fine, nan=0.0)
            coarse_kg = coarsen_sum(fine)  # (12, 180, 360), float64
            # Mass conservation: the block sum must reproduce the fine total.
            fine_total = float(fine.sum(dtype=np.float64))
            coarse_total = float(coarse_kg.sum())
            if not np.isclose(fine_total, coarse_total, rtol=1e-6, atol=0.0):
                msg = f"{sector} {year}: mass not conserved ({fine_total} vs {coarse_total})"
                raise RuntimeError(msg)
            flux = mass_to_flux(coarse_kg, year, area)
            pgc = coarse_total * (C_MOLAR_MASS / CO2_MOLAR_MASS) * 1e-12
            ds[sector] = (("time", "lat", "lon"), flux.astype(np.float32))
            ds[sector].attrs.update(
                units="mol m-2 s-1",
                long_name=f"GridFED {GRIDFED_VERSION} {sector} fossil CO2 flux (mol C m-2 s-1)",
                source_units="kg CO2 cell-1 month-1",
                annual_global_total_PgC=round(pgc, 6),
            )
            print(f"    {sector:<19s} {pgc:8.4f} PgC", flush=True)

    ds.attrs = {
        "title": f"GCB GridFED {GRIDFED_VERSION} fossil CO2 emissions, coarsened to 1° x 1°",
        "gridfed_version": GRIDFED_VERSION,
        "source_doi": GRIDFED_DOI,
        "source_url": source["url"],
        "source_zip_md5": source["md5"],
        "source_file": raw_nc.name,
        "source_description": src_attrs.get("description", ""),
        "source_history": src_attrs.get("history", ""),
        "source_source": src_attrs.get("source", ""),
        "processing": (
            "Each 1° cell is the exact sum of its 10 x 10 block of 0.1° cells "
            "(kg CO2 cell-1 month-1), converted to mol C m-2 s-1 with "
            f"CO2 molar mass {CO2_MOLAR_MASS} g/mol, spherical cell areas "
            f"(R = {EARTH_RADIUS} km) and calendar month lengths. TOTAL is as "
            "published and includes the negative CEMENT_CARBONATION sink; "
            "gross emissions = TOTAL - CEMENT_CARBONATION."
        ),
        "reference": "Jones et al. (2021), Sci. Data 8, 2, doi:10.1038/s41597-020-00779-6",
        "created": datetime.now(UTC).isoformat(),
        "created_by": f"download_gridfed.py, Miller-Pera FF pipeline commit {code_commit()}",
        "Conventions": "CF-1.8",
    }
    encoding: dict[str, dict[str, Any]] = {
        "time": {"units": "days since 1900-01-01", "dtype": "float64"},
        "time_bnds": {"units": "days since 1900-01-01", "dtype": "float64"},
    }
    encoding.update({s: {"zlib": True, "complevel": 4} for s in SECTORS})
    tmp = out_path.with_suffix(".nc.tmp")
    ds.to_netcdf(tmp, encoding=encoding)
    tmp.rename(out_path)
    print(f"  wrote {out_path} ({out_path.stat().st_size / 1e6:.1f} MB)")


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════


def parse_years(spec: str) -> list[int]:
    """'2020-2024' or '2020,2022' → sorted list of ints."""
    years: set[int] = set()
    for part in spec.split(","):
        if "-" in part:
            a, b = part.split("-", 1)
            years.update(range(int(a), int(b) + 1))
        else:
            years.add(int(part))
    return sorted(years)


def record_checksum(path: Path, name: str, md5: str) -> None:
    """Upsert ``<md5>  <name>`` into the md5sum-format checksum file (tracked in git)."""
    entries: dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                digest, fname = line.split(None, 1)
                entries[fname.strip()] = digest
    entries[name] = md5
    path.write_text("".join(f"{d}  {n}\n" for n, d in sorted(entries.items())), encoding="utf-8")


def process_year(year: int, out_dir: Path, *, force: bool, keep_raw: bool) -> bool:
    out_path = out_dir / GRIDFED_FILE_TEMPLATE.format(year=year)
    if out_path.exists() and not force:
        print(f"{year}: {out_path.name} exists — skipping")
        return True
    print(f"{year}:")
    raw_dir = out_dir / RAW_SUBDIR
    raw_dir.mkdir(parents=True, exist_ok=True)
    url = GRIDFED_URL.format(year=year)
    zip_path = raw_dir / url.rsplit("/", 1)[-1]
    if zip_path.exists():
        print(f"  reusing {zip_path.name}")
        md5 = md5_of(zip_path)
    else:
        md5 = download(url, zip_path)
    record_checksum(out_dir / CHECKSUM_FILE, zip_path.name, md5)

    raw_nc = unzip_single(zip_path, raw_dir)
    try:
        build_year(raw_nc, year, out_path, {"url": url, "md5": md5})
    finally:
        if not keep_raw:
            raw_nc.unlink(missing_ok=True)
            zip_path.unlink(missing_ok=True)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--years",
        default=f"{STARTING_YEAR}-{GRIDFED_LAST_YEAR}",
        help="year range/list, e.g. 2020-2024 or 2019,2021 (default: %(default)s)",
    )
    parser.add_argument("--force", action="store_true", help="rebuild years that already exist")
    parser.add_argument(
        "--keep-raw", action="store_true", help=f"keep the 0.1° zip + netCDF under {RAW_SUBDIR}/"
    )
    args = parser.parse_args(argv)

    out_dir = Path(GRIDFED_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    years = parse_years(args.years)
    print(f"GridFED {GRIDFED_VERSION} → {out_dir}/  ({len(years)} years)")
    for year in years:
        process_year(year, out_dir, force=args.force, keep_raw=args.keep_raw)
    if not args.keep_raw:
        shutil.rmtree(out_dir / RAW_SUBDIR, ignore_errors=True)
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
