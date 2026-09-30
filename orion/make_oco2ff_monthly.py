#!/usr/bin/env python3
"""make_oco2ff_monthly.py — Orion-side builder: OCO-2 FF hourly → 1° monthly per-year files.

Runs on Orion (``config.ORION_PYTHON`` — numpy + netCDF4 only, no repo
imports so it can be copied over and run standalone). ``download_oco2ff.py``
uploads and invokes it with ``--build``, then pulls the result.

Source: Andy Jacobson's CF-ised copy of the OCO-2 MIP fossil-fuel prior
(Basu & Nassar, Zenodo 10.5281/zenodo.19744619; ODIAC × TIMES), one file per
day with 24 hourly ``emission`` fields in mol C m⁻² s⁻¹::

    <src>/fossil_fuel_1x1_YYYYMMDD.nc

The published product ends in February 2026; the March–April 2026 files in
the same directory are Andy's extrapolation (``ncbo`` of the 2025 same-day
file + the mean Jan–Feb 2026−2025 difference). They are averaged too but
flagged ``extrapolated = 1`` (detected from their ``history`` attribute).

Per year this writes ``<dst>/oco2ff_<version>_1x1_<year>.nc``: every month
with data, ``emission`` = arithmetic mean of all hourly fields in the month
(float32, mol C m⁻² s⁻¹) plus ``n_source_hours``, ``extrapolated``,
``cell_area``, ``month_seconds``, mid-month ``time`` with ``time_bnds``,
and global attributes recording the source directory, file count, the
source files' own ``history``/``source``, and the annual global total.
Finally ``checksums.md5`` (md5sum format) covers the product files.

Usage (on Orion):
    python make_oco2ff_monthly.py --src /work2/.../v2026.2/CF --dst /work2/.../v2026.2.monthly_1x1
        [--years 2000-2026] [--force]
"""

from __future__ import annotations

import argparse
import hashlib
import re
import socket
import sys
from calendar import monthrange
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import netCDF4
import numpy as np

SCRIPT_VERSION = "2026-09-14"
EARTH_RADIUS_KM = 6371.009  # Miller-Pera FF constants.EARTH_RADIUS
C_MOLAR_MASS = 12.011
N_LAT, N_LON = 180, 360
FILE_RE = re.compile(r"^fossil_fuel_1x1_(\d{4})(\d{2})(\d{2})\.nc$")
EPOCH = datetime(1900, 1, 1, tzinfo=UTC)


def cell_areas_m2(earth_radius_km: float = EARTH_RADIUS_KM) -> np.ndarray:
    """Exact spherical 1° × 1° cell areas (m²), (180, 360), lat ascending."""
    lat_edges = np.deg2rad(np.arange(-90.0, 91.0, 1.0))
    band = (earth_radius_km * 1e3) ** 2 * np.deg2rad(1.0) * np.diff(np.sin(lat_edges))
    return np.repeat(band[:, np.newaxis], N_LON, axis=1)


def days_since_epoch(year: int, month: int, day: int) -> float:
    return (datetime(year, month, day, tzinfo=UTC) - EPOCH).total_seconds() / 86400.0


def month_bounds(year: int, month: int) -> tuple[float, float]:
    ny, nm = (year + 1, 1) if month == 12 else (year, month + 1)
    return days_since_epoch(year, month, 1), days_since_epoch(ny, nm, 1)


def list_year(src: Path, year: int) -> dict[int, list[Path]]:
    """Daily files of *year* grouped by month, sorted by day."""
    by_month: dict[int, list[Path]] = defaultdict(list)
    for p in sorted(src.iterdir()):
        m = FILE_RE.match(p.name)
        if m and int(m.group(1)) == year:
            by_month[int(m.group(2))].append(p)
    return dict(sorted(by_month.items()))


def read_day(path: Path) -> tuple[np.ndarray, bool, dict[str, str]]:
    """Return (sum over the 24 hourly fields, is_extrapolated, global attrs)."""
    with netCDF4.Dataset(path) as nc:
        var = nc["emission"]
        var.set_auto_mask(False)  # read raw values; check the fill ourselves
        data = np.asarray(var[:], dtype=np.float64)  # (24, 180, 360)
        fill = float(getattr(var, "_FillValue", -1e34))
        n_fill = int((data == fill).sum())
        if n_fill:
            msg = f"{path.name}: {n_fill} fill-value cells"
            raise ValueError(msg)
        if data.shape != (24, N_LAT, N_LON):
            msg = f"{path.name}: unexpected shape {data.shape}"
            raise ValueError(msg)
        attrs = {k: str(nc.getncattr(k)) for k in nc.ncattrs()}
    extrapolated = "ncbo" in attrs.get("history", "")
    return data.sum(axis=0), extrapolated, attrs


def month_mean(files: list[Path]) -> tuple[np.ndarray, int, bool, dict[str, str]]:
    """Mean hourly flux over all files of a month; returns (mean, n_hours, any_extrap, attrs)."""
    total = np.zeros((N_LAT, N_LON))
    n_hours = 0
    extrap = False
    attrs: dict[str, str] = {}
    for f in files:
        day_sum, is_ex, attrs = read_day(f)
        total += day_sum
        n_hours += 24
        extrap |= is_ex
    return total / n_hours, n_hours, extrap, attrs


def build_year(src: Path, dst: Path, year: int, version: str, *, force: bool) -> Path | None:
    out = dst / f"oco2ff_{version}_1x1_{year}.nc"
    if out.exists() and not force:
        print(f"{year}: {out.name} exists — skipping", flush=True)
        return out
    by_month = list_year(src, year)
    if not by_month:
        print(f"{year}: no source files — skipping", flush=True)
        return None
    print(
        f"{year}: {sum(len(v) for v in by_month.values())} daily files, "
        f"months {min(by_month)}..{max(by_month)}",
        flush=True,
    )

    area = cell_areas_m2()
    months = list(by_month)
    fields, n_hours, flags, secs, tvals, bnds = [], [], [], [], [], []
    last_attrs: dict[str, str] = {}
    ann_pgc = 0.0
    for m in months:
        mean, nh, ex, last_attrs = month_mean(by_month[m])
        expected = 24 * monthrange(year, m)[1]
        if nh != expected:
            print(f"  WARNING {year}-{m:02d}: {nh} source hours, expected {expected}", flush=True)
        sec = monthrange(year, m)[1] * 86400.0
        b0, b1 = month_bounds(year, m)
        pgc = float((mean * area).sum()) * sec * C_MOLAR_MASS * 1e-15
        ann_pgc += pgc
        fields.append(mean.astype(np.float32))
        n_hours.append(nh)
        flags.append(int(ex))
        secs.append(sec)
        tvals.append((b0 + b1) / 2.0)
        bnds.append((b0, b1))
        print(f"  {year}-{m:02d}: {pgc:.4f} PgC{'  (extrapolated)' if ex else ''}", flush=True)

    tmp = out.with_suffix(".nc.tmp")
    with netCDF4.Dataset(tmp, "w", format="NETCDF4") as nc:
        nc.createDimension("time", None)
        nc.createDimension("lat", N_LAT)
        nc.createDimension("lon", N_LON)
        nc.createDimension("bnds", 2)
        t = nc.createVariable("time", "f8", ("time",))
        t.units = f"days since {EPOCH:%Y-%m-%d} 00:00:00"
        t.calendar = "standard"
        t.long_name = "mid-month"
        t.bounds = "time_bnds"
        t[:] = tvals
        tb = nc.createVariable("time_bnds", "f8", ("time", "bnds"))
        tb.units = t.units
        tb[:] = bnds
        lat = nc.createVariable("lat", "f8", ("lat",))
        lat.units, lat.long_name = "degrees_north", "latitude (cell centre)"
        lat[:] = np.arange(-89.5, 90.0, 1.0)
        lon = nc.createVariable("lon", "f8", ("lon",))
        lon.units, lon.long_name = "degrees_east", "longitude (cell centre)"
        lon[:] = np.arange(-179.5, 180.0, 1.0)
        ca = nc.createVariable("cell_area", "f8", ("lat", "lon"))
        ca.units, ca.long_name = "m2", f"spherical cell area, R = {EARTH_RADIUS_KM} km"
        ca[:] = area
        ms = nc.createVariable("month_seconds", "f8", ("time",))
        ms.units, ms.long_name = "s", "seconds in month"
        ms[:] = secs
        nh_v = nc.createVariable("n_source_hours", "i4", ("time",))
        nh_v.long_name = "number of hourly source fields averaged"
        nh_v[:] = n_hours
        ex_v = nc.createVariable("extrapolated", "i1", ("time",))
        ex_v.long_name = (
            "1 = month not in the published product; A. Jacobson's extrapolation "
            "(2025 same-day + mean Jan-Feb 2026-2025 difference)"
        )
        ex_v.flag_values = np.array([0, 1], dtype=np.int8)
        ex_v.flag_meanings = "published extrapolated"
        ex_v[:] = flags
        e = nc.createVariable("emission", "f4", ("time", "lat", "lon"), zlib=True, complevel=4)
        e.units = "mol m-2 s-1"
        e.long_name = f"OCO-2 FF {version} fossil fuel CO2 flux (mol C m-2 s-1), monthly mean"
        e.cell_methods = "time: mean (of hourly fields)"
        e.annual_global_total_PgC = round(ann_pgc, 6)
        e[:] = np.stack(fields)

        nc.title = f"OCO-2 FF {version} (ODIAC x TIMES) monthly mean, 1 x 1 degree"
        nc.oco2ff_version = version
        nc.source_doi = "10.5281/zenodo.19744619"
        nc.source_dir = f"{socket.gethostname()}:{src}"
        nc.source_n_files = sum(len(v) for v in by_month.values())
        nc.source_source = last_attrs.get("source", "")
        nc.source_history = last_attrs.get("history", "")
        nc.processing = (
            "Arithmetic mean of every hourly 'emission' field in the month "
            "(equal-weight hours, so the mean flux is exact); no unit change. "
            "Global totals use spherical cell areas and calendar month lengths."
        )
        nc.reference = (
            "Basu & Nassar (2026), Fossil Fuel CO2 Emissions for the OCO2 MIP, "
            "v2026.2, Zenodo; Nassar et al. (2013) JGR 118, doi:10.1029/2012JD018196; "
            "Oda et al. (2018) ESSD 10, 87-107"
        )
        nc.created = datetime.now(UTC).isoformat()
        nc.created_by = f"{Path(__file__).name} {SCRIPT_VERSION}, Miller-Pera FF pipeline"
        nc.Conventions = "CF-1.8"
    tmp.replace(out)
    print(f"  wrote {out.name}  ({ann_pgc:.4f} PgC over {len(months)} months)", flush=True)
    return out


def write_checksums(dst: Path, version: str) -> None:
    lines = []
    for p in sorted(dst.glob(f"oco2ff_{version}_1x1_*.nc")):
        md5 = hashlib.md5()  # noqa: S324
        with p.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                md5.update(chunk)
        lines.append(f"{md5.hexdigest()}  {p.name}\n")
    (dst / "checksums.md5").write_text("".join(lines))
    print(f"wrote checksums.md5 ({len(lines)} files)")


def parse_years(spec: str) -> list[int]:
    years: set[int] = set()
    for part in spec.split(","):
        if "-" in part:
            a, b = part.split("-", 1)
            years.update(range(int(a), int(b) + 1))
        else:
            years.add(int(part))
    return sorted(years)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--src", required=True, type=Path, help="directory of daily CF files")
    ap.add_argument("--dst", required=True, type=Path, help="output directory")
    ap.add_argument("--version", default="v2026.2")
    ap.add_argument("--years", default="2000-2026")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)
    args.dst.mkdir(parents=True, exist_ok=True)
    for year in parse_years(args.years):
        build_year(args.src, args.dst, year, args.version, force=args.force)
    write_checksums(args.dst, args.version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
