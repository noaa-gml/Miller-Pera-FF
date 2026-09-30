#!/usr/bin/env python3
"""download_oco2ff.py — Pull the OCO-2 FF 1° monthly reference product from Orion.

OCO-2 FF (Basu & Nassar; ODIAC × TIMES, the OCO-2 MIP prior) is published
on Zenodo as ~28 GB of hourly files per year, and NOAA keeps a CF-ised
copy on Orion (``config.OCO2FF_ORION_SRC``). Rather than re-download the
archive, the monthly product is built *on Orion* by
``orion/make_oco2ff_monthly.py`` into ``config.OCO2FF_ORION_DIR`` and this
script fetches it (``rsync`` over the user's ``ssh`` alias
``config.ORION_HOST`` — expect one Duo push per ssh session).

It is a *reference* product — used only by ``compare_products.py`` — and
never feeds the pipeline.

    python download_oco2ff.py            # rsync the product + verify MD5s + schema
    python download_oco2ff.py --build    # first run the builder on Orion, then fetch
    python download_oco2ff.py --build --years 2025-2026 --force   # rebuild a subset

The product: ``inputs/OCO2FF_<version>/oco2ff_<version>_1x1_<year>.nc`` —
monthly-mean ``emission`` (mol C m⁻² s⁻¹, float32) with ``extrapolated``
flagging the months beyond the published record (Mar–Apr 2026 are Andy
Jacobson's extrapolation), ``n_source_hours``, ``cell_area``,
``month_seconds``; ``checksums.md5`` is produced on Orion and verified here.
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

import xarray as xr

from config import (
    OCO2FF_DIR,
    OCO2FF_FILE_TEMPLATE,
    OCO2FF_ORION_DIR,
    OCO2FF_ORION_SRC,
    OCO2FF_VERSION,
    ORION_HOST,
    ORION_PYTHON,
)

BUILDER = Path(__file__).parent / "orion" / "make_oco2ff_monthly.py"
FETCH_NAMES = (OCO2FF_FILE_TEMPLATE.format(year="*"), "checksums.md5", "README.notes")
REQUIRED_VARS = ("emission", "extrapolated", "n_source_hours", "cell_area", "month_seconds")


def _run(cmd: list[str]) -> None:
    """Run a command, streaming its output; raise on failure."""
    print("  $ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)  # fixed argv, no shell


def _tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        msg = f"{name!r} not found on PATH"
        raise FileNotFoundError(msg)
    return path


def build_on_orion(years: str | None, *, force: bool) -> None:
    """Upload the builder and run it on Orion (writes into OCO2FF_ORION_DIR)."""
    remote_script = f"{OCO2FF_ORION_DIR}/{BUILDER.name}"
    _run([_tool("ssh"), ORION_HOST, f"mkdir -p {OCO2FF_ORION_DIR}"])
    _run([_tool("scp"), "-q", str(BUILDER), f"{ORION_HOST}:{remote_script}"])
    remote_cmd = (
        f"{ORION_PYTHON} {remote_script} --src {OCO2FF_ORION_SRC} "
        f"--dst {OCO2FF_ORION_DIR} --version {OCO2FF_VERSION}"
    )
    if years:
        remote_cmd += f" --years {years}"
    if force:
        remote_cmd += " --force"
    _run([_tool("ssh"), ORION_HOST, remote_cmd])


def fetch(out_dir: Path) -> None:
    """Fetch the product files (+ checksums, notes) from Orion into *out_dir* via rsync."""
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [_tool("rsync"), "-av", "--progress"]
    for name in FETCH_NAMES:
        cmd += ["--include", name]
    cmd += ["--exclude", "*", f"{ORION_HOST}:{OCO2FF_ORION_DIR}/", f"{out_dir}/"]
    _run(cmd)


def verify_checksums(out_dir: Path) -> list[Path]:
    """Check every local product file against Orion's checksums.md5; return the files."""
    manifest = out_dir / "checksums.md5"
    if not manifest.exists():
        msg = f"{manifest} missing — was the product built on Orion?"
        raise FileNotFoundError(msg)
    files: list[Path] = []
    for line in manifest.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        digest, name = line.split(None, 1)
        p = out_dir / name.strip()
        if not p.exists():
            msg = f"{p.name} listed in checksums.md5 but not fetched"
            raise FileNotFoundError(msg)
        md5 = hashlib.md5()  # noqa: S324 — integrity, not security
        with p.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                md5.update(chunk)
        if md5.hexdigest() != digest:
            msg = f"{p.name}: md5 {md5.hexdigest()} != {digest} (corrupt transfer?)"
            raise RuntimeError(msg)
        files.append(p)
    print(f"  {len(files)} files match checksums.md5")
    return files


def validate(files: list[Path]) -> None:
    """Schema + sanity check of each product file."""
    for p in files:
        with xr.open_dataset(p) as ds:
            missing = set(REQUIRED_VARS) - set(ds.variables)
            if missing:
                msg = f"{p.name}: missing variables {sorted(missing)}"
                raise ValueError(msg)
            if ds.sizes["lat"] != 180 or ds.sizes["lon"] != 360 or not 1 <= ds.sizes["time"] <= 12:
                msg = f"{p.name}: unexpected dimensions {dict(ds.sizes)}"
                raise ValueError(msg)
            if ds.attrs.get("oco2ff_version") != OCO2FF_VERSION:
                msg = f"{p.name}: version {ds.attrs.get('oco2ff_version')!r} != {OCO2FF_VERSION!r}"
                raise ValueError(msg)
            n_ex = int(ds["extrapolated"].values.sum())
            pgc = ds["emission"].attrs.get("annual_global_total_PgC")
            print(
                f"  {p.name}: {ds.sizes['time']} months, {pgc} PgC"
                + (f", {n_ex} extrapolated" if n_ex else "")
            )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--build", action="store_true", help="run the builder on Orion first")
    ap.add_argument("--years", help="year range for --build, e.g. 2025-2026")
    ap.add_argument("--force", action="store_true", help="with --build: rebuild existing years")
    ap.add_argument("--no-fetch", action="store_true", help="only verify/validate local files")
    args = ap.parse_args(argv)

    out_dir = Path(OCO2FF_DIR)
    if args.build:
        print(f"Building OCO-2 FF {OCO2FF_VERSION} monthly product on {ORION_HOST} ...")
        build_on_orion(args.years, force=args.force)
    if not args.no_fetch:
        print(f"Fetching {ORION_HOST}:{OCO2FF_ORION_DIR} → {out_dir}/ ...")
        fetch(out_dir)
    print("Verifying ...")
    files = verify_checksums(out_dir)
    validate(files)
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
