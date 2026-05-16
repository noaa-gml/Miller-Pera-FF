#!/usr/bin/env python3
"""package_delivery.py — assemble the delivery bundle from source.

Replaces the error-prone manual ``cp`` sync into ``delivery/``. The
delivery directory is a *build artefact*: this script wipes it and rebuilds
it from the canonical source files, so it can never silently drift from the
repo.

Bundle layout::

    <outdir>/
    ├── README.md  methodology.html  landing_page.html  summary_figure.png
    ├── code/
    │   ├── *.py  pyproject.toml  verify_*.ipynb
    │   ├── inputs/   (country_aliases.json, canonical_countries.csv)
    │   └── tests/    (test_*.py)
    └── outputs/      (only with --with-outputs)
        ├── gml_ff_co2_2026b_<method>.nc
        ├── ct/flux1x1_ff_<method>.*.nc
        └── verify_report.html, v2026b_method_comparison.{md,png}

Usage::

    python package_delivery.py                              # code + docs only
    python package_delivery.py --with-outputs --method assumed
    python package_delivery.py --with-outputs --method cm_yearly --zip
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).parent
METHODS = ("assumed", "cm_yearly")

# Copied verbatim into <outdir>/
DOC_FILES = [
    "README.md", "methodology.html", "landing_page.html",
    "summary_figure.png", "CHANGELOG.md", "CITATION.cff", "LICENSE.md",
]

# Copied into <outdir>/code/
CODE_FILES = [
    "download_carbon_monitor.py",
    "extrapolate_edgar.py",
    "ingest.py",
    "ff_country.py",
    "post_process.py",
    "split_ct.py",
    "country_names.py",
    "constants.py",
    "config.py",
    "provenance.py",
    "compare_methods.py",
    "pyproject.toml",
    "environment.yml",
    "requirements.txt",
    "Makefile",
    "verify.ipynb",
    "verify_nrt.ipynb",
]

# Small reference inputs country_names.py needs at runtime.
CODE_INPUT_FILES = [
    "inputs/canonical_countries.csv",
    "inputs/country_aliases.json",
]

# Optional output extras copied alongside the NetCDFs.
OUTPUT_EXTRAS = [
    "verify_report.html",
    "v2026b_method_comparison.md",
    "v2026b_method_comparison.png",
]


def _copy(src: Path, dst: Path) -> None:
    """Copy src → dst, creating parent dirs. Errors if src is missing."""
    if not src.exists():
        raise FileNotFoundError(f"source file missing: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _copy_code(outdir: Path) -> list[str]:
    """Copy pipeline scripts, notebooks, reference inputs, and tests."""
    copied: list[str] = []
    code_dst = outdir / "code"

    for name in CODE_FILES:
        _copy(REPO / name, code_dst / name)
        copied.append(f"code/{name}")

    for rel in CODE_INPUT_FILES:
        _copy(REPO / rel, code_dst / rel)
        copied.append(f"code/{rel}")

    test_files = sorted((REPO / "tests").glob("test_*.py"))
    for f in test_files:
        _copy(f, code_dst / "tests" / f.name)
    if test_files:
        copied.append(f"code/tests/ ({len(test_files)} test files)")

    return copied


def _copy_outputs(outdir: Path, method: str) -> list[str]:
    """Copy the chosen method's NetCDF outputs + verification artefacts."""
    copied: list[str] = []
    out_src = REPO / "outputs"
    out_dst = outdir / "outputs"

    mono = out_src / f"gml_ff_co2_2026b_{method}.nc"
    if not mono.exists():
        raise FileNotFoundError(
            f"{mono} not found — run ff_country.py + post_process.py "
            f"--method {method} first",
        )
    _copy(mono, out_dst / mono.name)
    copied.append(f"outputs/{mono.name}")

    ct_files = sorted((out_src / "ct").glob(f"flux1x1_ff_{method}.*.nc"))
    for f in ct_files:
        _copy(f, out_dst / "ct" / f.name)
    if ct_files:
        copied.append(f"outputs/ct/ ({len(ct_files)} CarbonTracker files)")
    else:
        print(f"  WARNING: no outputs/ct/flux1x1_ff_{method}.*.nc files found")

    for extra in OUTPUT_EXTRAS:
        p = out_src / extra
        if p.exists():
            _copy(p, out_dst / extra)
            copied.append(f"outputs/{extra}")
        else:
            print(f"  note: optional {extra} not present — skipping")

    return copied


def build(outdir: Path, method: str | None, *, with_outputs: bool, make_zip: bool) -> int:
    """Assemble the bundle. Returns a process exit code."""
    if with_outputs and method is None:
        print("ERROR: --with-outputs requires --method", file=sys.stderr)
        return 2

    if outdir.exists():
        print(f"Wiping existing {outdir}/ ...")
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True)

    copied: list[str] = []

    print("Copying docs ...")
    for name in DOC_FILES:
        _copy(REPO / name, outdir / name)
        copied.append(name)

    print("Copying code ...")
    copied += _copy_code(outdir)

    if with_outputs:
        assert method is not None  # guarded above
        print(f"Copying outputs (method={method}) ...")
        copied += _copy_outputs(outdir, method)
    else:
        print("Skipping outputs (pass --with-outputs --method <m> to include).")

    print(f"\nBundle assembled at {outdir}/  ({len(copied)} items):")
    for item in copied:
        print(f"  {item}")

    if make_zip:
        zip_base = outdir.with_suffix("")
        archive = shutil.make_archive(str(zip_base), "zip", root_dir=outdir.parent,
                                      base_dir=outdir.name)
        size_mb = Path(archive).stat().st_size / (1024 * 1024)
        print(f"\nZipped → {archive}  ({size_mb:.1f} MB)")

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--outdir", type=Path, default=REPO / "delivery",
        help="Bundle output directory (default: delivery/). Wiped + rebuilt.",
    )
    parser.add_argument(
        "--method", choices=METHODS, default=None,
        help="Which v2026b output set to include (required with --with-outputs).",
    )
    parser.add_argument(
        "--with-outputs", action="store_true",
        help="Also copy the (large) NetCDF outputs for the chosen --method.",
    )
    parser.add_argument(
        "--zip", action="store_true", dest="make_zip",
        help="Zip the bundle when done.",
    )
    args = parser.parse_args(argv)
    return build(args.outdir, args.method,
                 with_outputs=args.with_outputs, make_zip=args.make_zip)


if __name__ == "__main__":
    sys.exit(main())
