#!/usr/bin/env python3
"""package_delivery.py — assemble the delivery bundle from source.

Replaces the error-prone manual ``cp`` sync into ``delivery/``. The
delivery directory is a *build artefact*: this script wipes it and rebuilds
it from the canonical source files, so it can never silently drift from the
repo.

Two output layouts:

* **Bundle** (default) — the citable archive layout, with docs, full source
  under ``code/``, and (optionally) outputs under ``outputs/``. Suitable for
  GitHub Releases / Zenodo / DOI archives.

* **Orion-flat** (``--orion-flat``) — the CarbonTracker drop-zone layout
  matching the ``/work2/noaa/co2/input/FF/Miller/<YYYYMMDD>/`` precedent:
  flat directory, no ``code/``, CT filenames stripped of their method
  suffix so consumer scripts read the same names release-to-release.

Bundle layout::

    <outdir>/
    ├── README.md  methodology.html  landing_page.html  summary_figure.png
    ├── CHANGELOG.md  CITATION.cff  LICENSE.md
    ├── code/
    │   ├── *.py  pyproject.toml  verify_*.ipynb
    │   ├── inputs/   (country_aliases.json, canonical_countries.csv)
    │   └── tests/    (test_*.py)
    └── outputs/      (only with --with-outputs)
        ├── gml_ff_co2_2026b_<method>.nc
        ├── ct/flux1x1_ff_<method>.*.nc
        └── verify_report.html, v2026b_method_comparison.{md,png}

Orion-flat layout (always requires --method)::

    <outdir>/
    ├── README.md  CHANGELOG.md  split_ct.py
    ├── flux1x1_ff.YYYY.nc       (per-year, full years only)
    ├── flux1x1_ff.YYYYMM.nc     (per-month, includes partial-year tail)
    ├── v2026b_method_comparison.{md,png}
    └── from_ash/
        └── gml_ff_co2_2026b_<method>.nc

Usage::

    python package_delivery.py                              # code + docs only
    python package_delivery.py --with-outputs --method assumed
    python package_delivery.py --with-outputs --method cm_yearly --zip
    python package_delivery.py --orion-flat --method assumed
    python package_delivery.py --orion-flat --method assumed --outdir delivery/20260608
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
    "timeutils.py",
    "config.py",
    "provenance.py",
    "compare_methods.py",
    "pyproject.toml",
    "environment.yml",
    "requirements.txt",
    "conda-lock.yml",
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

# ── Orion-flat layout ────────────────────────────────────────────────────────
# Lean: GitHub is the canonical source for everything else.
ORION_FLAT_DOCS = ["README.md", "CHANGELOG.md"]
ORION_FLAT_REPORTS = ["v2026b_method_comparison.md", "v2026b_method_comparison.png"]


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


def _copy_orion_flat(outdir: Path, method: str) -> list[str]:
    """Copy the chosen method's outputs in the CarbonTracker drop-zone layout.

    Differences vs the bundle layout:
      * Flat — no ``code/`` or ``outputs/`` subdirs.
      * ``flux1x1_ff_<method>.*.nc`` is renamed to ``flux1x1_ff.*.nc``, so
        consumers reading ``<Miller>/<dropdate>/flux1x1_ff.*.nc`` see the
        same names release-to-release.
      * Only the producing script (``split_ct.py``), README, CHANGELOG, and
        the method-comparison report ship as docs; the full source lives in
        the GitHub repo, not in the drop-zone.
      * The monolithic source goes to ``from_ash/`` (matches the 20260225
        precedent and keeps its method tag — it IS the per-method source).
    """
    copied: list[str] = []
    out_src = REPO / "outputs"

    # Docs at the top level.
    for name in ORION_FLAT_DOCS:
        _copy(REPO / name, outdir / name)
        copied.append(name)

    # The script that produced the CT files.
    _copy(REPO / "split_ct.py", outdir / "split_ct.py")
    copied.append("split_ct.py")

    # CT-format outputs flattened to top level, method suffix stripped.
    ct_files = sorted((out_src / "ct").glob(f"flux1x1_ff_{method}.*.nc"))
    if not ct_files:
        raise FileNotFoundError(
            f"no outputs/ct/flux1x1_ff_{method}.*.nc files — "
            f"run post_process.py --method {method} first",
        )
    src_prefix = f"flux1x1_ff_{method}."
    dst_prefix = "flux1x1_ff."
    for src in ct_files:
        new_name = src.name.replace(src_prefix, dst_prefix, 1)
        if new_name == src.name:  # defensive: glob should guarantee a hit
            raise ValueError(
                f"unexpected filename pattern (no '{src_prefix}'): {src.name}",
            )
        _copy(src, outdir / new_name)
    copied.append(
        f"flux1x1_ff.*.nc ({len(ct_files)} CarbonTracker files, "
        f"method tag stripped)",
    )

    # Method-comparison report so the alternative is on record.
    for name in ORION_FLAT_REPORTS:
        p = out_src / name
        if p.exists():
            _copy(p, outdir / name)
            copied.append(name)
        else:
            print(f"  note: optional {name} not present — skipping")

    # Monolithic source in from_ash/ — keeps the method tag (it IS the
    # per-method source, distinct from the CT-format derivatives).
    mono = out_src / f"gml_ff_co2_2026b_{method}.nc"
    if not mono.exists():
        raise FileNotFoundError(
            f"{mono} not found — run ff_country.py + post_process.py "
            f"--method {method} first",
        )
    _copy(mono, outdir / "from_ash" / mono.name)
    copied.append(f"from_ash/{mono.name}")

    return copied


def build(
    outdir: Path,
    method: str | None,
    *,
    with_outputs: bool,
    make_zip: bool,
    orion_flat: bool = False,
) -> int:
    """Assemble the bundle (or Orion-flat layout). Returns a process exit code."""
    if orion_flat and with_outputs:
        print(
            "ERROR: --orion-flat is its own layout; "
            "do not combine with --with-outputs",
            file=sys.stderr,
        )
        return 2
    if orion_flat and method is None:
        print("ERROR: --orion-flat requires --method", file=sys.stderr)
        return 2
    if with_outputs and method is None:
        print("ERROR: --with-outputs requires --method", file=sys.stderr)
        return 2

    if outdir.exists():
        print(f"Wiping existing {outdir}/ ...")
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True)

    copied: list[str] = []

    if orion_flat:
        assert method is not None  # guarded above
        print(f"Assembling Orion-flat delivery (method={method}) ...")
        copied += _copy_orion_flat(outdir, method)
        layout_name = "Orion-flat delivery"
    else:
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
        layout_name = "Bundle"

    print(f"\n{layout_name} assembled at {outdir}/  ({len(copied)} items):")
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
        "--outdir", type=Path, default=None,
        help=(
            "Output directory. Wiped + rebuilt. "
            "Default: delivery/ (or delivery_orion/ with --orion-flat)."
        ),
    )
    parser.add_argument(
        "--method", choices=METHODS, default=None,
        help=(
            "Which v2026b output set to include "
            "(required with --with-outputs or --orion-flat)."
        ),
    )
    parser.add_argument(
        "--with-outputs", action="store_true",
        help="Also copy the (large) NetCDF outputs for the chosen --method.",
    )
    parser.add_argument(
        "--orion-flat", action="store_true", dest="orion_flat",
        help=(
            "Build the flat Orion drop-zone layout instead of the bundle. "
            "Implies a single chosen --method; strips the method suffix "
            "from CT filenames so the layout matches "
            "/work2/.../Miller/<dropdate>/. "
            "Mutually exclusive with --with-outputs."
        ),
    )
    parser.add_argument(
        "--zip", action="store_true", dest="make_zip",
        help="Zip the bundle when done.",
    )
    args = parser.parse_args(argv)
    default_subdir = "delivery_orion" if args.orion_flat else "delivery"
    outdir = args.outdir if args.outdir is not None else REPO / default_subdir
    return build(
        outdir, args.method,
        with_outputs=args.with_outputs, make_zip=args.make_zip,
        orion_flat=args.orion_flat,
    )


if __name__ == "__main__":
    sys.exit(main())
