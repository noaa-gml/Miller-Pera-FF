"""Run-provenance metadata for the output netCDF files.

Records *how* a given output file was produced, so a data user holding
only the netCDF can trace it back to an exact pipeline state and input
dataset: the code commit (and whether the tree was clean), the run time,
the package versions, and a fingerprint of every input file
``config.py`` pointed at.

The package-version record is deliberate — an unpinned dependency that
shifts a binary ABI is exactly how a silently-wrong output happens, so
the versions that produced a file travel inside it.
"""

from __future__ import annotations

import importlib.metadata as md
import platform
import subprocess
from datetime import UTC, datetime
from glob import glob
from pathlib import Path

import config

PIPELINE_URL = "https://github.com/noaa-gml/miller-ff"

# Packages whose versions are recorded in every output file.
_TRACKED_PACKAGES = (
    "numpy", "scipy", "pandas", "xarray", "netCDF4",
    "cf-xarray", "pint-xarray", "xcdat", "xesmf",
)


def _git(*args: str) -> str:
    """Run a git command from the repo, returning stdout (empty on failure)."""
    try:
        result = subprocess.run(
            ["git", *args],
            capture_output=True, text=True, timeout=10, check=False,
            cwd=Path(__file__).parent,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def _git_commit() -> str:
    """Short commit SHA plus a clean / uncommitted-changes flag."""
    sha = _git("rev-parse", "--short", "HEAD")
    if not sha:
        return "unknown (not a git checkout)"
    state = "uncommitted changes present" if _git("status", "--porcelain") else "clean"
    return f"{sha} ({state})"


def _fingerprint(path: str) -> str:
    """`name  size  mtime` fingerprint of a single file (size+mtime ID it)."""
    p = Path(path)
    if not p.exists():
        return f"{path} — MISSING"
    st = p.stat()
    mtime = datetime.fromtimestamp(st.st_mtime, tz=UTC).date().isoformat()
    return f"{p.name}  {st.st_size:,} bytes  mtime={mtime}"


def _input_summary() -> str:
    """Multi-line fingerprint of every input file config.py points at."""
    lines: list[str] = []
    for label, path in [
        ("CDIAC global  ", config.CDIAC_GLOBAL_XLSX),
        ("CDIAC national", config.CDIAC_NATIONAL_XLSX),
        ("Energy Inst.  ", config.EI_XLSX),
    ]:
        lines.append(f"  {label}: {_fingerprint(path)}")

    cm_files = sorted(glob(config.CM_CSV_GLOB))
    cm = _fingerprint(cm_files[-1]) if cm_files else "(no CarbonMonitor CSV)"
    lines.append(f"  CarbonMonitor : {cm}")

    # Directory / glob inputs are summarised (EDGAR dirs hold dozens of files).
    for label, pattern in [
        ("EDGAR TOTALS  ", f"{config.EDGAR_TOTALS_DIR}/*.nc"),
        ("EDGAR NMM     ", f"{config.EDGAR_NMM_DIR}/*.nc"),
        ("EDGAR PRO_FFF ", f"{config.EDGAR_PRO_FFF_DIR}/*.nc"),
        ("USGS cement   ", config.USGS_CEMENT_GLOB),
    ]:
        files = [Path(f) for f in sorted(glob(pattern))]
        total = sum(f.stat().st_size for f in files if f.exists())
        lines.append(f"  {label}: {len(files)} files, {total:,} bytes total")

    return "\n".join(lines)


def _package_versions() -> str:
    """`pkg=version; …` for the packages that affect numerical output."""
    parts: list[str] = [f"python={platform.python_version()}"]
    for pkg in _TRACKED_PACKAGES:
        try:
            parts.append(f"{pkg}={md.version(pkg)}")
        except md.PackageNotFoundError:
            continue
    return "; ".join(parts)


def provenance_attrs(*, method: str) -> dict[str, str]:
    """Build the global attributes recording how an output file was produced.

    Merge the returned dict into a dataset's ``.attrs`` before writing the
    netCDF.
    """
    return {
        "pipeline_version": config.PRODUCT_VERSION,
        "pipeline_git_commit": _git_commit(),
        "pipeline_url": PIPELINE_URL,
        "v2026b_annual_method": method,
        "created": datetime.now(UTC).isoformat(),
        "created_on_host": platform.node() or "unknown",
        "package_versions": _package_versions(),
        "input_data_fingerprint": "\n" + _input_summary(),
    }
