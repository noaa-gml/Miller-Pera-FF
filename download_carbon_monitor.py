#!/usr/bin/env python3
"""download_carbon_monitor.py — Fetch CarbonMonitor near-real-time global CO2 data.

Downloads the latest global daily fossil-fuel + cement CO2 emissions CSV from
CarbonMonitor's public API and saves it under ``inputs/carbon_monitor/``. This
file feeds the NRT extrapolation step in the Miller-Pera FF pipeline — the
ratios derived from it (year-over-year and month-over-same-month-prior-year)
fill in months that are not yet covered by Energy Institute or USGS sources
(e.g., Feb-Apr 2026 in the v2026 production cycle).

Source:
    Project home : https://carbonmonitor.org/
    CSV endpoint : https://datas.carbonmonitor.org/API/downloadFullDataset.php
                   ?source=carbon_global

The CSV is updated frequently (typically weekly). It always covers
2019-01-01 through approximately the end of last month. The server returns
the file with a ``Content-Disposition`` filename of
``carbonmonitor-global_datas_<download-date>.csv`` — we honour that.

Notes for the ingest step (run later, not here):
  * The dataset already lists all 27 EU members individually plus an
    ``EU27`` aggregate. The historical IDL workaround that subtracted
    France/Germany/Italy/Spain/UK from "EU27 & UK" to recover the rest of
    EU is no longer needed — drop the ``EU27`` aggregate row and use the
    individual countries directly.
  * Three rows are not real countries: ``EU27``, ``ROW``, ``WORLD``. The
    last two are kept (ROW is the wildcard for any CDIAC country not
    directly tracked; WORLD is used for the bunker/global total).
  * Six sectors are reported. The IDL pipeline excludes
    ``Domestic Aviation`` and ``International Aviation``; we will do the
    same in ingest.

Usage:
    python download_carbon_monitor.py            # download if no fresh file exists
    python download_carbon_monitor.py --force    # always re-download
"""

from __future__ import annotations

import argparse
import re
import sys
import urllib.request
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from http.client import HTTPResponse

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
CM_URL = (
    "https://datas.carbonmonitor.org/API/downloadFullDataset.php"
    "?source=carbon_global"
)
OUT_DIR = Path("inputs/carbon_monitor")
EXPECTED_HEADER = "country,date,sector,value,"      # trailing comma on purpose
EXPECTED_SECTORS = {
    "Domestic Aviation", "Ground Transport", "Industry",
    "International Aviation", "Power", "Residential",
}
# Pipeline coverage requirement for the v2026 NRT extrapolation:
# 2025 must be complete (full year), 2026 must reach at least Q1.
REQUIRED_FULL_YEAR = 2025
REQUIRED_MIN_2026_MONTH = 3   # CM data through end-of-March 2026
TARGET_MIN_2026_MONTH = 4     # ideal: through end-of-April 2026 (per Andy's ask)
# Skip download if we already have a CSV from within this many days.
FRESH_THRESHOLD_DAYS = 7


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date_from_filename(name: str) -> date | None:
    """Pull a YYYY-MM-DD date out of carbonmonitor-global_datas_YYYY-MM-DD.csv."""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", name)
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _existing_recent_csv(out_dir: Path, threshold_days: int) -> Path | None:
    """Return a CSV in *out_dir* from within *threshold_days*, if one exists."""
    today = datetime.now(UTC).date()
    candidates: list[tuple[date, Path]] = []
    for f in out_dir.glob("carbonmonitor-global_datas_*.csv"):
        d = _parse_date_from_filename(f.name)
        if d is not None and (today - d).days <= threshold_days:
            candidates.append((d, f))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _filename_from_response(response: HTTPResponse, fallback_today: date) -> str:
    """Get the server-suggested filename, or build one with today's date."""
    cd = response.headers.get("Content-Disposition", "")
    m = re.search(r"filename=([^\s;]+)", cd)
    if m:
        return m.group(1).strip('"')
    return f"carbonmonitor-global_datas_{fallback_today.isoformat()}.csv"


def _download(url: str, out_path: Path) -> None:
    """Stream the CSV to *out_path*. Atomic via a .tmp + rename."""
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    print(f"Downloading: {url}\n         -> {out_path}")
    with urllib.request.urlopen(url, timeout=120) as resp, tmp.open("wb") as fh:  # noqa: S310
        bytes_total = 0
        while True:
            chunk = resp.read(64 * 1024)
            if not chunk:
                break
            fh.write(chunk)
            bytes_total += len(chunk)
    tmp.rename(out_path)
    mb = bytes_total / (1024 * 1024)
    print(f"  saved {bytes_total:,} bytes ({mb:.1f} MB)")


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

def _validate(csv_path: Path) -> dict[str, Any]:
    """Parse the CSV, sanity-check structure and coverage, return a summary dict."""
    print(f"Validating {csv_path.name} ...")

    # Header — must match exactly to catch upstream schema changes.
    header = csv_path.read_text(encoding="utf-8").splitlines()[0]
    if header.strip() != EXPECTED_HEADER:
        raise ValueError(
            f"Unexpected CSV header: {header!r} (expected {EXPECTED_HEADER!r}). "
            "CarbonMonitor may have changed their schema; check ingest "
            "before relying on this file.",
        )

    # The CSV has a trailing empty column — pandas will read it as an unnamed
    # column. Drop it.
    df = pd.read_csv(csv_path)
    df = df.loc[:, ~df.columns.str.match(r"Unnamed")]

    expected_cols = {"country", "date", "sector", "value"}
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Date format is DD/MM/YYYY.
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    sectors = set(df["sector"].unique())
    if sectors != EXPECTED_SECTORS:
        extra = sectors - EXPECTED_SECTORS
        gone = EXPECTED_SECTORS - sectors
        raise ValueError(
            f"Sector list changed: extra={sorted(extra)}, missing={sorted(gone)}",
        )

    countries = sorted(df["country"].unique())
    aggregates = {"EU27", "ROW", "WORLD"}
    individuals = [c for c in countries if c not in aggregates]
    have_aggregates = aggregates & set(countries)
    missing_aggregates = aggregates - set(countries)

    # Coverage: 2025 full year + at least Q1 2026.
    end = df["date"].max()
    start = df["date"].min()
    end_2025 = df.loc[df["date"].dt.year == REQUIRED_FULL_YEAR, "date"].max()
    end_2026 = df.loc[df["date"].dt.year == 2026, "date"].max() if (df["date"].dt.year == 2026).any() else None

    issues: list[str] = []
    if pd.isna(end_2025) or end_2025.month != 12 or end_2025.day < 31:
        issues.append(
            f"{REQUIRED_FULL_YEAR} is not complete: latest = "
            f"{end_2025.date() if pd.notna(end_2025) else 'none'}",
        )
    if end_2026 is None:
        issues.append("no 2026 data at all")
    elif end_2026.month < REQUIRED_MIN_2026_MONTH:
        issues.append(
            f"2026 only goes through month {end_2026.month} "
            f"(need at least month {REQUIRED_MIN_2026_MONTH})",
        )

    if missing_aggregates:
        issues.append(f"missing aggregate rows: {sorted(missing_aggregates)}")

    summary: dict[str, Any] = {
        "rows": len(df),
        "start": start.date(),
        "end": end.date(),
        "n_countries": len(individuals),
        "individuals": individuals,
        "aggregates_present": sorted(have_aggregates),
        "sectors": sorted(sectors),
        "issues": issues,
    }

    # Pretty-print the summary.
    print(f"  rows         : {summary['rows']:,}")
    print(f"  date range   : {summary['start']}  ..  {summary['end']}")
    print(f"  individuals  : {summary['n_countries']} countries")
    print(f"                 {', '.join(individuals)}")
    print(f"  aggregates   : {', '.join(sorted(have_aggregates))}")
    print(f"  sectors      : {', '.join(sorted(sectors))}")
    if end_2026 is not None and end_2026.month >= TARGET_MIN_2026_MONTH:
        print(f"  ✓ covers through {end_2026.date()} (meets Andy's April 2026 target)")
    elif end_2026 is not None:
        print(
            f"  ⚠ covers through {end_2026.date()} only — "
            f"April 2026 not yet available; re-run after the next CM update",
        )

    if issues:
        print("Validation issues:")
        for msg in issues:
            print(f"  ⚠ {msg}")

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--force", action="store_true",
        help=(
            "Re-download even if a fresh CSV "
            f"(< {FRESH_THRESHOLD_DAYS} days old) is already present."
        ),
    )
    parser.add_argument(
        "--out-dir", type=Path, default=OUT_DIR,
        help=f"Output directory (default: {OUT_DIR}).",
    )
    args = parser.parse_args(argv)

    args.out_dir.mkdir(parents=True, exist_ok=True)

    if not args.force:
        recent = _existing_recent_csv(args.out_dir, FRESH_THRESHOLD_DAYS)
        if recent is not None:
            print(
                f"A recent CSV already exists: {recent} "
                f"(< {FRESH_THRESHOLD_DAYS} days old). "
                "Use --force to re-download.",
            )
            _validate(recent)
            return 0

    today = datetime.now(UTC).date()
    with urllib.request.urlopen(CM_URL, timeout=60) as resp:  # noqa: S310
        suggested_name = _filename_from_response(resp, today)

    out_path = args.out_dir / suggested_name
    _download(CM_URL, out_path)
    summary = _validate(out_path)

    return 1 if summary["issues"] else 0


if __name__ == "__main__":
    sys.exit(main())
