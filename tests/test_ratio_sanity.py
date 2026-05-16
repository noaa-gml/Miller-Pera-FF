"""Sanity tests for the per-country ratio CSVs in processed_inputs/.

For EI, USGS, and CarbonMonitor we expect ratios to land in a plausible
range — anything outside [0.1, 10] would mean a 10× or 0.1× scaling of
emissions, which is almost certainly bad data rather than real.

Already enforced in verify.ipynb check 3d; here we run them as
fast unit tests that fail loud before delivery.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO = Path(__file__).parent.parent
PROC = REPO / "processed_inputs"

RATIO_LO = 0.1
RATIO_HI = 10.0


def _skip_if_missing(p: Path) -> None:
    if not p.exists():
        pytest.skip(f"{p} not present locally")


@pytest.mark.parametrize("fuel", ["gas", "oil", "coal", "flaring"])
def test_ei_country_ratios_sane(fuel: str):
    """Most EI ratios should be in [0.1, 10]. Real-world energy transitions
    (e.g. Portugal coal phase-out 2021, country starting a new export
    industry) can produce single outliers up to ~30× either way; we tolerate
    those as long as they're rare (< 1% of all year-country pairs).
    """
    p = PROC / f"EI_frac_changes_2020-2024_{fuel}.csv"
    _skip_if_missing(p)
    df = pd.read_csv(p)
    year_cols = [c for c in df.columns if c.isdigit()]
    vals = df[year_cols].to_numpy()
    finite = vals[np.isfinite(vals)]
    if len(finite) == 0:
        pytest.skip(f"no finite values in EI {fuel}")
    bad = finite[(finite < RATIO_LO) | (finite > RATIO_HI)]
    bad_frac = len(bad) / len(finite)
    # Hard limit: nothing > 100× or < 0.01× even for extreme real cases.
    extreme = finite[(finite < 0.01) | (finite > 100)]
    assert len(extreme) == 0, (
        f"EI {fuel} has {len(extreme)} ratios outside [0.01, 100] — likely "
        f"corrupt data: {extreme[:5]}"
    )
    # Soft limit: rare outliers in [0.01, 0.1] or [10, 100] are OK if < 1%.
    assert bad_frac < 0.01, (
        f"EI {fuel} has {bad_frac:.1%} ratios outside [{RATIO_LO}, {RATIO_HI}] "
        f"(threshold: 1%; range {bad.min():.3g}..{bad.max():.3g})"
    )


@pytest.mark.parametrize("fuel", ["gas", "oil", "coal"])
def test_ei_global_ratios_sane(fuel: str):
    p = PROC / f"EI_frac_changes_2020-2024_global_{fuel}.csv"
    _skip_if_missing(p)
    arr = np.loadtxt(p, skiprows=1)
    finite = arr[np.isfinite(arr)]
    assert (finite >= RATIO_LO).all() and (finite <= RATIO_HI).all(), \
        f"EI global {fuel} ratio out of [{RATIO_LO}, {RATIO_HI}]: range {finite.min():.3f}..{finite.max():.3f}"


def test_usgs_cement_ratios_sane():
    p = PROC / "USGS_cement_ratios_2020-2026.csv"
    _skip_if_missing(p)
    df = pd.read_csv(p)
    finite = df["cement"].to_numpy()
    finite = finite[np.isfinite(finite)]
    bad = finite[(finite < RATIO_LO) | (finite > RATIO_HI)]
    assert len(bad) == 0, \
        f"USGS cement has {len(bad)} ratios outside [{RATIO_LO}, {RATIO_HI}]: {bad[:5]} …"


def test_cm_yoy_ratios_sane():
    p = PROC / "CM_monthly_yoy_ratios_2026.csv"
    _skip_if_missing(p)
    df = pd.read_csv(p, index_col=0)
    vals = df.to_numpy()
    finite = vals[np.isfinite(vals)]
    bad = finite[(finite < RATIO_LO) | (finite > RATIO_HI)]
    assert len(bad) == 0, \
        f"CM YoY has {len(bad)} ratios outside [{RATIO_LO}, {RATIO_HI}]: {bad[:5]} …"


def test_cm_yearly_proxy_ratio_sane():
    p = PROC / "CM_yearly_ratio_proxy_2026.csv"
    _skip_if_missing(p)
    df = pd.read_csv(p, index_col=0)
    vals = df["yearly_ratio_proxy"].to_numpy()
    finite = vals[np.isfinite(vals)]
    # Yearly should be even closer to 1 than monthly
    assert (finite >= 0.5).all() and (finite <= 2.0).all(), \
        f"CM yearly proxy ratio outside [0.5, 2.0]: range {finite.min():.3f}..{finite.max():.3f}"


def test_cm_world_yearly_close_to_unity():
    """WORLD aggregate Q1-2026/Q1-2025 should be within 5% of 1 — a global jump
    larger than that signals a CM data anomaly worth investigating."""
    p = PROC / "CM_yearly_ratio_proxy_2026.csv"
    _skip_if_missing(p)
    df = pd.read_csv(p, index_col=0)
    world = df.at["WORLD", "yearly_ratio_proxy"]
    assert 0.95 < world < 1.05, f"WORLD CM yearly ratio = {world:.4f} (outside ±5%)"
