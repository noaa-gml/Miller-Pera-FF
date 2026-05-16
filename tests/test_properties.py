"""Property-based tests (hypothesis) for the pipeline's core numerics.

Tier 3 of the test plan. Rather than checking hand-picked inputs (Tier 1),
these generate hundreds of random inputs and assert that structural
invariants hold for *all* of them:

* PIQS spline — integral preservation, continuity, constant-input flatness.
* `_cumulative_extrap` — chained multiplication then flat-hold.
* `_distribute_to_grid` — per-country mass conservation onto the grid.
* `_is_leap` / `_days_per_month` — calendar identities vs the stdlib.

All functions under test live in ff_country.py, which imports only
numpy + pandas — so this whole file runs in CI without the geospatial stack.
"""
from __future__ import annotations

import calendar
import sys
from pathlib import Path

import numpy as np
from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.extra.numpy import arrays

sys.path.insert(0, str(Path(__file__).parent.parent))
from ff_country import (
    _cumulative_extrap,
    _days_per_month,
    _distribute_to_grid,
    _is_leap,
    _piqs,
)

# ── shared strategies ────────────────────────────────────────────────────────
# Annual emission totals — non-negative, plausible Gg C magnitudes.
emissions = st.floats(min_value=1.0, max_value=1e6,
                      allow_nan=False, allow_infinity=False)
# Year-over-year ratios — bounded growth/decline.
ratios = st.floats(min_value=0.3, max_value=3.0,
                   allow_nan=False, allow_infinity=False)


# ═════════════════════════════════════════════════════════════════════════════
# PIQS spline (Rasmussen 1991)
# ═════════════════════════════════════════════════════════════════════════════

@given(ybar=arrays(np.float64, st.integers(2, 30), elements=emissions))
@settings(max_examples=200, deadline=None)
def test_piqs_integral_preservation(ybar: np.ndarray) -> None:
    """For ANY annual series, each year's quadratic integrates back to ybar[i].

    This is the defining property of the integral-preserving spline — the
    per-segment quadratic is *constructed* to satisfy it, so it must hold to
    float precision for every input hypothesis can dream up.
    """
    n = len(ybar)
    x = np.arange(n + 1, dtype=float)
    fit = _piqs(x, ybar.reshape(n, 1, 1))
    for i in range(n):
        a, b, c = fit[0, i, 0, 0], fit[1, i, 0, 0], fit[2, i, 0, 0]
        # ∫₀¹ (a·t² + b·t + c) dt = a/3 + b/2 + c
        integral = a / 3.0 + b / 2.0 + c
        assert abs(integral - ybar[i]) <= 1e-7 * ybar[i] + 1e-9


@given(ybar=arrays(np.float64, st.integers(2, 30), elements=emissions))
@settings(max_examples=200, deadline=None)
def test_piqs_continuity(ybar: np.ndarray) -> None:
    """The spline is continuous at every interior year boundary."""
    n = len(ybar)
    x = np.arange(n + 1, dtype=float)
    fit = _piqs(x, ybar.reshape(n, 1, 1))
    for i in range(n - 1):
        right = fit[0, i, 0, 0] + fit[1, i, 0, 0] + fit[2, i, 0, 0]   # dt = 1
        left = fit[2, i + 1, 0, 0]                                     # dt = 0
        scale = max(abs(right), abs(left), 1.0)
        assert abs(right - left) <= 1e-6 * scale


@given(value=emissions, n=st.integers(2, 30))
@settings(max_examples=100, deadline=None)
def test_piqs_constant_series_is_flat(value: float, n: int) -> None:
    """A constant annual series produces a flat spline: a≈0, b≈0, c≈value."""
    x = np.arange(n + 1, dtype=float)
    fit = _piqs(x, np.full((n, 1, 1), value))
    np.testing.assert_allclose(fit[0], 0.0, atol=1e-8 * value)
    np.testing.assert_allclose(fit[1], 0.0, atol=1e-8 * value)
    np.testing.assert_allclose(fit[2], value, rtol=1e-9)


# ═════════════════════════════════════════════════════════════════════════════
# _cumulative_extrap
# ═════════════════════════════════════════════════════════════════════════════

@given(data=st.data())
@settings(max_examples=150, deadline=None)
def test_cumulative_extrap_chains_then_holds_flat(data: st.DataObject) -> None:
    """out[0] = base·r[0]; out[i] = out[i-1]·r[i]; beyond the ratios it holds flat."""
    n_extrap = data.draw(st.integers(1, 20))
    n_ratios = data.draw(st.integers(1, n_extrap))   # function needs n_ratios ≤ n_extrap
    r = data.draw(arrays(np.float64, n_ratios, elements=ratios))
    base_val = data.draw(emissions)

    out = _cumulative_extrap(np.array([base_val]), r, n_extrap)
    assert out.shape == (n_extrap, 1)

    expected = base_val * r[0]
    assert abs(out[0, 0] - expected) <= 1e-9 * abs(expected) + 1e-12
    for i in range(1, n_ratios):
        expected *= r[i]
        assert abs(out[i, 0] - expected) <= 1e-9 * abs(expected) + 1e-12
    # Years past the ratio coverage repeat the last computed value verbatim.
    for i in range(n_ratios, n_extrap):
        assert out[i, 0] == out[n_ratios - 1, 0]


@given(base_val=emissions, n_extrap=st.integers(1, 20), n_ratios=st.integers(1, 20))
@settings(max_examples=100, deadline=None)
def test_cumulative_extrap_unit_ratios_are_identity(
    base_val: float, n_extrap: int, n_ratios: int,
) -> None:
    """All-1.0 ratios → the output is just `base` repeated (no growth)."""
    n_ratios = min(n_ratios, n_extrap)
    out = _cumulative_extrap(np.array([base_val]), np.ones(n_ratios), n_extrap)
    np.testing.assert_allclose(out, base_val, rtol=1e-12)


# ═════════════════════════════════════════════════════════════════════════════
# _distribute_to_grid — mass conservation
# ═════════════════════════════════════════════════════════════════════════════
# Synthetic 360×180 grid with three countries on column slabs; the rest is
# ocean (code 0). fracarr is fixed-random EDGAR-like spatial patterns.
_DG_N_YRS = 2
_DG_CODES = [100, 200, 300]
_dg_gissmap = np.zeros((360, 180), dtype=int)
_dg_gissmap[0:60, :] = 100
_dg_gissmap[60:150, :] = 200
_dg_gissmap[150:175, :] = 300
_dg_codes_arr = np.array(_DG_CODES)
_dg_fracarr = np.random.default_rng(2026).uniform(
    0.0, 1.0, size=(_DG_N_YRS, 360, 180, 3),
)


@given(country_all=arrays(
    np.float64, (_DG_N_YRS, 3, 6),
    elements=st.floats(0.0, 1e7, allow_nan=False, allow_infinity=False),
))
@settings(max_examples=50, deadline=None)
def test_distribute_to_grid_conserves_mass(country_all: np.ndarray) -> None:
    """Every country's per-sector total is preserved exactly on the grid.

    `_distribute_to_grid` spreads each national sector total across the
    country's cells weighted by EDGAR patterns; the cell sum must equal the
    input. Sectors 1..5 (gas/oil/coal/flaring/cement) are distributed;
    index 0 (_TOTAL) is left zero by this stage.
    """
    flux = _distribute_to_grid(
        country_all, _dg_fracarr, _dg_gissmap, _dg_codes_arr, _DG_N_YRS, 1993,
    )
    assert flux.shape == (_DG_N_YRS, 360, 180, 6)
    gissmap_coarse = _dg_gissmap // 100 * 100
    for yr in range(_DG_N_YRS):
        for ci, code in enumerate(_DG_CODES):
            cells = np.where(gissmap_coarse == code)
            for sec in range(1, 6):
                got = float(flux[yr, cells[0], cells[1], sec].sum())
                want = float(country_all[yr, ci, sec])
                assert abs(got - want) <= 1e-8 * want + 1e-7


def test_distribute_to_grid_conserves_mass_uniform_fallback() -> None:
    """When EDGAR patterns are all zero, emissions spread uniformly — still conserved."""
    zero_frac = np.zeros((_DG_N_YRS, 360, 180, 3))
    country_all = np.full((_DG_N_YRS, 3, 6), 1234.5)
    flux = _distribute_to_grid(
        country_all, zero_frac, _dg_gissmap, _dg_codes_arr, _DG_N_YRS, 1993,
    )
    gissmap_coarse = _dg_gissmap // 100 * 100
    for yr in range(_DG_N_YRS):
        for code in _DG_CODES:
            cells = np.where(gissmap_coarse == code)
            for sec in range(1, 6):
                got = float(flux[yr, cells[0], cells[1], sec].sum())
                assert abs(got - 1234.5) <= 1e-9 * 1234.5


# ═════════════════════════════════════════════════════════════════════════════
# Calendar helpers
# ═════════════════════════════════════════════════════════════════════════════

@given(year=st.integers(min_value=1, max_value=4000))
def test_is_leap_matches_stdlib(year: int) -> None:
    assert _is_leap(year) == int(calendar.isleap(year))


@given(year=st.integers(min_value=1, max_value=4000))
def test_days_per_month_matches_stdlib(year: int) -> None:
    dpm = _days_per_month(year)
    assert len(dpm) == 12
    expected = [calendar.monthrange(year, m)[1] for m in range(1, 13)]
    assert list(dpm) == expected
    # Cross-check against _is_leap: the 12 month-lengths sum to the year length.
    assert int(sum(dpm)) == 365 + _is_leap(year)
