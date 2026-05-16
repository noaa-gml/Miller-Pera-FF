"""Tests for the PIQS spline (Rasmussen 1991) implementation in ff_country."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from ff_country import _piqs


def _eval_spline(fit: np.ndarray, x: np.ndarray, t: float) -> float:
    """Evaluate the spline at a single time t. Picks the year segment by x[i] <= t < x[i+1]."""
    i = int(np.searchsorted(x, t, side="right") - 1)
    i = max(0, min(i, fit.shape[1] - 1))
    dt = t - x[i]
    a, b, c = fit[0, i], fit[1, i], fit[2, i]
    return float(a * dt ** 2 + b * dt + c)


def _integrate_year(fit: np.ndarray, x: np.ndarray, i: int, n_samples: int = 365) -> float:
    """Numerically integrate the spline over year segment i. Returns mean over [x[i], x[i+1])."""
    span = x[i + 1] - x[i]
    ts = np.linspace(x[i], x[i + 1], n_samples, endpoint=False)
    vals = np.array([_eval_spline(fit, x, t) for t in ts])
    return float(vals.mean()) * 1.0 * (span / span)  # span/span = 1; mean is the year-mean


@pytest.fixture
def small_series():
    """5 years with a known piecewise-quadratic-friendly profile."""
    x = np.array([0.0, 1.0, 2.0, 3.0, 4.0, 5.0])           # year edges
    ybar = np.array([10.0, 12.0, 11.0, 14.0, 13.0])[:, None, None]  # 5 yearly values
    return x, ybar


def test_piqs_integral_preservation(small_series):
    """Each year's mean integrated over [x_i, x_{i+1}] must equal ybar[i]."""
    x, ybar = small_series
    fit = _piqs(x, ybar)
    assert fit.shape == (3, 5, 1, 1)
    for i in range(5):
        # High-density numerical integral of a·dt² + b·dt + c over [0, 1]
        a, b, c = fit[0, i, 0, 0], fit[1, i, 0, 0], fit[2, i, 0, 0]
        # Analytical integral of a t² + b t + c over [0, 1] = a/3 + b/2 + c
        integral = a / 3.0 + b / 2.0 + c
        assert abs(integral - ybar[i, 0, 0]) < 1e-9, \
            f"year {i}: integrated {integral:.6f} vs ybar {ybar[i, 0, 0]:.6f}"


def test_piqs_continuity_at_year_boundaries(small_series):
    """Spline value at right edge of year i must equal value at left edge of year i+1."""
    x, ybar = small_series
    fit = _piqs(x, ybar)
    for i in range(4):
        # Right edge of year i: dt = 1
        right = fit[0, i, 0, 0] * 1.0 + fit[1, i, 0, 0] * 1.0 + fit[2, i, 0, 0]
        # Left edge of year i+1: dt = 0  →  just c
        left  = fit[2, i + 1, 0, 0]
        assert abs(right - left) < 1e-9, \
            f"discontinuity at year boundary {i+1}: {right:.6f} vs {left:.6f}"


def test_piqs_handles_constant_series():
    """Constant input → constant output (a=0, b=0, c=ybar)."""
    x = np.array([0.0, 1.0, 2.0, 3.0])
    ybar = np.full((3, 1, 1), 7.0)
    fit = _piqs(x, ybar)
    np.testing.assert_allclose(fit[0], 0.0, atol=1e-9)
    np.testing.assert_allclose(fit[1], 0.0, atol=1e-9)
    np.testing.assert_allclose(fit[2], 7.0, atol=1e-9)


def test_piqs_vectorised_over_pixels():
    """PIQS supports per-pixel ybar; per-pixel fits should match scalar fits."""
    x = np.array([0.0, 1.0, 2.0, 3.0, 4.0])
    rng = np.random.default_rng(42)
    ybar = rng.uniform(5.0, 15.0, size=(4, 3, 2))
    fit = _piqs(x, ybar)
    assert fit.shape == (3, 4, 3, 2)
    # For each pixel, the scalar fit should match the vectorised fit.
    for i in range(3):
        for j in range(2):
            scalar_fit = _piqs(x, ybar[:, i, j].reshape(4, 1, 1))
            np.testing.assert_allclose(
                fit[:, :, i, j], scalar_fit[:, :, 0, 0], rtol=1e-12,
            )
