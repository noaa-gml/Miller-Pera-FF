"""Unit tests for the aggregation helpers in compare_national.py.

GridFED and the Miller-Pera output are gitignored bulk data, so the end-to-end
comparison isn't run here; these exercise the helpers on synthetic fields,
including a known-bad mask that the gate must reject.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from compare_national import TG_PER_MOL, annual_tg, country_sums, gate, nearest_owner


def _toy_map() -> np.ndarray[Any, Any]:
    cmap = np.zeros((6, 8), dtype=int)
    cmap[1:3, 1:4] = 100
    cmap[3:5, 4:7] = 200
    return cmap


def test_country_sums_partition_the_land() -> None:
    cmap = _toy_map()
    field = np.arange(cmap.size, dtype=float).reshape(cmap.shape)
    sums = country_sums(field, cmap, np.array([100, 200]))
    assert sums.sum() == pytest.approx(field[cmap > 0].sum())


def test_annual_tg_recovers_mass() -> None:
    flux = np.full((12, 2, 2), 1e-9)
    area = np.full((2, 2), 1e10)
    secs = np.full(12, 30 * 86400.0)
    out = annual_tg(flux, area, secs)
    assert out[0, 0] == pytest.approx(1e-9 * 1e10 * 360 * 86400 * TG_PER_MOL)


def test_nearest_owner_keeps_land_and_fills_ocean() -> None:
    cmap = _toy_map()
    land = cmap > 0
    owner = nearest_owner(cmap, land)
    assert (owner[land] == cmap[land]).all()
    assert set(np.unique(owner)) == {100, 200}
    assert owner[0, 0] == 100
    assert owner[5, 7] == 200


def test_gate_passes_matching_and_fails_misaligned() -> None:
    years = [2000, 2001]
    cdiac = pd.DataFrame({y: [100.0, 50.0, 10.0] for y in years}, index=["A", "B", "C"])
    assert gate(cdiac * 1.001, cdiac).max() < 0.01
    bad = cdiac.copy()
    bad.loc["A"] = 0.0  # a mis-oriented mask leaves a big emitter empty
    with pytest.raises(RuntimeError, match="mask gate FAILED"):
        gate(bad, cdiac)
