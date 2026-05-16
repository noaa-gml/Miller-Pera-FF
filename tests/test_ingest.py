"""Unit tests for ingest.py logic.

ingest.py imports the conda-only geospatial stack, so these sit behind
importorskip: they run in the conda-based CI job and skip in the lightweight
one.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


def _ingest():
    """Import ingest behind the geospatial-stack guard."""
    pytest.importorskip("xesmf")
    pytest.importorskip("xcdat")
    import ingest
    return ingest


def _country_frame():
    """A small (Nation, Year) frame shaped like the CDIAC national table."""
    nations = ["Aland", "Bigland", "Cland"]
    years = [2020, 2021]
    idx = pd.MultiIndex.from_product([nations, years], names=["Nation", "Year"])
    return pd.DataFrame(
        {
            "emissions":  [1.0, 2.0, 10.0, 20.0, 100.0, 200.0],
            "per_capita": [0.1, 0.2,  0.5,  0.6,   0.9,   1.0],
        },
        index=idx,
    )


def test_aggregate_countries_sums_emissions():
    """Little countries' emissions are summed into the big-country entry."""
    ingest = _ingest()
    out = ingest.aggregate_countries(_country_frame(), ["Aland", "Cland"], "ACland")
    merged = out.xs("ACland", level="Nation")["emissions"]
    assert list(merged) == [101.0, 202.0]   # Aland + Cland, per year


def test_aggregate_countries_per_capita_not_summed():
    """per_capita is intensive — it takes the first little country's value."""
    ingest = _ingest()
    out = ingest.aggregate_countries(_country_frame(), ["Aland", "Cland"], "ACland")
    pc = out.xs("ACland", level="Nation")["per_capita"]
    assert list(pc) == [0.1, 0.2]           # Aland's, not Aland + Cland


def test_aggregate_countries_drops_the_little_names():
    ingest = _ingest()
    out = ingest.aggregate_countries(_country_frame(), ["Aland", "Cland"], "ACland")
    nations = set(out.index.get_level_values("Nation"))
    assert "Aland" not in nations
    assert "Cland" not in nations
    assert {"ACland", "Bigland"} <= nations


def test_aggregate_countries_leaves_others_unchanged():
    ingest = _ingest()
    out = ingest.aggregate_countries(_country_frame(), ["Aland", "Cland"], "ACland")
    assert list(out.xs("Bigland", level="Nation")["emissions"]) == [10.0, 20.0]
