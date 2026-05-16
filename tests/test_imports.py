"""Smoke test: every pipeline module imports cleanly.

Importing a module runs its top-level code — imports, constants, and
function/class definitions — but not its ``__main__`` block, so this is a
cheap structural check, not a pipeline run. It catches a broken import or a
syntax error in a module that no other test happens to exercise.

CI's lightweight job installs only the pip stack (``requirements.txt``), so
the modules that need the conda-only geospatial packages (xesmf / xcdat) are
imported behind ``importorskip``: they run in the conda-based CI job and
skip in the lightweight one.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

# Importable with only the core scientific stack (requirements.txt).
LIGHT_MODULES = [
    "config",
    "constants",
    "timeutils",
    "country_names",
    "provenance",
    "download_carbon_monitor",
    "extrapolate_edgar",
    "ff_country",
    "split_ct",
    "compare_methods",
]

# Need the conda-only geospatial stack (xesmf / xcdat).
HEAVY_MODULES = ["ingest", "post_process"]


@pytest.mark.parametrize("module", LIGHT_MODULES)
def test_light_module_imports(module):
    importlib.import_module(module)


@pytest.mark.parametrize("module", HEAVY_MODULES)
def test_heavy_module_imports(module):
    pytest.importorskip("xesmf")
    pytest.importorskip("xcdat")
    importlib.import_module(module)
