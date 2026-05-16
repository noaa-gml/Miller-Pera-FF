"""Tests for provenance.py — the output-file provenance metadata."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import config
from provenance import provenance_attrs

EXPECTED_KEYS = {
    "pipeline_version",
    "pipeline_git_commit",
    "pipeline_url",
    "v2026b_annual_method",
    "created",
    "created_on_host",
    "package_versions",
    "input_data_fingerprint",
}


def test_provenance_attrs_has_expected_keys():
    assert set(provenance_attrs(method="assumed")) == EXPECTED_KEYS


def test_provenance_attrs_values_are_all_strings():
    """netCDF global attributes must be strings — xarray/netCDF4 require it."""
    for k, v in provenance_attrs(method="cm_yearly").items():
        assert isinstance(v, str), f"{k} is {type(v).__name__}, not str"


def test_provenance_records_the_method():
    assert provenance_attrs(method="assumed")["v2026b_annual_method"] == "assumed"
    assert provenance_attrs(method="cm_yearly")["v2026b_annual_method"] == "cm_yearly"


def test_provenance_version_matches_config():
    assert provenance_attrs(method="assumed")["pipeline_version"] == config.PRODUCT_VERSION


def test_provenance_git_commit_is_populated():
    """A git checkout reports a short SHA + clean/dirty state; otherwise a
    clear fallback string. Either way the field is non-empty."""
    commit = provenance_attrs(method="assumed")["pipeline_git_commit"]
    assert commit
    assert any(tag in commit for tag in ("clean", "uncommitted", "not a git checkout"))


def test_provenance_package_versions_records_numpy():
    """The package-version record must name numpy — the ABI-sensitive dep
    whose unpinned drift this provenance is meant to catch."""
    pkgs = provenance_attrs(method="assumed")["package_versions"]
    assert "numpy=" in pkgs
    assert "python=" in pkgs
