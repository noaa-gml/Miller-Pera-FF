"""Tests for inputs/canonical_countries.csv and inputs/country_aliases.json."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
import country_names

REPO = Path(__file__).parent.parent
ALIASES_PATH = REPO / "inputs" / "country_aliases.json"


@pytest.fixture(scope="module")
def canonical() -> set[str]:
    return set(country_names.load_canonical())


def test_canonical_count():
    names = country_names.load_canonical()
    # 189 canonical countries — positional alignment with GISS code table.
    assert len(names) == 189
    assert len(set(names)) == 189, "duplicates in canonical_countries.csv"


def test_canonical_no_blank_or_comment_rows():
    names = country_names.load_canonical()
    for n in names:
        assert n.strip() == n, f"trailing whitespace in {n!r}"
        assert not n.startswith("#")
        assert n != ""


def test_every_carbon_monitor_alias_value_is_canonical(canonical: set[str]):
    """Every value in CarbonMonitor_2026 must map to a canonical country.

    Catches typos when next year's CarbonMonitor mapping is added. We only
    enforce this strictly for the CarbonMonitor_2026 section; CDIAC_2022 and
    EI_2024 contain legacy intermediate names that get further aggregated /
    deleted in ingest (e.g., "Macedonia" → Yugoslavia, "Macau" → China),
    so their alias targets are not necessarily final canonical names.
    """
    aliases_blob = json.loads(ALIASES_PATH.read_text(encoding="utf-8"))
    cm = aliases_blob["CarbonMonitor_2026"]
    failures = [
        f"  {raw!r} → {value!r} (not canonical)"
        for raw, value in cm.items()
        if not raw.startswith("_") and value not in canonical
    ]
    assert not failures, "\n" + "\n".join(failures)


def test_every_alias_key_is_unique_within_section():
    """Catch accidental duplicate keys (JSON allows them; pandas would lose data)."""
    text = ALIASES_PATH.read_text(encoding="utf-8")
    # Re-parse with object_pairs_hook to detect duplicates.
    duplicates: list[tuple[str, str]] = []

    def _hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        seen: set[str] = set()
        for k, _ in pairs:
            if k in seen:
                duplicates.append((k, "duplicate"))
            seen.add(k)
        return dict(pairs)

    json.loads(text, object_pairs_hook=_hook)
    assert not duplicates, f"duplicate keys: {duplicates}"


def test_carbon_monitor_aliases_match_known_renames(canonical: set[str]):
    """Spot-check the CarbonMonitor_2026 section against expected renames."""
    aliases = country_names.load_aliases("CarbonMonitor_2026")
    # The five known renames from CM names → canonical.
    assert aliases["Italy"] == "Italy (Including San Marino)"
    assert aliases["Russian Federation"] == "Russia"
    assert aliases["United States"] == "United States Of America"
    assert aliases["Croatia"] == "Yugoslavia"
    assert aliases["Slovenia"] == "Yugoslavia"
    # Comment keys filtered out.
    assert "_comment" not in aliases
    # All targets canonical.
    for v in aliases.values():
        assert v in canonical


def test_validate_names_strict_requires_full_set(canonical: set[str]):
    # Strict mode passes when names == canonical.
    country_names.validate_names("test", canonical, canonical, strict=True)
    # Strict mode fails when something is missing.
    partial = canonical - {next(iter(canonical))}
    with pytest.raises(ValueError, match="missing"):
        country_names.validate_names("test", partial, canonical, strict=True)


def test_validate_names_rejects_unknown(canonical: set[str]):
    with pytest.raises(ValueError, match="not in canonical"):
        country_names.validate_names(
            "test", canonical | {"Atlantis"}, canonical, strict=False,
        )
