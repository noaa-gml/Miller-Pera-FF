"""Country name resolution for the Miller-Pera FF pipeline.

Single source of truth: inputs/canonical_countries.csv  (189 names, GISS-code order)
Alias mappings:         inputs/country_aliases.json     (per-source rename dicts)
"""

import json
from pathlib import Path

_BASE = Path(__file__).parent
_CANONICAL_PATH = _BASE / 'inputs' / 'canonical_countries.csv'
_ALIASES_PATH = _BASE / 'inputs' / 'country_aliases.json'


def load_canonical() -> list[str]:
    """Load the 189 canonical country names in GISS-code order."""
    names = []
    with open(_CANONICAL_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                names.append(line)
    return names


def load_aliases(source_key: str) -> dict[str, str]:
    """Load the rename dict for a given source (e.g. 'CDIAC_2022')."""
    with open(_ALIASES_PATH) as f:
        all_aliases = json.load(f)
    if source_key not in all_aliases:
        raise KeyError(f"No alias section '{source_key}' in {_ALIASES_PATH}")
    return {k: v for k, v in all_aliases[source_key].items()
            if not k.startswith('_')}


def validate_names(source_label: str, names: set[str], canonical: set[str],
                   *, strict: bool = False):
    """Check that *names* are a subset of *canonical*.

    If strict=True, require exact match (for CDIAC which must produce all 189).
    Raises ValueError with the offending names on failure.
    """
    unmatched = names - canonical
    if unmatched:
        raise ValueError(
            f"{source_label}: {len(unmatched)} names not in canonical list: "
            f"{sorted(unmatched)[:15]}")
    if strict:
        missing = canonical - names
        if missing:
            raise ValueError(
                f"{source_label}: {len(missing)} canonical countries missing: "
                f"{sorted(missing)[:15]}")
