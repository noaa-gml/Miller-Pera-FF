"""Shared physical constants for the Miller-Pera FF pipeline.

Single source of truth for values that more than one pipeline stage needs.
Defining them once here removes the risk of the copies silently drifting
apart (they were previously duplicated as module-level literals in
``ingest.py``, ``post_process.py``, and ``compare_methods.py``).

These are *physical* constants — unlike the per-year configuration
(``STARTING_YEAR``, ``LAST_CDIAC_YEAR``, …) they never change when the
pipeline is re-run for a new year, so they do not belong in the
"edit these each year" config blocks of the individual scripts.
"""

from __future__ import annotations

# Earth radius (km) used for all 1°×1° cell-area calculations. John Miller's
# value; also written verbatim into the `earth_radius` variable of every
# output netCDF so downstream tools can reproduce the area weighting.
EARTH_RADIUS: float = 6371.009

# Molar mass of carbon (g/mol). Used in the Gg C → mol conversion chain
# (Gg C → g → mol via `1e9 / C_MOLAR_MASS`) and the reverse mol → PgC.
C_MOLAR_MASS: float = 12.011

# Molar mass of oxygen (g/mol); CO2 = C + 2 O. Used to convert reference
# products reported as kg CO2 (GridFED) into mol.
O_MOLAR_MASS: float = 15.999
CO2_MOLAR_MASS: float = C_MOLAR_MASS + 2 * O_MOLAR_MASS
