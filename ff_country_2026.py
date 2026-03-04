#!/usr/bin/env python
"""Generate monthly 1×1° gridded fossil-fuel CO₂ emissions.

Combines three data sources to produce a ``(months × lon × lat)`` field
in Gg C yr⁻¹:

1. **CDIAC country totals** (1993–2022): annual national emissions broken
   into gas, liquid, solid, flaring, and cement sectors.
2. **EI fractional changes** (2023–2024): year-over-year ratios that
   extrapolate each country × fuel-type forward from CDIAC's last year.
   Years beyond EI coverage are held flat.
3. **EDGAR v8.0 spatial patterns** (1×1°): within-country fractions that
   distribute each nation's total across grid cells.

After gridding, *bunker fuels* (global total minus sum-of-countries) are
distributed over ocean cells.  Annual grids are temporally interpolated
to monthly resolution via the Rasmussen (1991) piecewise integral-
preserving quadratic spline, then modulated by Blasing et al. seasonal
cycles.

Inputs  (relative to working directory)::

    processed_inputs/CDIAC_global_2020.csv
    processed_inputs/CDIAC_national_2020.csv
    processed_inputs/EI_frac_changes_2020-2024_{gas,oil,coal}.csv
    processed_inputs/EI_frac_changes_2020-2024_global_{gas,oil,coal}.csv
    processed_inputs/USGS_cement_ratios_2020-2026.csv
    processed_inputs/USGS_cement_2026.csv
    processed_inputs/fracarr_2026.npz
    inputs/COUNTRY1X1.CODE.mod2.2013.csv
    inputs/COUNTRY1X1.1993.mod.txt
    inputs/emis_mon_usatotal_2col.txt
    inputs/eurasian_seasff.txt

Output::

    outputs/ff_monthly_2026_py.npz   (ff_monthly, ff_time)
"""

from __future__ import annotations

import calendar
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Sector-column ordering (shared by CDIAC CSVs and all internal arrays
# whose last axis has size 6):
#   0 = total,  1 = gas,  2 = liquid/oil,  3 = solid/coal,
#   4 = flaring,  5 = cement
# ---------------------------------------------------------------------------
_TOTAL = 0
_GAS   = 1
_OIL   = 2
_COAL  = 3
_FLARE = 4
_CEM   = 5
_FUELS  = slice(_GAS, _COAL + 1)       # gas, oil, coal
_NONFUEL = slice(_FLARE, _CEM + 1)     # flaring, cement
_ALL     = slice(_GAS, _CEM + 1)       # gas … cement
_NSECTORS = 6

# EDGAR pattern indices (axis 3 of fracarr):
_PAT_COMBUST = 0   # gas, oil, coal
_PAT_FLARE   = 1   # flaring
_PAT_CEMENT  = 2   # cement

# Map emission sector → EDGAR pattern index
_SECTOR_PAT = [
    None,          # 0 = total (not distributed individually)
    _PAT_COMBUST,  # 1 = gas
    _PAT_COMBUST,  # 2 = oil
    _PAT_COMBUST,  # 3 = coal
    _PAT_FLARE,    # 4 = flaring
    _PAT_CEMENT,   # 5 = cement
]

# GISS country-code prefixes that require full 5-digit subdivision
# matching (Czech Republic, former USSR, St Kitts & Nevis, Yemen).
_SUBDIV_PREFIXES = {41, 137, 172, 179}

# Geographic regions for seasonal-cycle application: (lon_slice, lat_slice)
_REGIONS = {
    "nam":   (slice(40, 121),  slice(120, 151)),   # 60–140°W, 30–60°N
    "glb":   (slice(0, 360),   slice(0, 180)),
    "euras": (slice(160, 351), slice(120, 151)),   # 20°W–170°E, 30–60°N
}


# ═══════════════════════════════════════════════════════════════════════════════
# Helper functions
# ═══════════════════════════════════════════════════════════════════════════════

def _is_leap(year: int) -> int:
    """Return 1 if *year* is a leap year, else 0."""
    return int(calendar.isleap(year))


def _days_per_month(year: int) -> np.ndarray:
    """Return a length-12 int array of days per month for *year*."""
    return np.array([calendar.monthrange(year, m)[1] for m in range(1, 13)])


def _cumulative_extrap(
    base: np.ndarray,
    ratios: np.ndarray,
    n_extrap_yrs: int,
) -> np.ndarray:
    """Multiply *base* by *ratios* cumulatively, then hold flat.

    For each ratio year *i*, ``out[i] = out[i-1] * ratios[i]``
    (starting from *base*).  Years beyond ``len(ratios)`` repeat
    the last computed value.
    """
    out = np.zeros((n_extrap_yrs,) + np.shape(base))
    n_ei = len(ratios)
    for i in range(n_ei):
        out[i] = (base if i == 0 else out[i - 1]) * ratios[i]
    out[n_ei:] = out[n_ei - 1]
    return out


def _piqs(x: np.ndarray, ybar: np.ndarray) -> np.ndarray:
    """Piecewise Integral-preserving Quadratic Spline (Rasmussen 1991).

    For each segment *k* the quadratic is::

        f_k(t) = a_k · (t − x_k)² + b_k · (t − x_k) + c_k

    where *a*, *b*, *c* are chosen so that the integral of ``f_k`` over
    ``[x_k, x_{k+1}]`` equals ``ybar_k · Δx_k``, continuity and
    differentiability hold at every internal knot, and the global
    smoothness measure *S₂* is minimised (Eqn 15 of Rasmussen).

    The implementation is vectorised: arbitrary trailing dimensions of
    *ybar* (e.g. lon × lat) are processed in parallel.

    Parameters
    ----------
    x : array, shape ``(n+1,)``
        Segment boundaries (e.g. year edges 1993, 1994, …, 2026).
    ybar : array, shape ``(n, ...)``
        Mean value in each segment.

    Returns
    -------
    coefficients : array, shape ``(3, n, ...)``
        ``[0]`` = *a*, ``[1]`` = *b*, ``[2]`` = *c*.

    Reference
    ---------
    L. A. Rasmussen, "Piecewise Integral Splines of Low Degree",
    *Computers & Geosciences* 17(9), 1255–1263, 1991.
    """
    x = np.asarray(x, dtype=np.float64)
    ybar = np.asarray(ybar, dtype=np.float64)
    n = len(x) - 1                          # number of segments
    pixel_shape = ybar.shape[1:]
    ones = (1,) * len(pixel_shape)          # for broadcasting scalars

    if n == 1:
        out = np.zeros((3,) + ybar.shape)
        out[2] = ybar
        return out

    delta = np.diff(x)                      # segment widths, shape (n,)

    # ── Tridiagonal system  A · y_interior = g3 + g1·y[0] + g2·y[n] ────
    diag = np.array([2.0 * (delta[i] + delta[i + 1]) for i in range(n - 1)])
    off_up   = delta[:n - 2].copy()         # super-diagonal
    off_down = delta[2:n].copy()            # sub-diagonal

    g1 = np.zeros(n - 1);  g1[0]  = -delta[1]
    g2 = np.zeros(n - 1);  g2[-1] = -delta[-2]
    g3 = 3.0 * (delta[:n - 1].reshape(-1, *ones) * ybar[1:]
              + delta[1:n].reshape(-1, *ones)     * ybar[:-1])

    # Forward elimination (Thomas algorithm)
    for i in range(1, n - 1):
        m = off_down[i - 1] / diag[i - 1]
        diag[i] -= m * off_up[i - 1]
        g1[i]   -= m * g1[i - 1]
        g3[i]   -= m * g3[i - 1]

    # Back-substitution → f1·y[0] + f2·y[n] + f3 = y_interior
    f1 = np.zeros(n + 1);  f1[0] = 1.0
    f2 = np.zeros(n + 1);  f2[n] = 1.0
    f3 = np.zeros((n + 1,) + pixel_shape)

    f1[n - 1] = g1[-1] / diag[-1]
    f2[n - 1] = g2[-1] / diag[-1]
    f3[n - 1] = g3[-1] / diag[-1]
    for i in range(n - 2, 0, -1):
        f1[i] = (g1[i - 1] - off_up[i - 1] * f1[i + 1]) / diag[i - 1]
        f2[i] = (g2[i - 1] - off_up[i - 1] * f2[i + 1]) / diag[i - 1]
        f3[i] = (g3[i - 1] - off_up[i - 1] * f3[i + 1]) / diag[i - 1]

    # ── Smoothness condition (Rasmussen Eqn 15) ────────────────────────
    dinv3 = delta ** -3
    f1s = f1[:n] + f1[1:]
    f2s = f2[:n] + f2[1:]
    f3s = f3[:n] + f3[1:]
    resid = 2.0 * ybar - f3s

    rhs0 = (dinv3.reshape(-1, *ones) * f1s.reshape(-1, *ones) * resid).sum(axis=0)
    rhs1 = (dinv3.reshape(-1, *ones) * f2s.reshape(-1, *ones) * resid).sum(axis=0)

    R00 = (dinv3 * f1s ** 2).sum()
    R11 = (dinv3 * f2s ** 2).sum()
    R01 = (dinv3 * f1s * f2s).sum()
    det = R00 * R11 - R01 ** 2

    y_first = (R11 * rhs0 - R01 * rhs1) / det
    y_last  = (R00 * rhs1 - R01 * rhs0) / det

    # ── Recover all ordinates and polynomial coefficients ──────────────
    y_ord = np.zeros((n + 1,) + pixel_shape)
    y_ord[0] = y_first
    y_ord[n] = y_last
    for i in range(1, n):
        y_ord[i] = f1[i] * y_first + f2[i] * y_last + f3[i]

    dinv2 = (delta ** -2).reshape(-1, *ones)
    dinv1 = (delta ** -1).reshape(-1, *ones)

    a = dinv2 * ( 3 * y_ord[:n] + 3 * y_ord[1:] - 6 * ybar)
    b = dinv1 * (-4 * y_ord[:n] - 2 * y_ord[1:] + 6 * ybar)
    c = y_ord[:n].copy()

    return np.stack([a, b, c])               # (3, n, ...)


def _extract_seasonal_cycle(
    t: np.ndarray,
    y: np.ndarray,
    npoly: int = 3,
    nharm: int = 4,
) -> np.ndarray:
    """Extract the mean seasonal cycle via polynomial + harmonic fit.

    Mimics ``ccg_ccgvu`` from the NOAA/GML CCG library.  Returns the
    harmonic component evaluated at 12 monthly mid-points (centred on
    zero, same units as *y*).
    """
    n_basis = (npoly + 1) + 2 * nharm
    design = np.zeros((len(t), n_basis))
    for p in range(npoly + 1):
        design[:, p] = t ** p
    for h in range(nharm):
        col = npoly + 1 + 2 * h
        design[:, col]     = np.sin(2.0 * np.pi * (h + 1) * t)
        design[:, col + 1] = np.cos(2.0 * np.pi * (h + 1) * t)

    coef, *_ = np.linalg.lstsq(design, y, rcond=None)

    t12 = (np.arange(12) + 0.5) / 12.0
    sc = np.zeros(12)
    for h in range(nharm):
        col = npoly + 1 + 2 * h
        sc += coef[col]     * np.sin(2.0 * np.pi * (h + 1) * t12)
        sc += coef[col + 1] * np.cos(2.0 * np.pi * (h + 1) * t12)
    return sc


# ═══════════════════════════════════════════════════════════════════════════════
# Data-loading functions
# ═══════════════════════════════════════════════════════════════════════════════

def _load_cdiac_global(yr_start: int, yr_cdiac: int) -> np.ndarray:
    """Read CDIAC global totals for ``[yr_start … yr_cdiac]``.

    Returns shape ``(n_cdiac_yrs, 6)`` — cols: total, gas, liquid,
    solid, flaring, cement (Gg C).
    """
    df = pd.read_csv("processed_inputs/CDIAC_global_2020.csv")
    mask = (df.iloc[:, 0] >= yr_start) & (df.iloc[:, 0] <= yr_cdiac)
    return df.loc[mask].iloc[:, 1:7].values.astype(np.float64)


def _load_cdiac_national(
    yr_start: int,
    n_cdiac_yrs: int,
) -> tuple[np.ndarray, list[str], int]:
    """Read CDIAC country-level data from *yr_start* onward.

    Returns
    -------
    country_cdiac : ``(n_cdiac_yrs, n_countries, 6)``
    country_names : list of retained country names
    n_countries   : number of countries retained
    """
    df = pd.read_csv("processed_inputs/CDIAC_national_2020.csv")
    grouped = df.groupby(df.columns[0], sort=False)
    all_names = list(grouped.groups.keys())

    data = np.zeros((n_cdiac_yrs, len(all_names), _NSECTORS))
    valid = np.ones(len(all_names), dtype=bool)

    for i, (name, grp) in enumerate(grouped):
        sub = grp.loc[grp.iloc[:, 1] >= yr_start]
        if len(sub) == 0:
            data[:, i, :] = -999
            valid[i] = False
        else:
            assert len(sub) == n_cdiac_yrs, (
                f"{name}: expected {n_cdiac_yrs} rows, got {len(sub)}")
            data[:, i, :] = sub.iloc[:, 2:8].values

    data = data[:, valid, :]
    names = [n for n, v in zip(all_names, valid) if v]
    return data, names, len(names)


def _load_ei_country_ratios(
    yr_cdiac: int,
    yr_ei: int,
    n_ei_yrs: int,
    n_countries: int,
    fuels: list[str],
) -> np.ndarray:
    """Read EI year-over-year fractional changes per country and fuel.

    Returns shape ``(n_ei_yrs, n_countries, len(fuels))``.
    """
    year_cols = [str(yr) for yr in range(yr_cdiac + 1, yr_ei + 1)]
    ratios = np.zeros((n_ei_yrs, n_countries, len(fuels)))
    for fi, fuel in enumerate(fuels):
        df = pd.read_csv(
            f"processed_inputs/EI_frac_changes_2020-2024_{fuel}.csv")
        assert len(df) == n_countries, (
            f"EI {fuel}: {len(df)} rows, expected {n_countries}")
        ratios[:, :, fi] = df[year_cols].values.T
    return ratios


def _load_ei_global_ratios(
    n_ei_yrs: int,
    fuels: list[str],
) -> np.ndarray:
    """Read EI global year-over-year ratios for each fuel.

    Returns shape ``(n_ei_yrs, len(fuels))``.
    """
    result = np.zeros((n_ei_yrs, len(fuels)))
    for fi, fuel in enumerate(fuels):
        vals = np.loadtxt(
            f"processed_inputs/EI_frac_changes_2020-2024_global_{fuel}.csv",
            skiprows=1)
        result[:, fi] = vals[-n_ei_yrs:]
    return result


def _load_cement_ratios(
    yr_cdiac: int,
    yr_final: int,
    country_names: list[str],
) -> tuple[np.ndarray, np.ndarray]:
    """Read per-country and global year-over-year cement ratios from USGS.

    Reads the cumulative-relative-to-2020 ratios produced by ingestion,
    converts to year-over-year, and aligns to the CDIAC country order.
    Covers ``yr_cdiac`` through ``yr_final`` (USGS data extends beyond EI).

    Returns
    -------
    country_ratios : ``(n_cement_yrs, n_countries)`` year-over-year multipliers.
    global_ratios  : ``(n_cement_yrs,)`` year-over-year multipliers (world total).
    """
    years = list(range(yr_cdiac, yr_final + 1))      # [2021, 2022, ..., 2025]

    # --- per-country ratios (cumulative relative to 2020) ---
    df = pd.read_csv("processed_inputs/USGS_cement_ratios_2020-2026.csv")
    wide = df.pivot(index="Nation", columns="Year", values="cement")
    wide = wide.reindex(columns=years).reindex(country_names)
    cumul = wide.values                              # (n_countries, 5)
    yoy = cumul[:, 1:] / cumul[:, :-1]              # (n_countries, n_cement_yrs)
    country_ratios = yoy.T                           # (n_cement_yrs, n_countries)

    # --- global ratios from world totals ---
    full = pd.read_csv("processed_inputs/USGS_cement_2026.csv")
    world = (full[full["Nation"] == "World total (rounded)"]
             .set_index("Year")["Cement"])
    world_vals = world.reindex(years).values.astype(float)
    global_ratios = world_vals[1:] / world_vals[:-1] # (n_cement_yrs,)

    print(f"  {len(country_names)} countries, years {years}")
    print(f"  global cement yoy: {global_ratios}")
    return country_ratios, global_ratios


def _load_giss_map() -> tuple[np.ndarray, np.ndarray]:
    """Read the GISS 1×1° country-code map and code table.

    Returns
    -------
    gissmap   : ``(360, 180)`` int — country codes; 0 = ocean.
    codes_arr : ``(n_countries,)`` int — code for each country (ordered
                to match the CDIAC country list).
    """
    codes_df = pd.read_csv(
        "inputs/COUNTRY1X1.CODE.mod2.2013.csv",
        header=None, names=["code", "name"])
    codes_arr = codes_df["code"].values

    raw = np.loadtxt("inputs/COUNTRY1X1.1993.mod.txt", skiprows=3, dtype=int)
    gissmap = raw.reshape((180, 360)).T       # → (lon, lat)
    return gissmap, codes_arr


def _load_edgar_patterns(
    n_total_yrs: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Read sector-specific EDGAR 1×1° fractional emission patterns.

    Returns
    -------
    fracarr : ``(n_total_yrs, 360, 180, 3)``
        Sector patterns: axis 3 is [combustion, flaring, cement].
    fracarr_totals : ``(n_total_yrs, 360, 180)``
        TOTALS pattern (used for bunker-fuel distribution).
    """
    data = np.load("processed_inputs/fracarr_2026.npz")
    fracarr = data["fracarr"].transpose((2, 0, 1, 3))  # (180,360,yrs,3) → (yrs,180,360,3)
    # Swap lat/lon: file is (180,360) = (lat,lon), we need (360,180) = (lon,lat)
    fracarr = fracarr.transpose((0, 2, 1, 3))           # (yrs,360,180,3)
    assert fracarr.shape == (n_total_yrs, 360, 180, 3), \
        f"fracarr shape {fracarr.shape} != expected ({n_total_yrs}, 360, 180, 3)"
    totals = data["totals"].transpose((2, 0, 1))         # (180,360,yrs) → (yrs,180,360)
    totals = totals.transpose((0, 2, 1))                  # (yrs,360,180)
    assert totals.shape == (n_total_yrs, 360, 180)
    return fracarr, totals


# ═══════════════════════════════════════════════════════════════════════════════
# Processing functions
# ═══════════════════════════════════════════════════════════════════════════════

def _extrapolate_countries(
    country_cdiac: np.ndarray,
    ei_ratios: np.ndarray,
    frac_inc_flare: np.ndarray,
    frac_inc_cement: np.ndarray,
    n_extrap_yrs: int,
) -> np.ndarray:
    """Extrapolate country totals beyond CDIAC using EI and USGS ratios.

    - Gas / oil / coal use per-country EI ratios (held flat beyond EI).
    - Flaring uses *global* EI ratios (held flat beyond EI).
    - Cement uses per-country USGS ratios (may cover more years than EI).

    Returns ``(n_total_yrs, n_countries, 6)`` — the CDIAC years
    concatenated with the extrapolated years.
    """
    n_countries = country_cdiac.shape[1]
    n_ei_yrs = ei_ratios.shape[0]
    base = country_cdiac[-1, :, :]                     # (n_countries, 6)

    addarr = np.zeros((n_extrap_yrs, n_countries, _NSECTORS))

    # Fuels + flaring: EI ratios (3 years, held flat for remaining)
    fuel_flare_ratios = np.zeros((n_ei_yrs, n_countries, 4))
    fuel_flare_ratios[:, :, 0:3] = ei_ratios
    fuel_flare_ratios[:, :, 3]   = frac_inc_flare[:, None]
    addarr[:, :, _GAS:_FLARE+1] = _cumulative_extrap(
        base[:, _GAS:_FLARE+1], fuel_flare_ratios, n_extrap_yrs)

    # Cement: USGS ratios (may span all extrap years)
    addarr[:, :, _CEM] = _cumulative_extrap(
        base[:, _CEM], frac_inc_cement, n_extrap_yrs)

    addarr[:, :, _TOTAL] = addarr[:, :, _ALL].sum(axis=2)

    print("  country extrap totals:", addarr[:, :, _TOTAL].sum(axis=1))
    return np.concatenate([country_cdiac, addarr], axis=0)


def _extrapolate_global(
    glob_cdiac: np.ndarray,
    ei_glob_ratios: np.ndarray,
    frac_inc_flare: np.ndarray,
    frac_inc_cement: np.ndarray,
    n_cdiac_yrs: int,
    n_extrap_yrs: int,
) -> np.ndarray:
    """Extrapolate global totals from CDIAC through yr_final.

    Returns ``(n_total_yrs, 6)``.
    """
    addtot = np.zeros((n_extrap_yrs, _NSECTORS))
    base = glob_cdiac[n_cdiac_yrs - 1]

    addtot[:, _FUELS] = _cumulative_extrap(base[_FUELS], ei_glob_ratios, n_extrap_yrs)
    addtot[:, _CEM]   = _cumulative_extrap(base[_CEM],   frac_inc_cement, n_extrap_yrs)
    addtot[:, _FLARE] = _cumulative_extrap(base[_FLARE], frac_inc_flare,  n_extrap_yrs)
    addtot[:, _TOTAL] = addtot[:, _ALL].sum(axis=1)
    print("  global extrap totals:", addtot[:, _TOTAL])

    return np.concatenate([glob_cdiac, addtot], axis=0)


def _distribute_to_grid(
    country_all: np.ndarray,
    fracarr: np.ndarray,
    gissmap: np.ndarray,
    codes_arr: np.ndarray,
    n_total_yrs: int,
    yr_start: int,
) -> np.ndarray:
    """Map country totals onto the 1×1° grid using sector-specific EDGAR patterns.

    Each emission sector uses its own spatial pattern:
    gas/oil/coal → combustion pattern, flaring → PRO_FFF, cement → NMM.

    For countries whose GISS code requires subdivision matching (Czech,
    USSR, St Kitts & Nevis, Yemen), the full 5-digit code is used;
    otherwise only the leading 3-digit *region* code is matched.

    Within each country, emissions are distributed proportionally to the
    EDGAR pixel fractions.  If all EDGAR fractions are zero for a country
    in a given year, emissions are spread uniformly.

    Parameters
    ----------
    fracarr : ``(n_total_yrs, 360, 180, 3)``
        Sector patterns: axis 3 is [combustion, flaring, cement].

    Returns ``(n_total_yrs, 360, 180, 6)``.
    """
    n_countries = country_all.shape[1]
    flux = np.zeros((n_total_yrs, 360, 180, _NSECTORS))
    gissmap_coarse = gissmap // 100 * 100

    for yr_idx in range(n_total_yrs):
        for ci in range(n_countries):
            code = codes_arr[ci]
            match_map = gissmap if (code // 100 in _SUBDIV_PREFIXES) \
                        else gissmap_coarse

            cells = np.where(match_map == code)
            n_cells = len(cells[0])
            if n_cells == 0:
                continue

            emissions = country_all[yr_idx, ci, :]    # (6,)

            # Distribute each sector using its own EDGAR pattern
            for sec in range(_GAS, _CEM + 1):
                pat_idx = _SECTOR_PAT[sec]
                pat = fracarr[yr_idx, :, :, pat_idx]
                frac_sum = pat[cells].sum()

                if n_cells > 1 and frac_sum > 0:
                    weights = pat[cells] / frac_sum
                    flux[yr_idx, cells[0], cells[1], sec] = (
                        emissions[sec] * weights)
                elif frac_sum == 0:
                    flux[yr_idx, cells[0], cells[1], sec] = (
                        emissions[sec] / n_cells)
                else:
                    flux[yr_idx, cells[0][0], cells[1][0], sec] = (
                        emissions[sec])

            flux[yr_idx, cells[0], cells[1], _TOTAL] = (
                flux[yr_idx, cells[0], cells[1], _GAS:_CEM+1].sum(axis=-1))

        if (yr_idx + 1) % 10 == 0 or yr_idx == n_total_yrs - 1:
            print(f"  year {yr_start + yr_idx} done")

    return flux


def _add_bunker_fuels(
    flux_annual: np.ndarray,
    glob_all: np.ndarray,
    country_all: np.ndarray,
    fracarr_totals: np.ndarray,
    gissmap: np.ndarray,
    n_total_yrs: int,
) -> np.ndarray:
    """Add bunker fuels (global − Σ countries) to ocean grid cells.

    Bunker emissions are distributed across ocean cells proportionally
    to the EDGAR TOTALS pattern restricted to ocean (GISS code = 0).

    Returns ``(n_total_yrs, 360, 180)`` — total-sector only (the
    per-sector breakdown is not needed downstream).
    """
    ocean_mask = (gissmap == 0).astype(float)          # (360, 180)
    ocean_edgar = ocean_mask[None, :, :] * fracarr_totals  # (n_yrs, 360, 180)
    ocean_total = ocean_edgar.sum(axis=(1, 2))         # (n_yrs,)

    bunker = glob_all - country_all.sum(axis=1)        # (n_yrs, 6)

    bunker_grid = np.zeros_like(flux_annual)
    for i in range(n_total_yrs):
        if ocean_total[i] > 0:
            weights = ocean_edgar[i] / ocean_total[i]  # (360, 180)
            bunker_grid[i] = bunker[i] * weights[:, :, None]

    return (flux_annual + bunker_grid)[:, :, :, _TOTAL]


def _interpolate_to_monthly(
    flux_total: np.ndarray,
    yr_start: int,
    n_total_yrs: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Temporally interpolate annual grids → monthly via piqs.

    1. Fit the Rasmussen piqs spline to annual values at every pixel.
    2. Evaluate the spline at daily resolution for each year.
    3. If any day in a pixel-year goes negative (a spline artefact for
       rapidly declining emissions), replace with the constant annual
       value for that pixel-year.
    4. Bin daily values into monthly means.

    Returns
    -------
    ff_monthly : ``(n_total_yrs × 12, 360, 180)``
    ff_time    : ``(n_total_yrs × 12,)`` — mid-month decimal years.
    """
    year_edges = np.arange(n_total_yrs + 1) + yr_start

    # Spline coefficients for every pixel at once
    fit = _piqs(year_edges, flux_total)       # (3, n_total_yrs, 360, 180)

    n_months = n_total_yrs * 12
    ff_monthly = np.zeros((n_months, 360, 180))
    ff_time = (np.arange(n_months, dtype=np.float64) / 12.0
               + yr_start + 1.0 / 24.0)

    month_idx = 0
    for k in range(n_total_yrs):
        yr = yr_start + k
        n_days = 365 + _is_leap(yr)

        # Mid-day time stamps (decimal year)
        t_daily = (np.arange(n_days, dtype=np.float64) / n_days
                   + yr + 0.5 / n_days)
        dt = t_daily - year_edges[k]

        # Evaluate quadratic:  a·dt² + b·dt + c   (vectorised)
        daily = (dt[:, None, None] ** 2 * fit[0, k]
               + dt[:, None, None]       * fit[1, k]
               +                           fit[2, k])

        # Clamp: where any day is negative, use flat annual value
        neg_mask = np.any(daily < 0, axis=0)
        if np.any(neg_mask):
            daily[:, neg_mask] = flux_total[k, neg_mask]

        # Bin daily → monthly
        dpm = _days_per_month(yr)
        day0 = 0
        for m in range(12):
            day1 = day0 + dpm[m]
            ff_monthly[month_idx] = daily[day0:day1].sum(axis=0) / dpm[m]
            month_idx += 1
            day0 = day1

        if (k + 1) % 10 == 0 or k == n_total_yrs - 1:
            print(f"  year {yr} done")

    return ff_monthly, ff_time


def _apply_seasonality(
    ff_monthly: np.ndarray,
    n_total_yrs: int,
    season: str,
    seas2: str,
) -> None:
    """Modulate monthly emissions with Blasing et al. seasonal cycles.

    Operates **in-place** on *ff_monthly*.

    ``season="nam"``
        US-derived Blasing curve applied to North America
        (30–60°N, 60–140°W).
    ``season="glb"``
        Applied globally.

    If ``seas2="euras"`` (and season is not global), a separate
    EDGAR-based Eurasian seasonal curve is applied to
    30–60°N, 20°W–170°E.
    """
    monthff = np.loadtxt("inputs/emis_mon_usatotal_2col.txt")
    sc12 = _extract_seasonal_cycle(monthff[:, 0], monthff[:, 1])

    # Normalise as fractional perturbation around zero
    seasff = sc12 / monthff[12:24, 1].mean()
    seasff_tiled = np.tile(seasff, n_total_yrs)

    if season not in _REGIONS:
        raise ValueError(f"Unknown season={season!r}")
    lon_sl, lat_sl = _REGIONS[season]

    ff_monthly[:, lon_sl, lat_sl] *= (seasff_tiled[:, None, None] + 1)

    # Eurasian adjustment (Western-Europe proxy via EDGAR)
    if seas2 == "euras" and season != "glb":
        seasffa = np.loadtxt("inputs/eurasian_seasff.txt", skiprows=3)
        seasffa_tiled = np.tile(seasffa, n_total_yrs)
        eur_lon, eur_lat = _REGIONS["euras"]
        ff_monthly[:, eur_lon, eur_lat] *= seasffa_tiled[:, None, None]


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """Run the full pipeline: load → extrapolate → grid → interpolate → save."""

    # ── Configuration ────────────────────────────────────────────────────
    season   = "nam"
    seas2    = "euras"
    yr_start = 1993
    yr_cdiac = 2022
    yr_ei    = 2024
    yr_final = 2025

    n_ei_yrs     = yr_ei - yr_cdiac            # 3
    n_cdiac_yrs  = yr_cdiac - yr_start + 1     # 29
    n_extrap_yrs = yr_final - yr_cdiac         # 4
    n_total_yrs  = n_cdiac_yrs + n_extrap_yrs  # 33
    fuels        = ["gas", "oil", "coal"]

    # EI flaring volumes (BCM) — read from ingest output instead of hardcoding
    _flaring_bcm = pd.read_csv('processed_inputs/EI_flaring_bcm.csv', index_col='Year')
    flaring = _flaring_bcm.loc[yr_cdiac:yr_ei, 'BCM'].values
    assert len(flaring) == n_ei_yrs + 1, (
        f"Expected {n_ei_yrs + 1} flaring BCM values ({yr_cdiac}–{yr_ei}), got {len(flaring)}")
    frac_inc_flare  = flaring[1:] / flaring[:-1]

    # ── 1. Load CDIAC global totals ──────────────────────────────────────
    print("Reading CDIAC global totals …")
    glob_cdiac = _load_cdiac_global(yr_start, yr_cdiac)

    # ── 2. Load CDIAC national data ──────────────────────────────────────
    print("Reading CDIAC national data …")
    country_cdiac, country_names, n_countries = _load_cdiac_national(
        yr_start, n_cdiac_yrs)
    Path("outputs").mkdir(exist_ok=True)
    np.savetxt(f"processed_inputs/CDIAC_countries_{yr_cdiac}.txt",
               country_names, fmt="%s")
    print(f"  {n_countries} countries retained")

    # ── 3. Load EI country-level ratios ──────────────────────────────────
    print("Reading EI country ratios …")
    ei_ratios = _load_ei_country_ratios(
        yr_cdiac, yr_ei, n_ei_yrs, n_countries, fuels)

    # ── 3b. Load USGS cement ratios (per-country) ────────────────────────
    print("Reading USGS cement ratios …")
    frac_inc_cement, frac_inc_cement_global = _load_cement_ratios(
        yr_cdiac, yr_final, country_names)

    # ── 4. Load GISS country grid ────────────────────────────────────────
    print("Reading GISS country grid …")
    gissmap, codes_arr = _load_giss_map()

    # ── 5. Load EDGAR patterns (sector-specific) ─────────────────────────
    print("Reading EDGAR patterns …")
    fracarr, fracarr_totals = _load_edgar_patterns(n_total_yrs)

    # ── 6. Extrapolate countries through yr_final ────────────────────────
    print("Extrapolating country data …")
    country_all = _extrapolate_countries(
        country_cdiac, ei_ratios, frac_inc_flare, frac_inc_cement,
        n_extrap_yrs)

    # ── 7. Distribute country totals onto the 1×1° grid ─────────────────
    print("Distributing country emissions to grid …")
    flux_annual = _distribute_to_grid(
        country_all, fracarr, gissmap, codes_arr, n_total_yrs, yr_start)
    del fracarr

    # ── 8. Add bunker fuels over ocean ───────────────────────────────────
    print("Computing bunker fuels …")
    ei_glob_ratios = _load_ei_global_ratios(n_ei_yrs, fuels)
    glob_all = _extrapolate_global(
        glob_cdiac, ei_glob_ratios, frac_inc_flare, frac_inc_cement_global,
        n_cdiac_yrs, n_extrap_yrs)
    flux_total = _add_bunker_fuels(
        flux_annual, glob_all, country_all, fracarr_totals, gissmap,
        n_total_yrs)
    del flux_annual, fracarr_totals

    # ── 9. Interpolate annual → monthly via piqs ─────────────────────────
    print("Running piqs interpolation …")
    ff_monthly, ff_time = _interpolate_to_monthly(
        flux_total, yr_start, n_total_yrs)
    del flux_total

    # ── 10. Apply seasonality ────────────────────────────────────────────
    if season:
        print("Applying seasonality …")
        _apply_seasonality(ff_monthly, n_total_yrs, season, seas2)

    # ── 11. Save ─────────────────────────────────────────────────────────
    out_path = "outputs/ff_monthly_2026_py.npz"
    np.savez(out_path, ff_monthly=ff_monthly, ff_time=ff_time)
    print(f"Saved {out_path}")


if __name__ == "__main__":
    main()
