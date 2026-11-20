#!/usr/bin/env python3
"""compare_national.py — Where Miller-Pera (CDIAC) and GridFED differ, by country and fuel.

GridFED's national totals are the Global Carbon Budget's fossil dataset, which
starts from CDIAC and corrects it (UNFCCC for Annex-I, extra categories).
Miller-Pera is CDIAC-at-AppState national totals on EDGAR patterns. This script
puts both on the same 1° country mask (the GISS ``COUNTRY1X1`` map the pipeline
itself grids with) and splits the global gap into countries and fuels.

Quantities, all TgC yr⁻¹ over the CDIAC years (1993–``LAST_CDIAC_YEAR``):

* **CDIAC**: ``processed_inputs/CDIAC_national_2020.csv`` (solid, liquid, gas,
  flaring, cement) and ``CDIAC_global_2020.csv``.
* **Miller-Pera**: ``outputs/{OUTPUT_PREFIX}_assumed.nc`` summed over each
  country's cells. Must reproduce CDIAC per country (gate below).
* **GridFED**: ``COAL, OIL, GAS, CEMENT`` summed over the same cells; bunkers
  and anything on cells the mask calls ocean are reported as their own rows.
  Gross = ``TOTAL − CEMENT_CARBONATION``. Fuel mapping to CDIAC: COAL ↔ solid,
  OIL ↔ liquid, GAS ↔ gas + flaring, CEMENT ↔ cement (as labelled; GridFED has
  no separate flaring or "other" field, so those land wherever GCB put them).

Gate: Miller-Pera summed on the mask must match CDIAC within 1 % for the ten
largest emitters in every year; if it does not, the mask is mis-oriented or
misaligned and every country number below would be wrong, so the script stops.

Inputs:  inputs/GridFED_v2025.1/GCP-GridFEDv2025.1_1x1_YYYY.nc (download_gridfed.py)
         outputs/gml_ff_co2_2026b_assumed.nc
         inputs/COUNTRY1X1.1993.mod.txt, inputs/COUNTRY1X1.CODE.mod2.2013.csv
         processed_inputs/CDIAC_{national,global}_2020.csv
Outputs: outputs/national_comparison.md, outputs/national_comparison.png

Usage:
    python compare_national.py
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from numpy.typing import NDArray
from scipy.ndimage import distance_transform_edt

from config import LAST_CDIAC_YEAR, OUTPUT_PREFIX, STARTING_YEAR
from constants import C_MOLAR_MASS

GRIDFED_VERSION = "v2025.1"
GRIDFED_DIR = Path(f"inputs/GridFED_{GRIDFED_VERSION}")
GRIDFED_FILE = "GCP-GridFED{v}_1x1_{year}.nc"
MILLER_NC = Path("outputs") / f"{OUTPUT_PREFIX}_assumed.nc"
CDIAC_NATIONAL = Path("processed_inputs/CDIAC_national_2020.csv")
CDIAC_GLOBAL = Path("processed_inputs/CDIAC_global_2020.csv")
COUNTRY_MAP = Path("inputs/COUNTRY1X1.1993.mod.txt")
COUNTRY_CODES = Path("inputs/COUNTRY1X1.CODE.mod2.2013.csv")
OUT_MD = Path("outputs/national_comparison.md")
# Ungridded GCB national table, the source of GridFED's national totals (one vintage
# older than GridFED v2025.1). Optional cross-check; gitignored (*.xlsx).
GCB_NATIONAL_XLSX = Path("inputs/National_Fossil_Carbon_Emissions_2024v1.0-2.xlsx")
GCB_NAMES = {"United States Of America": "USA", "Trinidad And Tobago": "Trinidad and Tobago"}
OUT_PNG = Path("outputs/national_comparison.png")

FUELS = ("COAL", "OIL", "GAS", "CEMENT")
CDIAC_FUEL = {
    "COAL": ["solid_fuel (Gg C)"],
    "OIL": ["liquid_fuel (Gg C)"],
    "GAS": ["gas_fuel (Gg C)", "flaring (Gg C)"],
    "CEMENT": ["cement (Gg C)"],
}
GATE_TOL = 0.01
GATE_TOP_N = 10
NORTH_AMERICA = ("United States Of America", "Canada", "Mexico")
TG_PER_MOL = C_MOLAR_MASS * 1e-12
Arr = NDArray[Any]
SUBDIV_PREFIXES = (41, 137, 172, 179)  # as ff_country._SUBDIV_PREFIXES


# ═══════════════════════════════════════════════════════════════════════════════
# Aggregation
# ═══════════════════════════════════════════════════════════════════════════════


def country_sums(field: Arr, cmap: Arr, codes: Arr) -> Arr:
    """Sum a ``(lat, lon)`` field over each code's cells; ``len(codes)`` values."""
    return np.array([field[cmap == c].sum() for c in codes])


def annual_tg(flux: Arr, area_m2: Arr, seconds: Arr) -> Arr:
    """Monthly ``(12, lat, lon)`` mol m⁻² s⁻¹ → annual TgC per cell ``(lat, lon)``."""
    return np.tensordot(seconds, flux, axes=1) * area_m2 * TG_PER_MOL


def load_mask() -> tuple[Arr, Arr, list[str]]:
    """GISS 1° country map ``(lat, lon)``, south row first, as ``ff_country.py`` matches it.

    The code file's row order is the CDIAC country order (``ff_country.py``).
    """
    fine = np.loadtxt(COUNTRY_MAP, skiprows=3, dtype=int).reshape(180, 360)
    # Cells carry province codes; a country owns its whole hundred-block unless it is
    # one of the subdivided entries, which match exactly (ff_country._SUBDIV_PREFIXES).
    cmap = np.where(np.isin(fine // 100, list(SUBDIV_PREFIXES)), fine, fine // 100 * 100)
    codes = pd.read_csv(COUNTRY_CODES, header=None, names=["code", "name"])["code"].to_numpy()
    names = list(pd.read_csv(CDIAC_NATIONAL)["Nation"].unique())
    if len(names) != len(codes):
        msg = f"{len(names)} CDIAC nations vs {len(codes)} mask codes"
        raise ValueError(msg)
    return cmap, codes, names


def nearest_owner(cmap: Arr, land: Arr) -> Arr:
    """Country code of each cell, with off-mask cells given their nearest mask cell's code.

    GridFED is built at 0.1°, so a 1° cell the GISS mask calls ocean can hold a
    coastal city or domestic shipping; Miller-Pera puts nothing national there.
    Grid-index distance on the 1° grid, longitude not wrapped (no emitter sits on
    the dateline).
    """
    _, (ii, jj) = distance_transform_edt(~land, return_indices=True)
    return cmap[ii, jj]


def build_tables(years: list[int]) -> dict[str, pd.DataFrame]:
    """Per-country, per-year TgC for Miller-Pera and GridFED fuels, plus global rows."""
    cmap, codes, names = load_mask()
    land = np.isin(cmap, codes)
    owner = nearest_owner(cmap, land)
    ds = xr.open_dataset(MILLER_NC)
    m_area = ds["cell_areas"].to_numpy() * 1e6
    m_time = pd.DatetimeIndex(ds["time_bnds"].to_numpy()[:, 0])  # no time coordinate in the file

    miller: dict[int, Arr] = {}
    gf: dict[str, dict[int, Arr]] = {f: {} for f in (*FUELS, "national", "national_onmask")}
    glob = []
    for y in years:
        sel = np.asarray(m_time.year == y)
        secs = np.array([t.days_in_month * 86400.0 for t in m_time[sel]])
        m_tg = annual_tg(ds["fossil_imp"].to_numpy()[sel], m_area, secs)
        miller[y] = country_sums(m_tg, cmap, codes)

        with xr.open_dataset(GRIDFED_DIR / GRIDFED_FILE.format(v=GRIDFED_VERSION, year=y)) as g:
            g_area, g_secs = g["cell_area"].to_numpy(), g["month_seconds"].to_numpy()
            per = {
                k: annual_tg(g[k].to_numpy().astype(float), g_area, g_secs)
                for k in (
                    *FUELS,
                    "BUNKER_AVIATION",
                    "BUNKER_SHIPPING",
                    "TOTAL",
                    "CEMENT_CARBONATION",
                )
            }
        national: Arr = np.sum([per[f] for f in FUELS], axis=0)
        for f in FUELS:
            gf[f][y] = country_sums(per[f], owner, codes)
        gf["national"][y] = country_sums(national, owner, codes)
        gf["national_onmask"][y] = country_sums(national, cmap, codes)
        bunkers = per["BUNKER_AVIATION"] + per["BUNKER_SHIPPING"]
        glob.append(
            {
                "year": y,
                "miller_global": m_tg.sum(),
                "miller_national": miller[y].sum(),
                "miller_ocean": m_tg[~land].sum(),
                "gridfed_gross": (per["TOTAL"] - per["CEMENT_CARBONATION"]).sum(),
                "gridfed_national": national.sum(),
                "gridfed_national_offmask": national[~land].sum(),
                "gridfed_bunkers": bunkers.sum(),
            }
        )
    ds.close()

    def frame(d: dict[int, Arr]) -> pd.DataFrame:
        return pd.DataFrame(d, index=names)

    out = {"miller": frame(miller), "glob": pd.DataFrame(glob).set_index("year")}
    for k, v in gf.items():
        out[f"gridfed_{k}"] = frame(v)
    return out


def cdiac_tables(years: list[int]) -> dict[str, pd.DataFrame]:
    """CDIAC national totals and fuels, TgC, ``(nation, year)``."""
    c = pd.read_csv(CDIAC_NATIONAL)
    c = c[c["Year"].isin(years)]
    out = {"total": c.pivot(index="Nation", columns="Year", values="total (Gg C)") / 1e3}
    for f, cols in CDIAC_FUEL.items():
        c[f] = c[cols].sum(axis=1)
        out[f] = c.pivot(index="Nation", columns="Year", values=f) / 1e3
    return out


def load_gcb(path: Path) -> pd.DataFrame | None:
    """GCB 'Territorial Emissions' sheet as ``(country, year)`` MtC = TgC, or None if absent."""
    if not path.exists():
        return None
    raw = pd.read_excel(path, sheet_name="Territorial Emissions", header=None)
    hdr = next(i for i in range(40) if "USA" in raw.iloc[i].astype(str).tolist())
    t = pd.read_excel(path, sheet_name="Territorial Emissions", header=hdr, index_col=0)
    years = pd.to_numeric(pd.Series(t.index), errors="coerce")
    t = t.loc[years.notna().to_numpy()]
    t.index = t.index.astype(int)
    return t.T


def gcb_value(gcb: pd.DataFrame | None, nation: str, year: int) -> float:
    if gcb is None:
        return float("nan")
    key = GCB_NAMES.get(nation, nation)
    if key not in gcb.index or year not in gcb.columns:
        return float("nan")
    return float(pd.to_numeric(gcb.loc[key, year]))


def gate(miller: pd.DataFrame, cdiac: pd.DataFrame) -> pd.Series:
    """Max |Miller-on-mask / CDIAC − 1| per top emitter; raises if any exceeds ``GATE_TOL``."""
    top = cdiac.mean(axis=1).nlargest(GATE_TOP_N).index
    err = (miller.loc[top, cdiac.columns] / cdiac.loc[top] - 1).abs().max(axis=1)
    if (err > GATE_TOL).any():
        msg = f"mask gate FAILED (tol {GATE_TOL:.0%}):\n{err[err > GATE_TOL]}"
        raise RuntimeError(msg)
    return err


# ═══════════════════════════════════════════════════════════════════════════════
# Report
# ═══════════════════════════════════════════════════════════════════════════════


def fmt(x: float, nd: int = 0) -> str:
    return f"{x:+.{nd}f}"


def write_report(
    t: dict[str, pd.DataFrame],
    c: dict[str, pd.DataFrame],
    err: pd.Series,
    years: list[int],
    gcb: pd.DataFrame | None,
) -> None:
    r = t["glob"]
    ylast, yfirst = years[-1], years[0]
    key = [y for y in (2000, 2010, ylast) if y in years]
    rows: list[tuple[str, Any]] = [(str(y), r.loc[y]) for y in key]
    rows.append((f"{yfirst}–{ylast} mean", r.mean()))
    fuel_gap = {f: t[f"gridfed_{f}"].sum() - c[f].sum() for f in FUELS}
    lines = [
        f"# Miller-Pera {OUTPUT_PREFIX.split('_')[-1]} (CDIAC) vs GridFED {GRIDFED_VERSION}, by country and fuel",
        "",
        "Generated by `compare_national.py`. TgC yr⁻¹. Miller-Pera is summed over the GISS 1° country "
        "mask the pipeline grids with; GridFED over the same mask, with every cell the mask calls ocean "
        "given to its nearest country (GridFED is built at 0.1°, so coastal cities and domestic shipping "
        "sit in cells the 1° mask calls ocean). GridFED gross = `TOTAL − CEMENT_CARBONATION`. Fuels: "
        "COAL ↔ CDIAC solid, OIL ↔ liquid, GAS ↔ gas + flaring, CEMENT ↔ cement.",
        "",
        f"Mask gate: Miller-Pera on the mask reproduces CDIAC within {err.max():.2%} for the "
        f"{GATE_TOP_N} largest emitters, every year {yfirst}–{ylast} (tolerance {GATE_TOL:.0%}).",
        "",
        "## The global gap, national vs bunkers",
        "",
        "| | Miller-Pera | GridFED gross | gap | national (GridFED − CDIAC) | bunkers (GridFED − Miller ocean cells) "
        "| residual |",
        "|---|---|---|---|---|---|---|",
    ]
    for label, row in rows:
        gap = row["gridfed_gross"] - row["miller_global"]
        nat = row["gridfed_national"] - row["miller_national"]
        bunk = row["gridfed_bunkers"] - row["miller_ocean"]
        lines.append(
            f"| {label} | {row['miller_global']:.0f} | {row['gridfed_gross']:.0f} | {fmt(gap)} "
            f"({gap / row['miller_global']:+.1%}) | {fmt(nat)} ({nat / row['miller_national']:+.1%}) | "
            f"{fmt(bunk)} | {fmt(gap - nat - bunk)} |"
        )
    lines += [
        "",
        "## Placement: GridFED national emissions on cells the 1° mask calls ocean",
        "",
        "Miller-Pera puts no national emission on these cells; the same mass sits on the nearest land "
        "cells instead. A spatial difference, not a total one.",
        "",
        "| | TgC yr⁻¹ | share of GridFED national |",
        "|---|---|---|",
    ]
    for label, row in rows:
        lines.append(
            f"| {label} | {row['gridfed_national_offmask']:.0f} | "
            f"{row['gridfed_national_offmask'] / row['gridfed_national']:.1%} |"
        )
    lines += [
        "",
        "## By fuel, all countries (GridFED − CDIAC)",
        "",
        "| | " + " | ".join(FUELS) + " | sum |",
        "|---|" + "---|" * (len(FUELS) + 1),
    ]
    for y in key:
        vals = [fuel_gap[f][y] for f in FUELS]
        lines.append(f"| {y} | " + " | ".join(fmt(v) for v in vals) + f" | {fmt(sum(vals))} |")
    vals = [fuel_gap[f].mean() for f in FUELS]
    lines.append(
        f"| {yfirst}–{ylast} mean | " + " | ".join(fmt(v) for v in vals) + f" | {fmt(sum(vals))} |"
    )
    d_last = t["gridfed_national"][ylast] - t["miller"][ylast]
    d_mean = (t["gridfed_national"] - t["miller"]).mean(axis=1)
    lines += [
        "",
        f"## Largest national differences, {ylast}",
        "",
        f"Ranked by |GridFED − Miller-Pera| in {ylast}; fuel columns are GridFED − CDIAC in {ylast}. "
        "At 1° a border cell belongs to one country, so small neighbours (e.g. the Baltics, Benelux) "
        "can trade mass with each other; read pairs of opposite sign across a border as one difference. "
        "The last column is the ungridded GCB 2024 national table against CDIAC, which has no mask at all.",
        "",
        f"| Country | Miller-Pera {ylast} | GridFED {ylast} | Δ {ylast} | Δ % | Δ mean {yfirst}–{ylast} | "
        + " | ".join(f"Δ {f}" for f in FUELS)
        + f" | GCB2024 table vs CDIAC {ylast} |",
        "|---|" + "---|" * (6 + len(FUELS)),
    ]
    for n in d_last.abs().nlargest(15).index:
        fuels = [t[f"gridfed_{f}"].loc[n, ylast] - c[f].loc[n, ylast] for f in FUELS]
        lines.append(
            f"| {n} | {t['miller'].loc[n, ylast]:.0f} | {t['gridfed_national'].loc[n, ylast]:.0f} | "
            f"{fmt(d_last[n])} | {d_last[n] / t['miller'].loc[n, ylast]:+.1%} | {fmt(d_mean[n])} | "
            + " | ".join(fmt(v) for v in fuels)
            + f" | {gcb_value(gcb, n, ylast) / c['total'].loc[n, ylast] - 1:+.1%} |"
        )
    na = list(NORTH_AMERICA)
    na_m, na_g = t["miller"].loc[na].sum(), t["gridfed_national"].loc[na].sum()
    lines += [
        "",
        "## North America (USA + Canada + Mexico)",
        "",
        "| | Miller-Pera | GridFED | Δ | Δ % | GCB2024 table vs CDIAC |",
        "|---|---|---|---|---|---|",
    ]
    for y in key:
        g_na = sum(gcb_value(gcb, n, y) for n in na)
        c_na = c["total"].loc[na, y].sum()
        lines.append(
            f"| {y} | {na_m[y]:.0f} | {na_g[y]:.0f} | {fmt(na_g[y] - na_m[y])} | "
            f"{(na_g[y] - na_m[y]) / na_m[y]:+.1%} | {g_na / c_na - 1:+.1%} |"
        )
    OUT_MD.write_text("\n".join(lines) + "\n")


def plot(t: dict[str, pd.DataFrame], c: dict[str, pd.DataFrame], years: list[int]) -> None:
    r = t["glob"]
    fig, (a, b) = plt.subplots(1, 2, figsize=(13, 5))
    parts = {f: (t[f"gridfed_{f}"].sum() - c[f].sum()).to_numpy() for f in FUELS}
    parts["bunkers"] = (r["gridfed_bunkers"] - r["miller_ocean"]).to_numpy()
    up, dn = np.zeros(len(years)), np.zeros(len(years))
    for k, v in parts.items():
        a.bar(years, v, bottom=np.where(v >= 0, up, dn), label=k)
        up += np.clip(v, 0, None)
        dn += np.clip(v, None, 0)
    a.plot(years, r["gridfed_gross"] - r["miller_global"], "k.-", label="total gap")
    a.axhline(0, color="0.5", lw=0.8)
    a.set_ylabel("GridFED − Miller-Pera (TgC yr⁻¹)")
    a.set_title(f"Global gap by fuel, GridFED {GRIDFED_VERSION} − CDIAC")
    a.legend(fontsize=8, loc="lower left")

    y = years[-1]
    d = t["gridfed_national"][y] - t["miller"][y]
    top = d.abs().nlargest(12).index[::-1]
    lp, ln = np.zeros(len(top)), np.zeros(len(top))
    for f in FUELS:
        v = (t[f"gridfed_{f}"].loc[top, y] - c[f].loc[top, y]).to_numpy()
        b.barh(range(len(top)), v, left=np.where(v >= 0, lp, ln), label=f)
        lp += np.clip(v, 0, None)
        ln += np.clip(v, None, 0)
    b.plot(d[top].to_numpy(), range(len(top)), "k|", ms=12, label="net")
    b.set_yticks(range(len(top)), [n[:22] for n in top], fontsize=8)
    b.axvline(0, color="0.5", lw=0.8)
    b.set_xlabel(f"GridFED − CDIAC, {y} (TgC yr⁻¹)")
    b.set_title(f"Largest national differences, {y}")
    b.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT_PNG, dpi=120)


def main() -> None:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    years = list(range(STARTING_YEAR, LAST_CDIAC_YEAR + 1))
    t = build_tables(years)
    c = cdiac_tables(years)
    err = gate(t["miller"], c["total"])
    write_report(t, c, err, years, load_gcb(GCB_NATIONAL_XLSX))
    plot(t, c, years)
    print(f"wrote {OUT_MD} and {OUT_PNG}; mask gate max error {err.max():.3%}")


if __name__ == "__main__":
    main()
