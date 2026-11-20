#!/usr/bin/env python3
"""compare_products.py — Compare Miller-Pera FF against independent gridded products.

Reference products (both regridded to the same 1° × 1° monthly grid, in
mol C m⁻² s⁻¹, so no unit or grid conversion happens here):

* **GCB GridFED** [Jones et al., 2021] — the Global Carbon Budget gridded
  fossil emissions dataset, as built by ``download_gridfed.py``
  (``config.GRIDFED_VERSION``; 0.1° summed to 1°). Compared as gross
  emissions, ``TOTAL − CEMENT_CARBONATION``.
* **OCO-2 FF** [Basu & Nassar, 2026] — ODIAC fossil-fuel fluxes convolved
  with TIMES temporal emission factors (the OCO-2 MIP prior), as built by
  ``orion/make_oco2ff_monthly.py`` + ``download_oco2ff.py``
  (``config.OCO2FF_VERSION``; monthly means of the hourly files on Orion).
  Published through Feb 2026; Mar–Apr 2026 are flagged ``extrapolated``
  (A. Jacobson's extension) and never enter the full-year statistics.

Inputs:  inputs/GridFED_<version>/GCP-GridFED<version>_1x1_YYYY.nc
         inputs/OCO2FF_<version>/oco2ff_<version>_1x1_YYYY.nc
         outputs/{OUTPUT_PREFIX}_<method>.nc
Outputs: outputs/product_comparison.md, outputs/product_comparison.png

Usage:
    python compare_products.py [--method assumed|cm_yearly]
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

from config import (
    CM_METHODS,
    GRIDFED_DIR,
    GRIDFED_FILE_TEMPLATE,
    GRIDFED_VERSION,
    OCO2FF_DIR,
    OCO2FF_FILE_TEMPLATE,
    OCO2FF_VERSION,
    OUTPUT_PREFIX,
    CMMethod,
)
from constants import C_MOLAR_MASS

if TYPE_CHECKING:
    from collections.abc import Callable

OUT_DIR = Path("outputs")
SUMMARY_MD = OUT_DIR / "product_comparison.md"
FIGURE_PNG = OUT_DIR / "product_comparison.png"

GRIDFED_LABEL = f"GridFED {GRIDFED_VERSION}"
OCO2FF_LABEL = f"OCO-2 FF {OCO2FF_VERSION}"

# Seasonal-cycle diagnostic: NH mid-latitude band and a stable window.
SEASONAL_LAT = (30.0, 60.0)
SEASONAL_YEARS = (2015, 2019)


# ═══════════════════════════════════════════════════════════════════════════════
# Loading
# ═══════════════════════════════════════════════════════════════════════════════


def load_gridfed() -> xr.DataArray:
    """GridFED gross emissions ``(time, lat, lon)`` in mol m⁻² s⁻¹.

    Reads the per-year 1° product written by ``download_gridfed.py``.
    The published ``TOTAL`` includes the negative cement-carbonation sink,
    which CDIAC / Miller-Pera do not count, so the comparable quantity is
    ``TOTAL − CEMENT_CARBONATION``.
    """
    files = sorted(Path(GRIDFED_DIR).glob(GRIDFED_FILE_TEMPLATE.format(year="*")))
    if not files:
        msg = f"No GridFED files under {GRIDFED_DIR}; run download_gridfed.py"
        raise FileNotFoundError(msg)
    arrays = []
    for f in files:
        with xr.open_dataset(f) as ds:
            arrays.append((ds["TOTAL"] - ds["CEMENT_CARBONATION"]).load())
    out = xr.concat(arrays, dim="time")
    out.name = "flux"
    return out.transpose("time", "lat", "lon")


def load_oco2ff() -> xr.DataArray:
    """OCO-2 FF monthly means ``(time, lat, lon)`` in mol C m⁻² s⁻¹.

    Reads the per-year 1° product built on Orion by
    ``orion/make_oco2ff_monthly.py`` and fetched by ``download_oco2ff.py``.
    Months flagged ``extrapolated`` (beyond the published record) are kept;
    they never enter the statistics, which use complete years only.
    """
    files = sorted(Path(OCO2FF_DIR).glob(OCO2FF_FILE_TEMPLATE.format(year="*")))
    if not files:
        msg = f"No OCO-2 FF files under {OCO2FF_DIR}; run download_oco2ff.py"
        raise FileNotFoundError(msg)
    arrays = []
    for f in files:
        with xr.open_dataset(f) as ds:
            arrays.append(ds["emission"].load())
    out = xr.concat(arrays, dim="time")
    out.name = "flux"
    return out.transpose("time", "lat", "lon")


PRODUCTS: dict[str, Callable[[], xr.DataArray]] = {
    GRIDFED_LABEL: load_gridfed,
    OCO2FF_LABEL: load_oco2ff,
}


def load_miller(method: CMMethod) -> tuple[xr.DataArray, np.ndarray]:
    """Return Miller fluxes ``(time, lat, lon)`` and cell areas (m²)."""
    path = OUT_DIR / f"{OUTPUT_PREFIX}_{method}.nc"
    if not path.exists():
        msg = f"Missing {path}; run ff_country.py + post_process.py --method {method} first"
        raise FileNotFoundError(msg)
    ds = xr.open_dataset(path)
    flux = ds["fossil_imp"].load()
    area_m2 = ds["cell_areas"].values * 1e6  # km² → m²
    ds.close()
    return flux, area_m2


# ═══════════════════════════════════════════════════════════════════════════════
# Metrics
# ═══════════════════════════════════════════════════════════════════════════════


def seconds_per_month(times: pd.DatetimeIndex) -> np.ndarray:
    return np.array([pd.Timestamp(t).days_in_month * 86400.0 for t in times])


def per_month_pgc(flux: xr.DataArray, area_m2: np.ndarray) -> pd.Series:
    """Global PgC per month from mol m⁻² s⁻¹ fluxes."""
    times = pd.DatetimeIndex(flux.time.values)
    total_mol_s = (flux * area_m2).sum(dim=("lat", "lon")).values
    pgc = total_mol_s * seconds_per_month(times) * C_MOLAR_MASS * 1e-15
    return pd.Series(pgc, index=times)


def annual_pgc(monthly: pd.Series) -> pd.Series:
    """Sum months into calendar years, keeping only complete (12-month) years."""
    grp = monthly.groupby(monthly.index.year)
    full = grp.count() == 12
    return grp.sum()[full]


def annual_mean_flux(flux: xr.DataArray) -> xr.DataArray:
    """Calendar-year mean flux ``(year, lat, lon)`` for complete years only."""
    counts = flux.time.dt.year.to_series().value_counts()
    full_years = sorted(counts[counts == 12].index)
    sel = flux.sel(time=flux.time.dt.year.isin(full_years))
    return sel.groupby("time.year").mean("time")


def spatial_stats(a: np.ndarray, b: np.ndarray) -> tuple[float, float, float]:
    """Cell-wise agreement of two 2-D fields: ``(r, r_log, rel_rms_%)``.

    ``r`` is the Pearson correlation of the raw fluxes — dominated by the few
    hundred point-source cells, so it is reported alongside ``r_log``, the
    correlation of log10 flux over cells emitting in both products, which
    weights the broad pattern. rel-RMS = RMS(b − a) / RMS(a).
    """
    av, bv = a.ravel(), b.ravel()
    r = float(np.corrcoef(av, bv)[0, 1])
    both = (av > 0) & (bv > 0)
    r_log = float(np.corrcoef(np.log10(av[both]), np.log10(bv[both]))[0, 1])
    rel_rms = 100.0 * float(np.sqrt(np.mean((bv - av) ** 2)) / np.sqrt(np.mean(av**2)))
    return r, r_log, rel_rms


def band_seasonal_cycle(flux: xr.DataArray, area_m2: np.ndarray) -> np.ndarray:
    """Mean monthly cycle (12 values, normalised to annual mean = 1) of the
    ``SEASONAL_LAT`` band over ``SEASONAL_YEARS``.
    """
    lo, hi = SEASONAL_LAT
    y0, y1 = SEASONAL_YEARS
    band = flux.sel(lat=slice(lo, hi), time=slice(f"{y0}-01-01", f"{y1}-12-31"))
    band_area = area_m2[(flux.lat.values >= lo) & (flux.lat.values <= hi), :]
    tot = (band * band_area).sum(dim=("lat", "lon"))
    cyc = tot.groupby("time.month").mean("time").values
    return cyc / cyc.mean()


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════


def main(method: CMMethod = "assumed") -> None:
    if method not in CM_METHODS:
        msg = f"Unknown method {method!r}; expected one of {CM_METHODS}"
        raise ValueError(msg)

    print(f"Loading Miller-Pera FF ({OUTPUT_PREFIX}_{method}.nc) ...")
    miller, area_m2 = load_miller(method)
    miller_label = f"Miller-Pera {OUTPUT_PREFIX.rsplit('_', 1)[-1]} ({method})"

    refs: dict[str, xr.DataArray] = {}
    for name, loader in PRODUCTS.items():
        print(f"Loading {name} ...")
        try:
            refs[name] = loader()
        except FileNotFoundError as exc:
            print(f"  SKIPPED: {exc}")
            continue
        assert np.allclose(refs[name].lat.values, miller.lat.values), f"{name}: lat grid differs"
        assert np.allclose(refs[name].lon.values, miller.lon.values), f"{name}: lon grid differs"
        print(
            f"  {refs[name].sizes['time']} months, "
            f"{str(refs[name].time.values[0])[:7]} → {str(refs[name].time.values[-1])[:7]}"
        )

    if not refs:
        msg = "No reference products found; run download_gridfed.py / download_oco2ff.py"
        raise FileNotFoundError(msg)

    # ── Global totals ───────────────────────────────────────────────
    monthly = {miller_label: per_month_pgc(miller, area_m2)}
    monthly.update({n: per_month_pgc(da, area_m2) for n, da in refs.items()})
    annual = pd.DataFrame({n: annual_pgc(s) for n, s in monthly.items()})
    annual.index.name = "year"

    # ── Spatial agreement on annual-mean fields ─────────────────────
    miller_ann = annual_mean_flux(miller)
    spatial: dict[str, pd.DataFrame] = {}
    for name, da in refs.items():
        ref_ann = annual_mean_flux(da)
        years = sorted(set(map(int, miller_ann.year.values)) & set(map(int, ref_ann.year.values)))
        rows = []
        for y in years:
            r, r_log, rel = spatial_stats(
                miller_ann.sel(year=y).values,
                ref_ann.sel(year=y).values,
            )
            rows.append({"year": y, "r": r, "r_log": r_log, "rel_rms_%": rel})
        spatial[name] = pd.DataFrame(rows).set_index("year")

    # ── Seasonal cycle, NH mid-latitudes ────────────────────────────
    cycles = {miller_label: band_seasonal_cycle(miller, area_m2)}
    cycles.update({n: band_seasonal_cycle(da, area_m2) for n, da in refs.items()})
    amplitude = {n: 100.0 * (c.max() - c.min()) for n, c in cycles.items()}  # % of annual mean

    # ── Markdown summary ────────────────────────────────────────────
    lines: list[str] = [
        f"# {miller_label} — comparison with GridFED and OCO-2 FF (ODIAC × TIMES)",
        "",
        "All three products on the same 1° × 1° monthly grid in mol C m⁻² s⁻¹; global",
        "totals use the Miller-Pera cell areas (spherical Earth, R = 6371.009 km).",
        "",
        f"* **GridFED {GRIDFED_VERSION}** — Global Carbon Budget gridded fossil emissions "
        "(Jones et al., 2021), 0.1° summed to 1° by `download_gridfed.py`; gross "
        "emissions (`TOTAL − CEMENT_CARBONATION`, i.e. excluding the cement "
        "carbonation sink, which CDIAC / Miller-Pera do not count).",
        f"* **OCO-2 FF {OCO2FF_VERSION}** — ODIAC fossil-fuel fluxes convolved with TIMES "
        "temporal factors (Basu & Nassar; the OCO-2 MIP prior), monthly means of the "
        "hourly files (`orion/make_oco2ff_monthly.py`); published through Feb 2026 "
        "(Mar–Apr 2026 are A. Jacobson's extrapolation, flagged `extrapolated`).",
        "",
        "## Annual global totals (PgC yr⁻¹)",
        "",
    ]
    ref_names = list(refs)
    header = (
        "| Year | "
        + " | ".join([miller_label, *ref_names])
        + " | "
        + " | ".join(f"Δ {n.split()[0]} (%)" for n in ref_names)
        + " |"
    )
    lines += [header, "|" + "---|" * (2 + 2 * len(ref_names))]
    for y, row in annual.iterrows():
        m = row[miller_label]
        vals = [f"{m:.3f}"] + [f"{row[n]:.3f}" if pd.notna(row[n]) else "—" for n in ref_names]
        deltas = [f"{100 * (row[n] - m) / m:+.1f}%" if pd.notna(row[n]) else "—" for n in ref_names]
        lines.append(f"| {y} | " + " | ".join(vals) + " | " + " | ".join(deltas) + " |")

    lines += [
        "",
        "### Summary over the overlap years",
        "",
        "| Product | Years | mean Δ (%) | range Δ (%) |",
        "|---|---|---|---|",
    ]
    summary: dict[str, dict[str, float | str]] = {}
    for n in ref_names:
        both = annual[[miller_label, n]].dropna()
        d = 100 * (both[n] - both[miller_label]) / both[miller_label]
        summary[n] = {
            "years": f"{both.index.min()}–{both.index.max()}",
            "mean": float(d.mean()),
            "min": float(d.min()),
            "max": float(d.max()),
        }
        lines.append(
            f"| {n} | {summary[n]['years']} | {d.mean():+.1f}% | {d.min():+.1f}% … {d.max():+.1f}% |"
        )

    lines += [
        "",
        "## Spatial agreement (annual-mean flux, all cells)",
        "",
        "Pearson correlation *r* across the 64 800 cells of the annual-mean flux;",
        "*r*(log) is the same on log₁₀ flux over cells emitting in both products (the",
        "linear *r* is dominated by the largest point-source cells); rel-RMS =",
        "RMS(product − Miller) / RMS(Miller).",
        "",
        "| Year | "
        + " | ".join(
            f"r ({n.split()[0]}) | r log ({n.split()[0]}) | rel-RMS ({n.split()[0]})"
            for n in ref_names
        )
        + " |",
        "|---|" + "---|---|---|" * len(ref_names),
    ]
    all_years = sorted(set().union(*(set(df.index) for df in spatial.values())))
    for y in all_years:
        cells = []
        for n in ref_names:
            if y in spatial[n].index:
                row = spatial[n].loc[y]
                cells.append(f"{row['r']:.3f} | {row['r_log']:.3f} | {row['rel_rms_%']:.0f}%")
            else:
                cells.append("— | — | —")
        lines.append(f"| {y} | " + " | ".join(cells) + " |")
    for n in ref_names:
        df = spatial[n]
        summary[n]["r_mean"] = float(df["r"].mean())
        summary[n]["r_min"] = float(df["r"].min())
        summary[n]["r_log_mean"] = float(df["r_log"].mean())
        summary[n]["rel_rms_mean"] = float(df["rel_rms_%"].mean())
    lines += [""] + [
        f"* **{n}**: mean r = {summary[n]['r_mean']:.3f} (min {summary[n]['r_min']:.3f}), "
        f"mean r(log) = {summary[n]['r_log_mean']:.3f}, "
        f"mean rel-RMS = {summary[n]['rel_rms_mean']:.0f}%"
        for n in ref_names
    ]

    lo, hi = SEASONAL_LAT
    y0, y1 = SEASONAL_YEARS
    lines += [
        "",
        f"## Seasonal cycle, {lo:.0f}–{hi:.0f}°N band, {y0}–{y1} mean",
        "",
        "Monthly total of the band normalised to its annual mean; amplitude = max − min.",
        "",
        "| Product | "
        + " | ".join(pd.Timestamp(2000, m, 1).strftime("%b") for m in range(1, 13))
        + " | amplitude |",
        "|---|" + "---|" * 13,
    ]
    for n, c in cycles.items():
        lines.append(f"| {n} | " + " | ".join(f"{v:.3f}" for v in c) + f" | {amplitude[n]:.1f}% |")

    # ── Figure: 2x2 ─────────────────────────────────────────────────
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), constrained_layout=True)
    ref_styles = [("C0", "s--"), ("C1", "^--"), ("C2", "v--")]
    styles = {miller_label: ("k", "o-")}
    styles.update({n: ref_styles[i] for i, n in enumerate(ref_names)})

    ax = axes[0, 0]
    for n in annual.columns:
        col, sty = styles[n]
        ax.plot(annual.index, annual[n], sty, color=col, label=n, lw=1.2, ms=3)
    ax.set_xlabel("Year")
    ax.set_ylabel("Global FF CO₂ (PgC yr⁻¹)")
    ax.set_title("(a) Annual global totals")
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)

    ax = axes[0, 1]
    for n in ref_names:
        col, sty = styles[n]
        d = 100 * (annual[n] - annual[miller_label]) / annual[miller_label]
        ax.plot(d.index, d, sty, color=col, label=n, lw=1.2, ms=3)
    ax.axhline(0, color="k", lw=0.8)
    ax.set_xlabel("Year")
    ax.set_ylabel("(product − Miller) / Miller  (%)")
    ax.set_title("(b) Annual-total difference from Miller-Pera")
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)

    ax = axes[1, 0]
    for n in ref_names:
        col, sty = styles[n]
        ax.plot(spatial[n].index, spatial[n]["r"], sty, color=col, label=f"r — {n}", lw=1.2, ms=3)
        ax.plot(
            spatial[n].index,
            spatial[n]["r_log"],
            ":",
            marker=sty[0],
            color=col,
            lw=1.2,
            ms=3,
            label=f"r (log₁₀ flux) — {n}",
        )
    ax.set_xlabel("Year")
    ax.set_ylabel("Pearson r across cells (annual-mean flux)")
    ax.set_ylim(0.5, 1.0)
    ax.set_title("(c) Spatial correlation with Miller-Pera (dashed: linear, dotted: log)")
    ax.legend(fontsize=8, loc="lower left")
    ax.grid(alpha=0.3)

    ax = axes[1, 1]
    months = np.arange(1, 13)
    for n, c in cycles.items():
        col, sty = styles[n]
        ax.plot(months, c, sty, color=col, label=f"{n}  (amp {amplitude[n]:.0f}%)", lw=1.2, ms=4)
    ax.axhline(1, color="k", lw=0.8, alpha=0.5)
    ax.set_xticks(months)
    ax.set_xticklabels([pd.Timestamp(2000, m, 1).strftime("%b") for m in months])
    ax.set_ylabel("Monthly / annual mean")
    ax.set_title(f"(d) Seasonal cycle, {lo:.0f}–{hi:.0f}°N, {y0}–{y1}")
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)

    fig.suptitle(f"{miller_label} vs. GridFED and OCO-2 FF", fontsize=14, y=1.01)
    fig.savefig(FIGURE_PNG, dpi=110, bbox_inches="tight")
    print(f"  wrote {FIGURE_PNG}")

    SUMMARY_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  wrote {SUMMARY_MD}")

    print()
    print("-" * 72)
    for n in ref_names:
        s = summary[n]
        print(
            f"{n}: {s['years']}  annual Δ mean {s['mean']:+.1f}% "
            f"[{s['min']:+.1f}%, {s['max']:+.1f}%]  spatial r {s['r_mean']:.3f} "
            f"(min {s['r_min']:.3f})  r(log) {s['r_log_mean']:.3f}  "
            f"rel-RMS {s['rel_rms_mean']:.0f}%"
        )
    print("seasonal amplitude 30–60°N: " + ", ".join(f"{n} {a:.1f}%" for n, a in amplitude.items()))
    print("-" * 72)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--method",
        choices=CM_METHODS,
        default="assumed",
        help="Which Miller-Pera monolithic to compare.",
    )
    main(method=parser.parse_args().method)
