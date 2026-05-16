#!/usr/bin/env python3
"""compare_methods_2026b.py — Side-by-side comparison of v2026b methods.

Compares ``outputs/gml_ff_co2_2026b_assumed.nc`` against
``outputs/gml_ff_co2_2026b_cm_yearly.nc`` across global totals,
per-month 2026 partial year, top emitters, and spatial deltas.
Writes a short markdown summary and a 2x2 figure.

Usage:
    python compare_methods_2026b.py
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

from constants import C_MOLAR_MASS

OUT_DIR = Path("outputs")
ASSUMED_PATH = OUT_DIR / "gml_ff_co2_2026b_assumed.nc"
CM_PATH      = OUT_DIR / "gml_ff_co2_2026b_cm_yearly.nc"
SUMMARY_MD   = OUT_DIR / "v2026b_method_comparison.md"
FIGURE_PNG   = OUT_DIR / "v2026b_method_comparison.png"


def per_month_pgc(ds: xr.Dataset) -> np.ndarray:
    """Return PgC per month from mol m-2 s-1 fluxes + cell areas + month_lengths."""
    fi = ds["fossil_imp"]
    cell_area_m2 = ds["cell_areas"].values * 1e6   # km² → m²
    spm = ds["month_lengths"].values                # seconds/month
    total_mol = (fi * cell_area_m2).sum(dim=("lat", "lon")).values * spm
    return total_mol * C_MOLAR_MASS * 1e-15          # mol → g → PgC


def main() -> None:
    if not ASSUMED_PATH.exists():
        msg = f"Missing {ASSUMED_PATH}; run ff_country_2026.py + post_process_2026.py --method assumed first"
        raise FileNotFoundError(msg)
    if not CM_PATH.exists():
        msg = f"Missing {CM_PATH}; run ff_country_2026.py + post_process_2026.py --method cm_yearly first"
        raise FileNotFoundError(msg)

    print(f"Loading {ASSUMED_PATH.name} and {CM_PATH.name} ...")
    ds_a = xr.open_dataset(ASSUMED_PATH)
    ds_b = xr.open_dataset(CM_PATH)

    times = pd.DatetimeIndex(ds_a.time.values)
    yr = times.year
    mo = times.month

    # Per-month PgC
    pgc_a = per_month_pgc(ds_a)
    pgc_b = per_month_pgc(ds_b)

    # Annual aggregates
    df_ann = pd.DataFrame(
        {"assumed": pgc_a, "cm_yearly": pgc_b, "year": yr, "month": mo},
    ).groupby("year")[["assumed", "cm_yearly"]].sum()
    df_ann["delta"]   = df_ann["cm_yearly"] - df_ann["assumed"]
    df_ann["delta_%"] = 100 * df_ann["delta"] / df_ann["assumed"]

    # Per-month Q1 2026
    mask_2026 = yr == 2026
    df_2026 = pd.DataFrame({
        "month":     [f"2026-{m:02d}" for m in mo[mask_2026]],
        "assumed":   pgc_a[mask_2026],
        "cm_yearly": pgc_b[mask_2026],
    })
    df_2026["delta"]   = df_2026["cm_yearly"] - df_2026["assumed"]
    df_2026["delta_%"] = 100 * df_2026["delta"] / df_2026["assumed"]

    # Spatial RMS per month
    fa = ds_a["fossil_imp"].values
    fb = ds_b["fossil_imp"].values
    rms_per_month = np.sqrt(np.mean((fb - fa) ** 2, axis=(1, 2)))
    norm = np.sqrt(np.mean(fa ** 2, axis=(1, 2)))
    rel_rms = np.where(norm > 0, 100 * rms_per_month / norm, 0)

    # ── Markdown summary ────────────────────────────────────────────
    lines: list[str] = [
        "# v2026b — Method comparison: `assumed` vs `cm_yearly`",
        "",
        "Two annual baselines for the 2025 → 2026 step:",
        "",
        "* **assumed**: gas/oil +2.5%, coal/flaring +1% (per-fuel; same as 2025)",
        "* **cm_yearly**: per-country CM Q1-2026/Q1-2025 ratio applied uniformly across fuels",
        "",
        "Both methods produce the same Feb..Apr 2026 monthly shape "
        "(via `Jan_2026 × CM_monthly_ratio` overwrite).",
        "",
        "## Annual totals (PgC) — last 6 years",
        "",
        "| Year | assumed | cm_yearly | Δ (PgC) | Δ (%) |",
        "|------|---------|-----------|---------|-------|",
    ]
    for y in [2020, 2021, 2022, 2023, 2024, 2025, 2026]:
        if y not in df_ann.index:
            continue
        row = df_ann.loc[y]
        partial = " *(partial Jan..Apr)*" if y == 2026 else ""
        lines.append(
            f"| {y}{partial} | {row['assumed']:.4f} | {row['cm_yearly']:.4f} "
            f"| {row['delta']:+.4f} | {row['delta_%']:+.2f}% |",
        )

    lines += [
        "",
        "## Per-month 2026 (partial year)",
        "",
        "| Month | assumed | cm_yearly | Δ (PgC) | Δ (%) |",
        "|-------|---------|-----------|---------|-------|",
    ]
    for _, r in df_2026.iterrows():
        lines.append(
            f"| {r['month']} | {r['assumed']:.4f} | {r['cm_yearly']:.4f} "
            f"| {r['delta']:+.4f} | {r['delta_%']:+.2f}% |",
        )

    lines += [
        "",
        "## Spatial RMS difference per month (last 16 months)",
        "",
        "| Month | RMS (mol m⁻² s⁻¹) | rel-RMS (%) |",
        "|-------|---------------------|-------------|",
    ]
    last_16 = slice(-16, None)
    for t, r, rr in zip(times[last_16], rms_per_month[last_16], rel_rms[last_16],
                        strict=True):
        lines.append(f"| {str(t)[:7]} | {r:.3e} | {rr:.2f}% |")

    # ── Figure: 2x2 ─────────────────────────────────────────────────
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), constrained_layout=True)

    # (a) global annual totals
    ax = axes[0, 0]
    df_ann_full = df_ann.copy()
    # Mark partial year so the eye doesn't compare 4 months to 12
    full_mask = df_ann_full.index < 2026
    ax.plot(df_ann_full.index[full_mask], df_ann_full.loc[full_mask, "assumed"],
            "o-", label="assumed", lw=1.2, ms=3)
    ax.plot(df_ann_full.index[full_mask], df_ann_full.loc[full_mask, "cm_yearly"],
            "x--", label="cm_yearly", lw=1.2, ms=4)
    if 2026 in df_ann_full.index:
        ax.plot([2026], [df_ann_full.loc[2026, "assumed"]],   "o",
                color="C0", alpha=0.4, label="2026 (Jan–Apr only)")
        ax.plot([2026], [df_ann_full.loc[2026, "cm_yearly"]], "x",
                color="C1", alpha=0.4)
    ax.set_xlabel("Year")
    ax.set_ylabel("Global FF CO₂ (PgC/yr)")
    ax.set_title("(a) Annual global totals")
    ax.legend(loc="lower right", fontsize=9)
    ax.grid(alpha=0.3)

    # (b) per-month 2026 bars
    ax = axes[0, 1]
    x = np.arange(len(df_2026))
    width = 0.4
    ax.bar(x - width/2, df_2026["assumed"],   width=width, label="assumed",   color="C0")
    ax.bar(x + width/2, df_2026["cm_yearly"], width=width, label="cm_yearly", color="C1")
    ax.set_xticks(x)
    ax.set_xticklabels(df_2026["month"], rotation=45, ha="right")
    ax.set_ylabel("Monthly PgC")
    ax.set_title("(b) 2026 partial-year monthly totals (Jan..Apr)")
    ax.legend(loc="lower right", fontsize=9)
    ax.grid(alpha=0.3, axis="y")

    # (c) relative RMS difference per month, 2024..2026
    ax = axes[1, 0]
    last_36 = slice(-36, None)
    ax.plot(times[last_36], rel_rms[last_36], "k-", lw=1)
    ax.axvline(np.datetime64("2026-01-15"), color="r", ls="--", alpha=0.5,
               label="Jan 2026 (CM overwrite begins)")
    ax.set_ylabel("Relative RMS difference (%)")
    ax.set_title("(c) Spatial RMS(cm_yearly − assumed) / RMS(assumed)  —  last 3 years")
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(alpha=0.3)
    fig.autofmt_xdate(rotation=30)

    # (d) spatial difference map for March 2026 (last fully-overwritten month)
    ax = axes[1, 1]
    mar_idx = int(np.where((yr == 2026) & (mo == 3))[0][0])
    diff = (fb[mar_idx] - fa[mar_idx]) * 1e9   # nmol m-2 s-1 for readability
    vmax = float(np.percentile(np.abs(diff), 99))
    im = ax.imshow(diff, origin="lower", cmap="RdBu_r", vmin=-vmax, vmax=vmax,
                   extent=(-180, 180, -90, 90))
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("(d) March 2026 spatial difference  (cm_yearly − assumed,  nmol m⁻² s⁻¹)")
    fig.colorbar(im, ax=ax, shrink=0.7, label="Δ (nmol m⁻² s⁻¹)")
    ax.grid(alpha=0.2)

    fig.suptitle("v2026b method comparison", fontsize=14, y=1.01)
    fig.savefig(FIGURE_PNG, dpi=110, bbox_inches="tight")
    print(f"  wrote {FIGURE_PNG}")

    SUMMARY_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  wrote {SUMMARY_MD}")

    print()
    print("-" * 72)
    print("\n".join(lines[:30]))   # first ~30 lines as a teaser
    print("-" * 72)
    print(f"Full report → {SUMMARY_MD}")
    print(f"Figure      → {FIGURE_PNG}")


if __name__ == "__main__":
    main()
