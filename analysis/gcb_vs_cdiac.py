"""Annual global fossil-fuel CO2: GCB 2024 vs CDIAC at AppState.

GCB   : Global_Carbon_Budget_2024_v1.0-2.xlsx, 'Fossil Emissions by Category'
        ("fossil emissions excluding carbonation"), MtC/yr, 1959-2019.
CDIAC : CDIAC at AppState global.1750_2022.xlsx (Erb & Marland), MtC/yr,
        the total that Miller-Pera FF is scaled to.
"""
import openpyxl
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import os as _os
_HERE = _os.path.dirname(_os.path.abspath(__file__))
_ROOT = _os.path.dirname(_HERE)
GCB_XLSX = _os.path.join(_ROOT, "inputs", "Global_Carbon_Budget_2024_v1.0-2.xlsx")
CDIAC_XLSX = _os.path.join(_ROOT, "inputs", "31449082", "global.1750_2022.xlsx")
OUT = _os.path.join(_HERE, "gcb_vs_cdiac.png")

BLUE, ORANGE, INK, INK2, MUTED = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#b8b7b0"

ws = openpyxl.load_workbook(GCB_XLSX, read_only=True, data_only=True)["Fossil Emissions by Category"]
gcb = {r[0]: r[1] for r in ws.iter_rows(min_row=10, max_col=8, values_only=True)
       if isinstance(r[0], (int, float)) and r[0] >= 1959 and r[1] is not None}

ws = openpyxl.load_workbook(CDIAC_XLSX, read_only=True, data_only=True)["Sheet1"]
cdiac = {r[0]: r[1] for r in ws.iter_rows(min_row=2, max_col=7, values_only=True)
         if isinstance(r[0], (int, float)) and r[0] >= 1959 and r[1] is not None}

yr_g = np.array(sorted(gcb)); v_g = np.array([gcb[y] for y in yr_g], float)
yr_c = np.array(sorted(cdiac)); v_c = np.array([cdiac[y] for y in yr_c], float)
both = np.array(sorted(set(yr_g) & set(yr_c)))
d_abs = np.array([cdiac[y] - gcb[y] for y in both], float)
d_pct = 100.0 * d_abs / np.array([gcb[y] for y in both], float)

fig, (ax, ax2) = plt.subplots(2, 1, figsize=(9.5, 6.8), sharex=True,
                              gridspec_kw={"height_ratios": [2.6, 1], "hspace": 0.12})
fig.patch.set_facecolor("#fcfcfb")
for a in (ax, ax2):
    a.set_facecolor("#fcfcfb")
    a.grid(True, color=MUTED, lw=0.5, alpha=0.45)
    a.set_axisbelow(True)
    for s in ("top", "right"):
        a.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        a.spines[s].set_color(MUTED)
    a.tick_params(colors=INK2, length=3)

ax.plot(yr_g, v_g, color=BLUE, lw=2.0, label="GCB 2024 (fossil excl. carbonation)")
ax.plot(yr_c, v_c, color=ORANGE, lw=2.0, label="CDIAC at AppState (total FF + cement)")
ax.annotate("GCB 2024", (yr_g[-1], v_g[-1]), xytext=(6, -12), textcoords="offset points",
            color=INK2, fontsize=9, va="center")
ax.annotate("CDIAC at AppState", (yr_c[-1], v_c[-1]), xytext=(6, 4), textcoords="offset points",
            color=INK2, fontsize=9, va="center")
ax.set_ylabel("MtC yr$^{-1}$", color=INK2, fontsize=10)
ax.set_title("Global annual fossil-fuel CO$_2$: GCB 2024 vs CDIAC at AppState",
             color=INK, fontsize=13, loc="left", pad=10)
ax.legend(frameon=False, loc="upper left", fontsize=9, labelcolor=INK2)
ax.set_xlim(1959, 2024)

ax2.axhline(0, color=INK2, lw=1.0)
ax2.plot(both, d_pct, color=BLUE, lw=2.0)
ax2.fill_between(both, 0, d_pct, color=BLUE, alpha=0.12)
ax2.set_ylabel("CDIAC $-$ GCB  (%)", color=INK2, fontsize=10)
ax2.set_xlabel("Year", color=INK2, fontsize=10)
ax2.annotate(f"{d_pct[-1]:+.1f}% in {both[-1]}", (both[-1], d_pct[-1]), xytext=(6, 0),
             textcoords="offset points", color=INK2, fontsize=9, va="center")

fig.text(0.008, 0.012,
         "GCB: Friedlingstein et al. (2024), Global_Carbon_Budget_2024_v1.0-2.xlsx.  "
         "CDIAC at AppState: Erb & Marland, global.1750_2022.xlsx.  Both MtC yr$^{-1}$.",
         color=INK2, fontsize=7.5)
fig.savefig(OUT, dpi=170, bbox_inches="tight", facecolor=fig.get_facecolor())

print(f"wrote {OUT}")
print(f"overlap {both[0]}-{both[-1]}: mean diff {d_abs.mean():+.0f} MtC ({d_pct.mean():+.2f}%), "
      f"range {d_pct.min():+.2f}% to {d_pct.max():+.2f}%")
for y in (1993, 2000, 2010, 2015, 2019, 2021, 2022):
    if y in set(both):
        i = list(both).index(y)
        print(f"  {y}: GCB {gcb[y]:.0f}  CDIAC {cdiac[y]:.0f}  diff {d_abs[i]:+.0f} MtC ({d_pct[i]:+.2f}%)")
