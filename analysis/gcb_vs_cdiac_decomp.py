"""Where the CDIAC-at-AppState minus GCB2024 global total difference comes from.

Stacked contribution of each emitting category to the annual global difference
(CDIAC at AppState - GCB 2024), MtC/yr.  Categories are as reported by each
source: coal/solid, oil/liquid, gas, cement, flaring, plus GCB's "Other"
(non-cement industrial process CO2), which has no CDIAC counterpart.
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
OUT = _os.path.join(_HERE, "gcb_vs_cdiac_decomp.png")
Y0, Y1 = 1993, 2022

CATS = ["coal", "oil", "gas", "cement", "flaring", "other"]
LABEL = {"coal": "Coal / solid", "oil": "Oil / liquid", "gas": "Gas", "cement": "Cement",
         "flaring": "Flaring", "other": 'GCB "Other" (no CDIAC counterpart)'}
COLOR = {"coal": "#2a78d6", "oil": "#eb6834", "gas": "#1baf7a", "cement": "#eda100",
         "flaring": "#e87ba4", "other": "#008300"}
SURF, INK, INK2, MUTED = "#fcfcfb", "#0b0b0b", "#52514e", "#b8b7b0"

ws = openpyxl.load_workbook(GCB_XLSX, read_only=True, data_only=True)["Fossil Emissions by Category"]
gcb = {r[0]: dict(zip(["total"] + CATS, r[1:8]))
       for r in ws.iter_rows(min_row=10, max_col=8, values_only=True)
       if isinstance(r[0], (int, float)) and r[0] >= 1959 and r[1] is not None}
ws = openpyxl.load_workbook(CDIAC_XLSX, read_only=True, data_only=True)["Sheet1"]
cd = {r[0]: dict(zip(["total"] + CATS[:5], r[1:7]))
      for r in ws.iter_rows(min_row=2, max_col=7, values_only=True)
      if isinstance(r[0], (int, float)) and r[0] >= Y0}

yrs = np.array([y for y in range(Y0, Y1 + 1) if y in gcb and y in cd])
diff = {c: np.array([(cd[y].get(c) or 0) - (gcb[y].get(c) or 0) for y in yrs], float) for c in CATS}
total = np.array([cd[y]["total"] - gcb[y]["total"] for y in yrs], float)

fig, ax = plt.subplots(figsize=(10, 5.6))
fig.patch.set_facecolor(SURF)
ax.set_facecolor(SURF)
ax.grid(True, axis="y", color=MUTED, lw=0.5, alpha=0.45)
ax.set_axisbelow(True)
for s in ("top", "right"):
    ax.spines[s].set_visible(False)
for s in ("left", "bottom"):
    ax.spines[s].set_color(MUTED)
ax.tick_params(colors=INK2, length=3)

pos = np.zeros_like(yrs, float)
neg = np.zeros_like(yrs, float)
for c in CATS:
    v = diff[c]
    up, dn = np.clip(v, 0, None), np.clip(v, None, 0)
    ax.bar(yrs, up, bottom=pos, color=COLOR[c], width=0.74, label=LABEL[c],
           edgecolor=SURF, linewidth=1.0)
    ax.bar(yrs, dn, bottom=neg, color=COLOR[c], width=0.74,
           edgecolor=SURF, linewidth=1.0)
    pos += up
    neg += dn

ax.axhline(0, color=INK2, lw=1.0)
ax.plot(yrs, total, color=INK, lw=2.0, marker="o", ms=4.5,
        markerfacecolor=INK, markeredgecolor=SURF, markeredgewidth=1.2,
        label="Net (CDIAC $-$ GCB total)", zorder=5)

ax.set_ylabel("CDIAC at AppState $-$ GCB 2024   (MtC yr$^{-1}$)", color=INK2, fontsize=10)
ax.set_xlabel("Year", color=INK2, fontsize=10)
ax.set_title("Where the CDIAC$-$GCB global total difference comes from",
             color=INK, fontsize=13, loc="left", pad=46)
ax.legend(frameon=False, fontsize=8.5, ncol=3, loc="lower left",
          bbox_to_anchor=(0.0, 1.005), labelcolor=INK2)
ax.set_xlim(yrs[0] - 0.7, yrs[-1] + 0.7)

fig.text(0.008, 0.005,
         "Bars sum to the net line. GCB's \"Other\" category and flaring are the persistent "
         "negative terms; the late-record swing is coal and gas.", color=INK2, fontsize=7.5)
fig.savefig(OUT, dpi=170, bbox_inches="tight", facecolor=SURF)
print("wrote", OUT)

print(f"\nmean contribution {yrs[0]}-{yrs[-1]} (MtC/yr), and {yrs[-1]} value:")
for c in CATS:
    print(f"  {LABEL[c]:34s} {diff[c].mean():+7.1f}   {yrs[-1]} {diff[c][-1]:+7.0f}")
print(f"  {'NET':34s} {total.mean():+7.1f}   {yrs[-1]} {total[-1]:+7.0f}")
