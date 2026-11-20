"""Global gas-flaring CO2: EI (satellite volumes) vs GCB 2024 vs CDIAC at AppState.

EI    : 'CO2 from Flaring' sheet of EI-Stats-Review-ALL-data-2025.xlsx, MtCO2/yr,
        World Bank GGFR / VIIRS flared volumes x ~2.02 MtCO2 per bcm. 1975-2024.
GCB   : Global_Carbon_Budget_2024_v1.0-2.xlsx, 'Fossil Emissions by Category',
        Flaring column, MtC/yr, through 2022.
CDIAC : CDIAC at AppState global.1750_2022.xlsx, gas-flaring column, MtC/yr.
"""
import openpyxl
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import os as _os
_HERE = _os.path.dirname(_os.path.abspath(__file__))
_ROOT = _os.path.dirname(_HERE)
EI_XLSX = _os.path.join(_ROOT, "inputs/EI-Stats-Review-ALL-data-2025.xlsx")
GCB_XLSX = _os.path.join(_ROOT, "inputs", "Global_Carbon_Budget_2024_v1.0-2.xlsx")
CDIAC_XLSX = _os.path.join(_ROOT, "inputs", "31449082", "global.1750_2022.xlsx")
OUT = _os.path.join(_HERE, "flaring_three_way.png")

BLUE, ORANGE, INK, INK2, MUTED, SURF = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#b8b7b0", "#fcfcfb"

wb = openpyxl.load_workbook(EI_XLSX, read_only=True, data_only=True)
def ei_world(sheet):
    rows = list(wb[sheet].iter_rows(min_row=1, max_row=120, max_col=70, values_only=True))
    hdr = next(r for r in rows if len([c for c in r if isinstance(c, (int, float)) and 1960 <= c <= 2024]) > 10)
    yidx = {}
    for i, c in enumerate(hdr):
        if isinstance(c, (int, float)) and 1960 <= c <= 2024 and c not in yidx:
            yidx[c] = i
    row = next(r for r in rows if r[0] and str(r[0]).strip() in ("Total World", "World"))
    return {y: row[i] for y, i in yidx.items() if isinstance(row[i], (int, float))}

ei = {y: v / 3.664 for y, v in ei_world("CO2 from Flaring").items()}   # MtCO2 -> MtC
ws = openpyxl.load_workbook(GCB_XLSX, read_only=True, data_only=True)["Fossil Emissions by Category"]
gcb = {r[0]: r[6] for r in ws.iter_rows(min_row=10, max_col=8, values_only=True)
       if isinstance(r[0], (int, float)) and r[0] >= 1975}
ws = openpyxl.load_workbook(CDIAC_XLSX, read_only=True, data_only=True)["Sheet1"]
cd = {r[0]: r[6] for r in ws.iter_rows(min_row=2, max_col=7, values_only=True)
      if isinstance(r[0], (int, float)) and r[0] >= 1975}

ye = np.array(sorted(ei)); ve = np.array([ei[y] for y in ye])
yg = np.array(sorted(gcb)); vg = np.array([gcb[y] for y in yg])
yc = np.array(sorted(cd)); vc = np.array([cd[y] for y in yc])
common = np.array(sorted(set(ye) & set(yg) & set(cd)))
rg = np.array([gcb[y] / ei[y] for y in common])
rc = np.array([cd[y] / ei[y] for y in common])

fig, (ax, ax2) = plt.subplots(2, 1, figsize=(9.5, 6.8), sharex=True,
                              gridspec_kw={"height_ratios": [2.4, 1], "hspace": 0.12})
fig.patch.set_facecolor(SURF)
for a in (ax, ax2):
    a.set_facecolor(SURF)
    a.grid(True, color=MUTED, lw=0.5, alpha=0.45)
    a.set_axisbelow(True)
    for s in ("top", "right"):
        a.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        a.spines[s].set_color(MUTED)
    a.tick_params(colors=INK2, length=3)

ax.plot(ye, ve, color=INK, lw=2.2, label="EI / GGFR satellite volumes (benchmark)")
ax.plot(yg, vg, color=BLUE, lw=2.0, label="GCB 2024")
ax.plot(yc, vc, color=ORANGE, lw=2.0, label="CDIAC at AppState")
for x, y, txt, col in ((ye[-1], ve[-1], "EI / GGFR", INK2), (yg[-1], vg[-1], "GCB 2024", INK2),
                       (yc[-1], vc[-1], "CDIAC", INK2)):
    ax.annotate(txt, (x, y), xytext=(6, 0), textcoords="offset points", color=col,
                fontsize=9, va="center")
ax.set_ylabel("Flaring CO$_2$  (MtC yr$^{-1}$)", color=INK2, fontsize=10)
ax.set_title("Global gas-flaring CO$_2$: two inventories against satellite flare volumes",
             color=INK, fontsize=13, loc="left", pad=10)
ax.legend(frameon=False, loc="lower right", fontsize=9, labelcolor=INK2)
ax.set_xlim(1975, 2029)

ax2.axhline(1.0, color=INK, lw=1.4)
ax2.plot(common, rg, color=BLUE, lw=2.0)
ax2.plot(common, rc, color=ORANGE, lw=2.0)
ax2.annotate(f"GCB  {rg.mean():.2f}$\\times$ mean", (common[-1], rg[-1]), xytext=(6, 0),
             textcoords="offset points", color=INK2, fontsize=9, va="center")
ax2.annotate(f"CDIAC  {rc.mean():.2f}$\\times$", (common[-1], rc[-1]), xytext=(6, 0),
             textcoords="offset points", color=INK2, fontsize=9, va="center")
ax2.set_ylabel("ratio to EI / GGFR", color=INK2, fontsize=10)
ax2.set_xlabel("Year", color=INK2, fontsize=10)

fig.text(0.008, 0.012,
         "EI 'CO$_2$ from Flaring' = GGFR/VIIRS flared volumes $\\times$ ~2.02 MtCO$_2$ bcm$^{-1}$, converted to MtC. "
         "Satellite volumes capture unreported flares, so they are an upper-leaning benchmark.",
         color=INK2, fontsize=7.5)
fig.savefig(OUT, dpi=170, bbox_inches="tight", facecolor=SURF)
print("wrote", OUT)
print(f"1975-2022 mean ratio to EI: GCB {rg.mean():.2f}x, CDIAC {rc.mean():.2f}x")
print(f"2000-2022 mean ratio to EI: GCB {rg[common>=2000].mean():.2f}x, CDIAC {rc[common>=2000].mean():.2f}x")
