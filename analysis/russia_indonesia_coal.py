"""Coal CO2 for Russia and Indonesia: GCB vs CDIAC at AppState vs EI energy statistics.

These are the two countries that carry most of the residual GCB-CDIAC global coal
gap (China's coal is the identical series in both).  EI coal consumption is
converted with ONE global factor (world coal emissions / world coal consumption,
2000-2022 mean) so the country levels stay comparable across sources.
"""
import csv
import openpyxl
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OWID = "/tmp/owid.csv"
import os as _os
_HERE = _os.path.dirname(_os.path.abspath(__file__))
_ROOT = _os.path.dirname(_HERE)
CDIAC_XLSX = _os.path.join(_ROOT, "inputs", "31449082", "nation.1750_2022.xlsx")
EI_XLSX = _os.path.join(_ROOT, "inputs/EI-Stats-Review-ALL-data-2025.xlsx")
OUT = _os.path.join(_HERE, "russia_indonesia_coal.png")
Y0 = 1992

BLUE, ORANGE, INK, INK2, MUTED, SURF = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#b8b7b0", "#fcfcfb"
PAIRS = [("Russia", "Russia", "Russian Federation"),
         ("Indonesia", "Indonesia", "Indonesia")]

gcb = {}
with open(OWID) as f:
    for r in csv.DictReader(f):
        if r["year"] and r["coal_co2"]:
            gcb.setdefault(r["country"], {})[int(r["year"])] = float(r["coal_co2"]) / 3.664

wb = openpyxl.load_workbook(CDIAC_XLSX, read_only=True, data_only=True)
cd = {}
for r in wb["Sheet1"].iter_rows(min_row=2, max_col=8, values_only=True):
    if isinstance(r[2], (int, float)) and isinstance(r[4], (int, float)):
        cd.setdefault(r[0], {})[int(r[2])] = r[4] / 1000.0

wb = openpyxl.load_workbook(EI_XLSX, read_only=True, data_only=True)
rows = list(wb["Coal Consumption - EJ"].iter_rows(min_row=1, max_row=120, max_col=70, values_only=True))
hdr = next(r for r in rows if len([c for c in r if isinstance(c, (int, float)) and 1960 <= c <= 2024]) > 20)
yidx = {}
for i, c in enumerate(hdr):
    if isinstance(c, (int, float)) and 1960 <= c <= 2024 and c not in yidx:
        yidx[c] = i
def ei_series(name):
    row = next(r for r in rows if r[0] and str(r[0]).strip() == name)
    return {y: row[i] for y, i in yidx.items() if isinstance(row[i], (int, float))}

world_ej = ei_series("Total World")
EF = np.mean([gcb["World"][y] / world_ej[y] for y in range(2000, 2023)])   # MtC per EJ, global mean

fig, axes = plt.subplots(2, 2, figsize=(10.6, 7.2), sharex=True,
                         gridspec_kw={"height_ratios": [2.2, 1.1], "hspace": 0.13, "wspace": 0.22})
fig.patch.set_facecolor(SURF)
for a in axes.ravel():
    a.set_facecolor(SURF)
    a.grid(True, color=MUTED, lw=0.5, alpha=0.45)
    a.set_axisbelow(True)
    for s in ("top", "right"):
        a.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        a.spines[s].set_color(MUTED)
    a.tick_params(colors=INK2, length=3)

for col, (label, owid_name, cdiac_name) in enumerate(PAIRS):
    ax, ax2 = axes[0, col], axes[1, col]
    g, c = gcb[owid_name], cd[cdiac_name]
    e = {y: v * EF for y, v in ei_series(label if label == "Indonesia" else "Russian Federation").items()}
    for d, colr, lw, ls, lab in ((e, INK, 2.2, "-", f"EI consumption $\\times$ {EF:.1f} MtC EJ$^{{-1}}$"),
                                 (g, BLUE, 2.0, "-", "GCB (latest vintage)"),
                                 (c, ORANGE, 2.0, "-", "CDIAC at AppState")):
        x = np.array([y for y in sorted(d) if y >= Y0])
        ax.plot(x, [d[y] for y in x], color=colr, lw=lw, ls=ls, label=lab)
    ax.margins(y=0.10)
    ax.set_title(label, color=INK, fontsize=12, loc="left", pad=6)
    if col == 0:
        ax.set_ylabel("coal CO$_2$  (MtC yr$^{-1}$)", color=INK2, fontsize=10)
        ax.legend(frameon=False, fontsize=8.5, loc="lower left", labelcolor=INK2)

    common = [y for y in sorted(set(g) & set(c) & set(e)) if y >= Y0]
    ax2.axhline(1.0, color=INK, lw=1.4)
    ax2.plot(common, [g[y] / e[y] for y in common], color=BLUE, lw=2.0)
    ax2.plot(common, [c[y] / e[y] for y in common], color=ORANGE, lw=2.0)
    ax2.set_xlabel("Year", color=INK2, fontsize=10)
    ax2.set_xlim(Y0, 2025)
    if col == 0:
        ax2.set_ylabel("ratio to EI-implied", color=INK2, fontsize=10)
    r = {n: np.mean([d[y] / e[y] for y in common if y >= 2010])
         for n, d in (("GCB", g), ("CDIAC", c))}
    ax2.annotate(f"since 2010:  GCB {r['GCB']:.2f}$\\times$   CDIAC {r['CDIAC']:.2f}$\\times$",
                 xy=(0.02, 0.06), xycoords="axes fraction", color=INK2, fontsize=8.5)

fig.suptitle("Coal CO$_2$ where GCB and CDIAC actually disagree: Russia and Indonesia",
             color=INK, fontsize=13.5, x=0.008, ha="left", y=0.985)
fig.text(0.008, 0.008,
         "One global emission factor is applied to both countries' EI coal consumption, so the EI line carries no "
         "country-specific coal-quality adjustment; treat it as an approximate level check, not ground truth.",
         color=INK2, fontsize=7.5)
fig.savefig(OUT, dpi=170, bbox_inches="tight", facecolor=SURF)
print("wrote", OUT, f"| global EF {EF:.2f} MtC/EJ")
for label, owid_name, cdiac_name in PAIRS:
    g, c = gcb[owid_name], cd[cdiac_name]
    e = {y: v * EF for y, v in ei_series(label if label == "Indonesia" else "Russian Federation").items()}
    for y in (2010, 2019, 2022):
        print(f"  {label:10s} {y}: GCB {g[y]:6.1f}  CDIAC {c[y]:6.1f}  EI-implied {e[y]:6.1f}   "
              f"CDIAC/EI {c[y]/e[y]:.2f}  GCB/EI {g[y]/e[y]:.2f}")
