"""China coal CO2: GCB (latest vintage) vs CDIAC at AppState vs EI coal consumption.

GCB   : OWID mirror of the Global Carbon Budget national fuel-type dataset
        (Andrew & Peters), coal_co2, MtCO2 -> MtC. Latest vintage, through 2024.
CDIAC : CDIAC at AppState nation.1750_2022.xlsx, solid-fuel column, through 2022.
EI    : EI Statistical Review 2025, 'Coal Consumption - EJ', China, converted at a
        single implied factor (mean CDIAC/EI over 2000-2022) so the shapes are
        comparable; the level of this line is an emission-factor assumption, the
        growth rates in the lower panel are not.
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
OUT = _os.path.join(_HERE, "china_coal_three_way.png")
Y0 = 1990

BLUE, ORANGE, INK, INK2, MUTED, SURF = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#b8b7b0", "#fcfcfb"

gcb = {}
with open(OWID) as f:
    for r in csv.DictReader(f):
        if r["country"] == "China" and r["year"] and r["coal_co2"]:
            gcb[int(r["year"])] = float(r["coal_co2"]) / 3.664

wb = openpyxl.load_workbook(CDIAC_XLSX, read_only=True, data_only=True)
cd = {int(r[2]): r[4] / 1000.0 for r in wb["Sheet1"].iter_rows(min_row=2, max_col=8, values_only=True)
      if r[0] == "China" and isinstance(r[2], (int, float)) and isinstance(r[4], (int, float))}

wb = openpyxl.load_workbook(EI_XLSX, read_only=True, data_only=True)
rows = list(wb["Coal Consumption - EJ"].iter_rows(min_row=1, max_row=120, max_col=70, values_only=True))
hdr = next(r for r in rows if len([c for c in r if isinstance(c, (int, float)) and 1960 <= c <= 2024]) > 20)
yidx = {}
for i, c in enumerate(hdr):
    if isinstance(c, (int, float)) and 1960 <= c <= 2024 and c not in yidx:
        yidx[c] = i
row = next(r for r in rows if r[0] and str(r[0]).strip() == "China")
ei_ej = {y: row[i] for y, i in yidx.items() if isinstance(row[i], (int, float))}

cal = np.mean([cd[y] / ei_ej[y] for y in range(2000, 2023) if y in cd and y in ei_ej])
ei = {y: v * cal for y, v in ei_ej.items()}

yg = np.array([y for y in sorted(gcb) if y >= Y0]); vg = np.array([gcb[y] for y in yg])
yc = np.array([y for y in sorted(cd) if y >= Y0]); vc = np.array([cd[y] for y in yc])
ye = np.array([y for y in sorted(ei) if y >= Y0]); ve = np.array([ei[y] for y in ye])
ident = np.array([abs(cd[y] - gcb[y]) / cd[y] for y in yc if y in gcb]).max()

fig, (ax, ax2) = plt.subplots(2, 1, figsize=(9.8, 7.0), sharex=True,
                              gridspec_kw={"height_ratios": [2.3, 1.2], "hspace": 0.12})
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

ax.plot(ye, ve, color=INK, lw=2.2, label=f"EI coal consumption $\\times$ {cal:.1f} MtC EJ$^{{-1}}$")
ax.plot(yg, vg, color=BLUE, lw=4.0, alpha=0.9, solid_capstyle="round", label="GCB (latest vintage, via OWID)")
ax.plot(yc, vc, color=ORANGE, lw=1.8, ls=(0, (5, 3)), label="CDIAC at AppState")
ax.set_ylabel("China coal CO$_2$  (MtC yr$^{-1}$)", color=INK2, fontsize=10)
ax.set_title("China coal CO$_2$: GCB and CDIAC are the same series; EI is the only independent line",
             color=INK, fontsize=12.5, loc="left", pad=10)
ax.legend(frameon=False, loc="upper left", fontsize=9, labelcolor=INK2)
ax.set_xlim(Y0, 2027)
ax.annotate("GCB $-$ CDIAC: identical series\n(agree to $<$0.01% in every year)",
            xy=(2011, cd[2011]), xytext=(2001, 2050), color=INK2, fontsize=8.5,
            arrowprops=dict(arrowstyle="-", color=MUTED, lw=1))

def yoy(d):
    ys = [y for y in sorted(d) if y - 1 in d and y >= 2000]
    return np.array(ys), np.array([100 * (d[y] / d[y - 1] - 1) for y in ys])

ax2.axhline(0, color=MUTED, lw=1.0)
for d, col, lw, ls, lab in ((ei, INK, 2.0, "-", "EI"), (gcb, BLUE, 3.2, "-", "GCB"),
                            (cd, ORANGE, 1.6, (0, (5, 3)), "CDIAC")):
    x, y = yoy(d)
    ax2.plot(x, y, color=col, lw=lw, ls=ls, alpha=0.9)
ax2.set_ylabel("year-on-year change (%)", color=INK2, fontsize=10)
ax2.set_xlabel("Year", color=INK2, fontsize=10)
ax2.annotate("2022: inventories +5.8%,\nEI consumption +0.3%", xy=(2022, 5.8), xytext=(2014.2, 9.5),
             color=INK2, fontsize=8.5, arrowprops=dict(arrowstyle="->", color=INK2, lw=0.9))

fig.text(0.008, 0.010,
         "The EI line is scaled by one constant factor, so its level is not an independent check on emission factors "
         "— only its shape and growth rates are.", color=INK2, fontsize=7.5)
fig.savefig(OUT, dpi=170, bbox_inches="tight", facecolor=SURF)
print("wrote", OUT)
print(f"calibration {cal:.2f} MtC/EJ; max |GCB-CDIAC|/CDIAC = {100*ident:.3f}%")
for y in (2019, 2020, 2021, 2022, 2023, 2024):
    g = gcb.get(y); c = cd.get(y); e = ei.get(y)
    print(f"  {y}: GCB {g if g is None else round(g)}  CDIAC {c if c is None else round(c)}  EI-scaled {e if e is None else round(e)}")
