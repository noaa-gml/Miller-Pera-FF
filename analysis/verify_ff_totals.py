"""Does the gridded Miller-Pera FF product integrate to the CDIAC global total?

Integrates outputs/yearly/gml_ff_co2_2026.YYYY.nc (fossil_imp, mol m-2 s-1) over
area and time to annual PgC, and compares with CDIAC at AppState global totals
(global.1750_2022.xlsx, MtC/yr).  CDIAC runs through 2022; 2023-2025 are the
product's own EI/USGS/assumed extrapolation and have no CDIAC counterpart.
"""
import glob
import os
import re
import numpy as np
import netCDF4 as nc
import openpyxl

import os as _os
_HERE = _os.path.dirname(_os.path.abspath(__file__))
_ROOT = _os.path.dirname(_HERE)
YEARLY = _os.path.join(_ROOT, "outputs/yearly/gml_ff_co2_2026.*.nc")
CDIAC_XLSX = _os.path.join(_ROOT, "inputs", "31449082", "global.1750_2022.xlsx")
R = 6371000.0
MC = 12.011

ws = openpyxl.load_workbook(CDIAC_XLSX, read_only=True, data_only=True)["Sheet1"]
cdiac = {int(r[0]): r[1] for r in ws.iter_rows(min_row=2, max_col=7, values_only=True)
         if isinstance(r[0], (int, float)) and isinstance(r[1], (int, float))}

def cell_area(lat, lon):
    dlon = np.deg2rad(abs(lon[1] - lon[0]))
    dlat = abs(lat[1] - lat[0])
    edges = np.deg2rad(np.clip(np.stack([lat - dlat / 2, lat + dlat / 2]), -90, 90))
    band = R ** 2 * dlon * (np.sin(edges[1]) - np.sin(edges[0]))   # m2 per cell, per lat
    return np.repeat(band[:, None], lon.size, axis=1)

print(f"{'year':>6} {'gridded PgC':>12} {'CDIAC PgC':>11} {'diff':>10} {'diff %':>8}")
rows = []
for f in sorted(glob.glob(YEARLY)):
    year = int(re.search(r"\.(\d{4})\.nc$", f).group(1))
    with nc.Dataset(f) as d:
        lat = d["lat"][:]; lon = d["lon"][:]
        flux = d["fossil_imp"][:]                       # (time, lat, lon) mol m-2 s-1
        t = d["time"][:]
    area = cell_area(np.asarray(lat), np.asarray(lon))
    # exact calendar month lengths (time stamps are mid-month)
    import calendar
    dates = nc.num2date(t, "days since 1970-01-01")
    secs = np.array([calendar.monthrange(d.year, d.month)[1] * 86400.0 for d in dates])
    total_mol = float(np.sum(flux * area[None, :, :] * secs[:, None, None]))
    pgc = total_mol * MC / 1e15
    c = cdiac.get(year)
    if c is None:
        print(f"{year:6d} {pgc:12.4f} {'--':>11} {'--':>10} {'--':>8}   (beyond CDIAC)")
    else:
        cp = c / 1000.0
        print(f"{year:6d} {pgc:12.4f} {cp:11.4f} {pgc-cp:+10.4f} {100*(pgc-cp)/cp:+7.3f}%")
        rows.append((year, pgc, cp))
a = np.array([[r[1], r[2]] for r in rows])
d = 100 * (a[:, 0] - a[:, 1]) / a[:, 1]
print(f"\n{len(rows)} years with CDIAC: mean {d.mean():+.3f}%, max |diff| {np.abs(d).max():.3f}% "
      f"in {rows[int(np.abs(d).argmax())][0]}")
