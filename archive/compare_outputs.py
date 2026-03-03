#!/usr/bin/env python
"""compare_outputs.py — Compare IDL and Python ff_country_2026 outputs.

Loads:
  outputs/ff_monthly_2026.sav     (from IDL ff_country_2026.pro)
  outputs/ff_monthly_2026_py.npz  (from Python ff_country_2026.py)

Reports max absolute and relative errors, spatial/temporal location of
worst disagreements, and per-year statistics.
"""

import numpy as np
from scipy.io import readsav

# ── Load IDL output ──────────────────────────────────────────────────────────
print("Loading IDL .sav …")
sav = readsav("outputs/ff_monthly_2026.sav")
# IDL arrays are column-major; scipy.io.readsav transposes to row-major.
# ff_monthly in IDL is [n_months, 360, 180]  (time, lon, lat)
# readsav gives us shape (180, 360, n_months) — reversed
idl_raw = sav["ff_monthly"]
idl_time = sav["ff_time"].squeeze()

# Transpose to (time, lon, lat) to match the Python convention
idl = idl_raw.transpose()  # → (n_months, 360, 180)
print(f"  IDL shape:    {idl.shape}   dtype={idl.dtype}")
print(f"  IDL time:     {idl_time[:3]} … {idl_time[-3:]}")

# ── Load Python output ───────────────────────────────────────────────────────
print("Loading Python .npz …")
pyd = np.load("outputs/ff_monthly_2026_py.npz")
py = pyd["ff_monthly"]          # (n_months, 360, 180)
py_time = pyd["ff_time"]

print(f"  Python shape: {py.shape}   dtype={py.dtype}")
print(f"  Python time:  {py_time[:3]} … {py_time[-3:]}")

# ── Basic shape check ────────────────────────────────────────────────────────
assert idl.shape == py.shape, f"Shape mismatch: IDL {idl.shape} vs Python {py.shape}"
assert len(idl_time) == len(py_time), "Time axis length mismatch"

# ── Time axis comparison ─────────────────────────────────────────────────────
time_diff = np.abs(idl_time - py_time)
print(f"\nTime axis max |diff|: {time_diff.max():.2e}")

# ── Field comparison ─────────────────────────────────────────────────────────
diff = idl - py
abs_diff = np.abs(diff)

# Overall statistics
print(f"\n{'='*60}")
print("Overall ff_monthly comparison")
print(f"{'='*60}")
print(f"  Max |IDL|:            {np.abs(idl).max():.6e}")
print(f"  Max |Python|:         {np.abs(py).max():.6e}")
print(f"  Max |diff|:           {abs_diff.max():.6e}")
print(f"  Mean |diff|:          {abs_diff.mean():.6e}")
print(f"  RMS diff:             {np.sqrt((diff**2).mean()):.6e}")

# Relative error (avoid division by zero)
denom = np.maximum(np.abs(idl), np.abs(py))
nonzero = denom > 0
if nonzero.any():
    rel = np.zeros_like(diff)
    rel[nonzero] = abs_diff[nonzero] / denom[nonzero]
    print(f"  Max |relative diff|:  {rel.max():.6e}")
    print(f"  Mean |relative diff|: {rel[nonzero].mean():.6e}")

    # Where is the worst relative error?
    worst = np.unravel_index(rel.argmax(), rel.shape)
    print(f"\n  Worst relative error at (month, lon, lat) = {worst}")
    print(f"    IDL value:    {idl[worst]:.10e}")
    print(f"    Python value: {py[worst]:.10e}")
    print(f"    Diff:         {diff[worst]:.10e}")

# Where is the worst absolute error?
worst_abs = np.unravel_index(abs_diff.argmax(), abs_diff.shape)
print(f"\n  Worst absolute error at (month, lon, lat) = {worst_abs}")
print(f"    IDL value:    {idl[worst_abs]:.10e}")
print(f"    Python value: {py[worst_abs]:.10e}")
print(f"    Diff:         {diff[worst_abs]:.10e}")

# ── Per-year statistics ──────────────────────────────────────────────────────
yr_start = 1993
n_total_yrs = idl.shape[0] // 12
print(f"\n{'='*60}")
print("Per-year max |diff| and max |relative diff|")
print(f"{'='*60}")
print(f"{'Year':>6s}  {'Max|diff|':>12s}  {'Max|rel|':>12s}  {'Global sum IDL':>16s}  {'Global sum Py':>16s}")
for yr in range(n_total_yrs):
    sl = slice(yr * 12, (yr + 1) * 12)
    yd = abs_diff[sl]
    yi = idl[sl]
    yp = py[sl]
    yd_max = yd.max()
    # relative
    ydenom = np.maximum(np.abs(yi), np.abs(yp))
    nz = ydenom > 0
    if nz.any():
        yr_rel = np.zeros_like(yd)
        yr_rel[nz] = yd[nz] / ydenom[nz]
        yr_rel_max = yr_rel.max()
    else:
        yr_rel_max = 0.0
    # annual sums (summing monthly means ≈ annual total / 12)
    sum_idl = yi.sum()
    sum_py = yp.sum()
    print(f"{yr_start + yr:6d}  {yd_max:12.4e}  {yr_rel_max:12.4e}  {sum_idl:16.4e}  {sum_py:16.4e}")

# ── Identical-pixel count ────────────────────────────────────────────────────
exact = np.sum(idl == py)
total = idl.size
print(f"\nExact matches:  {exact:,} / {total:,}  ({100*exact/total:.2f}%)")

# ── Threshold summary ────────────────────────────────────────────────────────
for thr in [1e-10, 1e-6, 1e-4, 1e-2, 1.0]:
    n = np.sum(abs_diff > thr)
    print(f"  |diff| > {thr:.0e}:  {n:>12,} cells ({100*n/total:.4f}%)")

print("\nDone.")
