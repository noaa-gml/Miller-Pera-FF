# OCO-2 FF v2026.2 — 1° × 1° monthly reference product

**Reference product only** — read by `compare_products.py`, never by the
pipeline. The `*.nc` files are gitignored (≈ 3 MB per year); fetch them with

```bash
python download_oco2ff.py            # rsync from Orion + verify MD5s + schema
python download_oco2ff.py --build    # (re)build on Orion first, then fetch
```

Source: Basu, S. & Nassar, R. (2026), *Fossil Fuel CO₂ Emissions for the
OCO2 Model Intercomparison Project (MIP)*, v2026.2,
[doi:10.5281/zenodo.19744619](https://doi.org/10.5281/zenodo.19744619) —
ODIAC 2024 (Oda et al., 2018) monthly fluxes disaggregated to hourly with
the TIMES day-of-week / time-of-day factors (Nassar et al., 2013); 2024
onward scaled from ODIAC 2023 by CarbonMonitor. Published through
**February 2026**, hourly, 1°, ~28 GB.

NOAA's copy is Andy Jacobson's CF-ised conversion on Orion,
`/work2/noaa/co2/input/FF/OCO2/v2026.2/CF/fossil_fuel_1x1_YYYYMMDD.nc`
(`emission` in mol C m⁻² s⁻¹; the Zenodo tarballs there are not group-readable).
That directory also holds his **March–April 2026 extrapolation** (`ncbo`:
2025 same-day + `demis_2026.nc`, the mean Jan–Feb 2026 − 2025 difference).

What the two scripts do:

- `orion/make_oco2ff_monthly.py` (runs on Orion, `config.ORION_PYTHON`):
  per year, averages every hourly field of each month (equal-weight hours,
  so the mean flux is exact, no unit change) and writes
  `oco2ff_v2026.2_1x1_<year>.nc` into `config.OCO2FF_ORION_DIR`
  (`/work2/noaa/co2/input/FF/OCO2/v2026.2.monthly_1x1/`) with
  `n_source_hours`, `extrapolated` (1 for the Mar–Apr 2026 extension,
  detected from the source files' `history`), `cell_area`, `month_seconds`,
  `time_bnds`, and global attributes recording the source directory, file
  count and the source files' own `history`/`source`. Then `checksums.md5`.
- `download_oco2ff.py` (local): `rsync`s the product over the `orion` ssh
  alias, verifies every file against `checksums.md5`, and checks the schema.

Statistics in `compare_products.py` use complete years only, so the
extrapolated months never enter them.
