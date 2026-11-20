# GCB GridFED v2025.1 — 1° × 1° monthly reference product

**Reference product only** — read by `compare_products.py`, never by the
pipeline. The `*.nc` files are gitignored (≈ 9 MB per year); rebuild them
with

```bash
python download_gridfed.py            # 1993..GRIDFED_LAST_YEAR (config.py)
```

Source: Jones, M. W. et al. (2021), *Gridded fossil CO₂ emissions and related
O₂ combustion consistent with national inventories 1959–2018*, Sci. Data 8, 2,
[doi:10.1038/s41597-020-00779-6](https://doi.org/10.1038/s41597-020-00779-6).
Release v2025.1 (Nov 2025, 1959–2024): Zenodo
[10.5281/zenodo.17467681](https://doi.org/10.5281/zenodo.17467681) holds only
the 2024 file (Zenodo's size cap); the full set is served by UEA at
`https://opendap.uea.ac.uk/opendap/hyrax/greenocean/GridFED/GridFEDv2025.1/`
(the 2024 zip there is byte-identical to Zenodo's — MD5 `3bc876cd…`).

What `download_gridfed.py` does per year:

1. downloads `GCP-GridFEDv2025.1_<year>.zip` (~1.3 GB; one 8.7 GB netCDF
   inside) and appends its MD5 to `checksums.md5`;
2. reads the eight `CO2` sectors — `OIL, GAS, COAL, BUNKER_AVIATION,
   BUNKER_SHIPPING, CEMENT, CEMENT_CARBONATION, TOTAL` (kg CO₂ cell⁻¹
   month⁻¹ at 0.1°) — and sums each 10 × 10 block into the 1° cell
   (mass-conserving; asserted to 10⁻⁶);
3. converts to a flux, mol C m⁻² s⁻¹, with CO₂ = 44.009 g mol⁻¹, spherical
   cell areas (R = 6371.009 km, `cell_area` variable) and calendar month
   lengths (`month_seconds`), so mass is exactly recoverable;
4. writes `GCP-GridFEDv2025.1_1x1_<year>.nc` with the source URL, zip MD5,
   original global attributes, and pipeline commit as attributes; deletes the
   raw download (keep it with `--keep-raw`).

**`TOTAL` is as published and includes the negative `CEMENT_CARBONATION`
sink.** Gross emissions comparable to CDIAC / Miller-Pera are
`TOTAL − CEMENT_CARBONATION` (~0.22 PgC yr⁻¹ larger in 2024).
