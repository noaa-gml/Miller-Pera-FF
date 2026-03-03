# Miller Fossil Fuel CO₂ Prior Estimates

Gridded 1°×1° monthly fossil fuel CO₂ emission estimates for use as prior fluxes in CarbonTracker (CT). Combines national emissions inventories (CDIAC, Energy Institute, USGS cement) with EDGAR spatial patterns to produce a global, monthly, country-resolved product.

**Current version:** 2026 (covers 1993–2025)

**Repository:** <https://github.com/noaa-gml/miller-ff>

**Produced by:** `ingest_2026.py` → `ff_country_2026.py` → `post_process_2026.py` → `split_ct_2026.py`

## Files in This Directory

If you are reading this on the HPC or in a delivered output directory, the layout is:

```
./
├── README.md                       ← this file
├── split_ct_2026.py                ← script that produced the CT-format files below
├── flux1x1_ff.1993.nc              ← CarbonTracker-format, per-year (33 files)
├── flux1x1_ff.199301.nc            ← CarbonTracker-format, per-month (396 files)
├── ...
├── flux1x1_ff.2025.nc
├── flux1x1_ff.202512.nc
└── from_ash/
    └── ash_ff_2026.nc              ← monolithic source file (all years, all variables)
```

### CarbonTracker-Format Files (`flux1x1_ff.*.nc`)

| Pattern | Description |
|---|---|
| `flux1x1_ff.{YYYY}.nc` | Per-year CarbonTracker-format. Variable `fossil_imp` (date, lat, lon) with `decimal_date`, `date_components`, `date_bounds`. 33 files (1993–2025). |
| `flux1x1_ff.{YYYYMM}.nc` | Per-month CarbonTracker-format. Same variables, 396 files (one per month). |

CarbonTracker conventions:
- Time dimension named `date` (midpoint of each month)
- `date_bounds` (month start/end), `decimal_date` (leap-year aware fractional year)
- `date_components` / `calendar_components` (integer year, month, day, etc.)
- CarbonTracker global attributes (Notes, disclaimer, contact info)
- Only `fossil_imp` + coordinate bounds (no diagnostic variables)

### Monolithic Source File (`from_ash/ash_ff_2026.nc`)

All years in one file, used as input to `split_ct_2026.py`. Contains `fossil_imp` (mol/m²/s) and `fossil_imp_cell` (mol/cell/yr), 1°×1°, monthly 1993-01 through 2025-12.

### TM5/Fortran Per-Year Files (in the development repo)

The per-year TM5-format files (`ash_ff_2026.{YYYY}.nc`) are generated during production but are **not** included in this HPC directory. They live in the development repo under `outputs/yearly/`. If you need them, see the repository.

Fortran rc settings for `emission_co2_ff__Miller.F90`:
```
ff.input.dir      = <path to yearly/>
ff.ncfile.prefix  = ash_ff_2026
ff.ncfile.varname = fossil_imp
```

The NetCDF dimension order `(time, lat, lon)` maps to the Fortran column-major buffer `ff_input(nlon360, nlat180, 12)` = `(lon, lat, time)` as expected.

## Data Sources

| Source | What it provides | Where to get updates |
|---|---|---|
| **CDIAC at AppState** | National & global annual FF+cement emissions through 2021 | [rieee.appstate.edu/projects-programs/cdiac](https://rieee.appstate.edu/projects-programs/cdiac/) |
| **Energy Institute (EI) Statistical Review** | National oil/gas/coal consumption + flaring (EJ & TG CO₂) through 2024. Used to extrapolate CDIAC forward. | [energyinst.org/statistical-review](https://www.energyinst.org/statistical-review) — download the "All Data" xlsx |
| **EDGAR 2025 GHG** | Gridded 0.1°×0.1° CO₂ flux fields used for sector-specific spatial patterning within countries (TOTALS, NMM for cement, PRO_FFF for flaring) | [edgar.jrc.ec.europa.eu](https://edgar.jrc.ec.europa.eu/dataset_ghg2025) |
| **USGS Mineral Commodity Summaries — Cement** | National cement production for cement-specific extrapolation ratios | [usgs.gov/…/cement-statistics](https://www.usgs.gov/centers/national-minerals-information-center/cement-statistics-and-information) |
| **GISS Country Grid** | 1°×1° country assignment map (1993 boundaries) | [data.giss.nasa.gov/landuse/country.html](https://data.giss.nasa.gov/landuse/country.html) |

---

# Production Pipeline (Development Reference)

The sections below describe how to regenerate these files from source. The code lives at <https://github.com/noaa-gml/miller-ff>.

The workflow has 5 steps, run in order:

```
extrapolate_edgar.py  →  ingest_2026.py  →  ff_country_2026.py  →  post_process_2026.py  →  split_ct_2026.py  →  verify_2026.ipynb
     (optional)            (Python)            (Python)               (Python)               (Python)              (Python)
```

### Step 0: Extrapolate EDGAR (optional)

**`extrapolate_edgar.py`** — Only needed when EDGAR doesn't cover the final year. Runs over all three sector directories (TOTALS, NMM, PRO_FFF). For each sector it auto-detects the latest real file, estimates a growth rate from the last 3 real years of data, and writes "FAKE" placeholder files through `LAST_PIPELINE_YEAR`. Skips years that already have a FAKE file. Includes read-back verification.

```bash
/opt/anaconda3/envs/p312/bin/python extrapolate_edgar.py
```

- **Inputs:** Latest non-FAKE file in each of `inputs/TOTALS_flx_nc_2025_GHG/`, `inputs/NMM_flx_nc_2025_GHG/`, `inputs/PRO_FFF_flx_nc_2025_GHG/` (auto-detected per sector)
- **Output:** `EDGAR_2025_GHG_CO2_{YYYY}_{SECTOR}_FAKE_flx.nc` in each sector directory, for each year beyond EDGAR coverage through `LAST_PIPELINE_YEAR`
- **Config:** `LAST_PIPELINE_YEAR` (default 2025), `N_YEARS_FOR_RATE` (default 3), `FALLBACK_RATE` (default 0.01)
- **When to skip:** If all three sector directories already have real or FAKE files through the final extrapolation year

### Step 1: Ingest & Process Input Data

**`ingest_2026.py`** — The main data-preparation script. Reads all raw inputs, harmonizes country names, computes fractional change ratios, regrids EDGAR 0.1° → 1°, and writes processed intermediates. Takes ~5 minutes (EDGAR regridding is the bottleneck).

```bash
/opt/anaconda3/envs/p312/bin/python ingest_2026.py
```

**Inputs read:**
- `inputs/CDIAC/global.1751_2021.xlsx` — global totals
- `inputs/CDIAC/nation.1751_2021.xlsx` — national totals
- `inputs/EI-Stats-Review-ALL-data-2025.xlsx` — EI oil/gas/coal/flaring
- `inputs/TOTALS_flx_nc_2025_GHG/*.nc` — EDGAR gridded fluxes (all sectors combined)
- `inputs/NMM_flx_nc_2025_GHG/*.nc` — EDGAR NMM sector (cement spatial pattern)
- `inputs/PRO_FFF_flx_nc_2025_GHG/*.nc` — EDGAR PRO_FFF sector (flaring spatial pattern)
- `inputs/USGS_cement/mcs????-cement.csv` — USGS cement CSVs
- `inputs/EI_2024_flaring_regions.json` — mapping of EI flaring region names → CDIAC countries
- `inputs/EI_2024_fuel_regions.json` — mapping of EI fuel region names → CDIAC countries
- `inputs/COUNTRY1X1.1993.mod.txt` — GISS country grid
- `inputs/COUNTRY1X1.CODE.mod2.2013.csv` — country code mapping

**Outputs written to `processed_inputs/`:**
| File | Description |
|---|---|
| `CDIAC_global_2020.csv` | Global annual totals (Gg C): total, gas, liquid, solid, flaring, cement |
| `CDIAC_national_2020.csv` | National annual totals (Gg C), same columns |
| `CDIAC_countries.csv` | Ordered list of country names |
| `EI_frac_changes_2020-2024_{fuel}.csv` | Year-over-year fractional changes per country (gas/oil/coal) |
| `EI_frac_changes_2020-2024_global_{fuel}.csv` | Global year-over-year fractional changes |
| `EI_national_2024.csv` | EI data mapped to CDIAC countries |
| `USGS_cement_2026.csv` | Merged USGS cement production data |
| `USGS_cement_ratios_2020-2026.csv` | Cement production ratios relative to 2020 |
| `fracarr_2026.npz` | Sector-specific normalized EDGAR spatial fractions (180×360×nyears×3: combustion/flaring/cement) + TOTALS pattern for bunker fuels |

### Step 2: Country-Level Gridding (Python)

**`ff_country_2026.py`** — The core gridding routine. Assigns national emission totals to 1°×1° grid cells using sector-specific EDGAR spatial patterns (combustion for gas/oil/coal, NMM for cement, PRO_FFF for flaring), extrapolates beyond CDIAC using EI fuel ratios and per-country USGS cement ratios, adds "bunker" emissions to ocean cells, interpolates annual→monthly via Rasmussen (1991) integral-preserving quadratic splines, and applies Blasing et al. seasonal cycles for N. America and Eurasia.

```bash
/opt/anaconda3/envs/p312/bin/python ff_country_2026.py
```

- **Key parameters** (configured near top of `main()`):
  - `yr_start = 1993` — first year
  - `yr_cdiac = 2021` — last CDIAC year
  - `yr_ei = 2024` — last EI year
  - `yr_final = 2025` — final extrapolation year
  - Seasonal cycles: N. America ("nam") and Eurasia ("euras")

- **Spatial distribution:** Each sector uses its own EDGAR pattern within each country:
  - Gas / oil / coal → combustion pattern (TOTALS minus NMM minus PRO_FFF)
  - Flaring → PRO_FFF (fuel exploitation) — concentrated in oil/gas-producing regions
  - Cement → NMM (non-metallic minerals) — concentrated at cement plants
  - Bunker fuels (global minus sum-of-countries) → TOTALS pattern over ocean cells

- **Extrapolation approach:**
  - Gas / oil / coal use per-country EI ratios (3 years: 2022–2024, held flat for 2025)
  - Flaring uses global EI ratios (held flat for 2025)
  - Cement uses per-country USGS ratios (4 years: 2022–2025, not held flat — USGS data extends beyond EI)
  - Global totals are extrapolated independently using the same source ratios

- **Inputs:** All files from `processed_inputs/` (CDIAC, EI, USGS cement ratios, EDGAR spatial patterns)
- **Output:** `outputs/ff_monthly_2026_py.npz`

### Step 3: Post-Process to netCDF

**`post_process_2026.py`** — Loads the `.npz` output from Step 2, converts units (Gg C → mol/m²/s), cross-validates the conversion against an independent pint-based computation, and writes:

1. **Per-year files** to `outputs/yearly/` — one `.nc` per year, dims `(time, lat, lon)`, variable `fossil_imp`, float32. These are the files TM5 reads.
2. **Monolithic file** `outputs/ash_ff_2026.nc` — all years in one file for verification and plotting.
3. Calls `split_ct_2026.py` to produce CarbonTracker-format files (see below).

Built-in verification checks: input quality (NaN/Inf/negative), cell areas sum to 4πR², time monotonicity, global totals (5–15 PgC/yr, <20% YoY change), round-trip read-back, cross-validation with pint-based unit chain.

```bash
/opt/anaconda3/envs/p312/bin/python post_process_2026.py
```

- **Input:** `outputs/ff_monthly_2026_py.npz`
- **Output:** `outputs/yearly/ash_ff_2026.{YYYY}.nc` (33 files), `outputs/ash_ff_2026.nc`

### Step 3b: CarbonTracker-Format Split

**`split_ct_2026.py`** — Reads the monolithic `outputs/ash_ff_2026.nc` and writes CarbonTracker-format per-year and per-month files. Automatically called by `post_process_2026.py`, but can also be run standalone.

Output format:
- Time dim named `date` (midpoint of each month)
- `date_bounds`, `decimal_date` (leap-year aware), `date_components` (int32), `calendar_components` (int32)
- CarbonTracker global attributes (Notes, disclaimer, etc.)
- Only `fossil_imp` (float32, zlib compressed) + coordinate bounds
- Validates file counts and 12-months-per-year on output

```bash
python split_ct_2026.py
```

- **Input:** `outputs/ash_ff_2026.nc`
- **Output:** `outputs/ct/flux1x1_ff.{YYYY}.nc` (33 files), `outputs/ct/flux1x1_ff.{YYYYMM}.nc` (396 files)

Comparison checks are in `verify_2026.ipynb` (Check 6e).

### Step 4: Verify

**`verify_2026.ipynb`** — 90 checks in 11 sections, split into two parts:

**Part I — Input Data Integrity (Sections 1–4)**

*Section 1 — CDIAC Data Checks (1a–1l):*
- **1a.** CDIAC accounting identities — sector sums ≈ national totals; national sum + bunkers = global total
- **1b.** Suspicious year-over-year jumps in nationals (> 30% for significant emitters)
- **1c.** Gaps in country records — nations with missing years between first and last appearance
- **1d.** Negative emission values — negative totals (critical, data corruption) and negative sector values (gas/liquid/solid/flaring/cement; total can still be positive, e.g. Estonia liquid_fuel 2018–2021)
- **1e.** Country-to-grid mapping completeness — verifies positional alignment between the CDIAC country list and the GISS code table (ff_country matches by position, not by name), then checks that each country's assigned code maps to ≥1 grid cell; countries with 0 cells have emissions silently dropped. Name differences (e.g. 'Russia' vs 'RUSSIAN FEDERATION') are shown as informational, not failures.
- **1f.** Bunker fraction plausibility — global minus national sum should be 2–8% of total with no sudden jumps
- **1g.** Country zero-emission transitions — significant CDIAC emitters that drop to zero (data truncation check)
- **1h.** CDIAC Excel schema integrity — sheet name 'Sheet1' present; all expected column headers intact (detects renamed/reordered columns in future CDIAC releases)
- **1i.** CDIAC aggregation & rename drift — "little" country names (Eritrea, Gibraltar, Lesotho, etc.) and old CDIAC spelling variants (Russian Federation, Viet Nam, etc.) must be absent from the processed CSV; if present, a country name changed in a new CDIAC release and the aggregation/rename silently missed it
- **1j.** French departments interpolation sanity — French Guiana, Guadeloupe, Martinique, Réunion have non-zero, monotonically-increasing values in 2011–2014 (confirming the ingest NaN-then-interpolate step ran correctly)
- **1k.** CDIAC sector-sum integrity (processed data) — strict check that every row in the processed `CDIAC_national_2020.csv` has |sector_sum − total| ≤ 1 Gg C; verifies that `ingest_2026.py`'s total-recomputation fix covered all cases. Complements check 1a (which only flags large emitters at 5%).
- **1l.** CDIAC interpolation audit — loads the raw CDIAC xlsx and compares with the processed CSV to identify every (nation, year, sector) cell that was NaN in the raw data and non-zero after ingest's linear interpolation. Reports counts by sector and lists affected countries and year ranges. Flaring entries are highlighted because the raw total column does not include interpolated flaring values, making the total-recomputation in ingest critical.

*Section 2 — EDGAR Spatial Input Checks (2a–2j):*
- **2a.** File completeness — all three sectors (TOTALS, NMM, PRO_FFF) cover the same years
- **2b.** `global_total` attribute accuracy — parsed Gt value matches actual pixel sum to < 2%
- **2c.** TOTALS ≥ NMM + PRO_FFF at every pixel (version mismatch detection)
- **2d.** Real-vs-FAKE year split — count of real vs extrapolated files per sector; flag if > 3 FAKE years
- **2e.** Inter-year global total continuity — among real files, flag > 20% year-over-year change
- **2f.** Negative pixel values — sample check on most recent real year per sector
- **2g.** File dimension consistency — all files in each sector directory must have identical grid shape
- **2h.** Extrapolation boundary — growth rate continuity at real→FAKE transition year
- **2i.** Spatial pattern stability of extrapolated years (r ≈ 1.0 across all three sectors)
- **2j.** EDGAR NetCDF variable & grid schema — `fluxes` variable present, `year`/`units` attributes intact (`kg m-2 s-1`), grid shape (1800, 3600); catches variable renames between EDGAR releases

*Section 3 — EI & USGS Input Checks (3a–3o):*
- **3a.** USGS cement vs CDIAC cement — clinker-converted world total agrees within ±30%
- **3b.** EI country coverage vs CDIAC — countries missing from EI fall back to global growth rates; flag large emitters
- **3c.** EI vs CDIAC 2021 validation — apply 2021 EI ratios to 2020 CDIAC nationals; compare to actual (< 5% error)
- **3d.** EI fractional change plausibility — flag ratios > 3× or < 0.2× that would produce unrealistic extrapolations
- **3e.** USGS cement year coverage — ratios available for all pipeline extrapolation years (2022–2025)
- **3f.** USGS cement major country coverage — top cement producers (China, India, USA, etc.) present
- **3g.** EI Excel sheet & year column schema — all 4 required sheets present ('Primary Energy Cons (old meth)', 'Gas/Coal Consumption - EJ', 'CO2 from Flaring'), 'Total World' row found, last year column = expected `LAST_EI_YEAR`; catches sheet renames between annual EI releases
- **3h.** USGS cement CSV column schema — 'Nation' column present, cement columns follow `'Cement YYYY (Gg)'` format, no suppressed/withheld ('W', '--') values
- **3i.** EI national CSV year coverage & structure — `EI_national_2024.csv` has all 4 fuel columns, all years from 1993–`LAST_EI_YEAR`, country count matches CDIAC, base year `LAST_CDIAC_YEAR − 1` present (required for `pct_change` ratio), no NaN in extrapolation years
- **3j.** EI fractional change file completeness — `EI_frac_changes_*_{gas,coal,oil}.csv` have all extrapolation year columns, correct country count, no NaN/Inf, all ratios positive
- **3k.** USGS cement sentinel label stability — `'World total (rounded)'` and `'Other countries (rounded)'` present in merged CSV; these strings are hard-coded in ingest — if USGS renames them in a future edition the pipeline silently mis-computes
- **3l.** USGS cement fallback coverage — identifies which CDIAC countries fall back to the global 'Other countries' cement ratio (case-insensitive name matching); ranks fallback countries by CDIAC cement emissions
- **3m.** Hardcoded flaring ratios vs EI global flaring — compares year-over-year direction of hard-coded BCM volumes (`flaring = [152.7, 146.8, 157.1, 158.8]` in `ff_country`) against EI TG CO₂ flaring totals; flags years where BCM and EI point in opposite directions (requires manual review of `flaring` array each update)
- **3n.** EI region-vs-direct coverage audit — classifies each CDIAC country as using per-country EI data or a regional aggregate (from `EI_2024_fuel_regions.json` / `EI_2024_flaring_regions.json`); flags large emitters relying on regional rates, which may indicate a name mismatch that accidentally pushed a directly-covered country into a region bucket
- **3o.** USGS cement name join audit — reads all raw USGS cement CSVs, applies `USGS_RENAMES`, and checks whether each country name (after renaming, title-cased) exactly matches a CDIAC country; USGS entries that fail to match have their cement data silently replaced by the global `'Other countries'` average. Complements check 3l (CDIAC-side fallback) with the USGS-side view.

*Section 4 — Processed Intermediate Checks (4a–4h):*
- **4a.** Monthly flux array (`ff_monthly_2026_py.npz`) — shape, time span, non-negativity, spatial coverage
- **4b.** Country grid land cell coverage — unique codes, ocean fraction, suspicious high-flux unassigned cells
- **4c.** `fracarr_2026.npz` integrity — shape, value range [0, 1], no NaN
- **4d.** GISS country grid file integrity — `COUNTRY1X1.1993.mod.txt` reshapes to (180, 360), code range valid, ocean fraction 55–80%, all code-table entries represented in the map
- **4e.** Country list ordering consistency — `processed_inputs/CDIAC_countries_2021.txt` and `processed_inputs/CDIAC_countries.csv` must have identical country names in the same positional order (ff_country relies on positional alignment between these two files)
- **4f.** Seasonal cycle input file format & amplitude — Blasing file (`emis_mon_usatotal_2col.txt`) has ≥ 24 rows and amplitude `(max−min)/mean < 1.0` (otherwise `1 + seasff` could go negative); Eurasian file (`eurasian_seasff.txt`) has exactly 12 rows with all scale factors in 0.3–3.0
- **4g.** Piqs negative-clamp fraction — detects pixel-years where all 12 monthly values are identical and non-zero (the spline produced a negative daily value and ff_country substituted the flat annual mean); bar chart by year, red if clamped fraction > 2%
- **4h.** fracarr sector index sanity — verifies that the three layers of `fracarr_2026.npz` are in the expected order (0 = combustion, 1 = flaring, 2 = cement) by computing Spearman rank correlation between each layer and its corresponding EDGAR source (TOTALS−NMM−PRO_FFF, PRO_FFF, NMM respectively); a layer-order swap would silently mis-assign flaring to cement locations, undetectable from global totals

**Part II — Output Quality Checks (Sections 5–10)**

*Section 5 — Global Totals & Trends (5a–5h):*
- **5a.** Global totals vs CDIAC (should match for 1993–2021)
- **5b.** Global totals time series plot
- **5c.** Year-over-year growth rates
- **5d.** Per-capita emissions sanity check — top 10 emitters vs expected tC/person/yr benchmarks
- **5e.** 2020 COVID-19 dip — CDIAC 2020 drop vs 2019 (expected −6 to −8%), also verified in output
- **5f.** Post-CDIAC trend validation — 2022–2024 year-over-year changes vs EI-implied growth
- **5g.** Decadal growth rate benchmarks — 1993–2000, 2000–2010, 2010–2020 vs published literature
- **5h.** National-sum conservation at CDIAC→EI boundary — for 2022–2024, applies EI fractional changes to CDIAC 2021 country totals and compares the national sum against the output global total (expect ratio ≈ 1.00 within 2%); catches countries lost or double-counted in `ff_country_2026.py`'s extrapolation loop

*Section 6 — Output Data Integrity & Format (6a–6k):*
- **6a.** Data quality (NaN, negatives, spatial sanity)
- **6b.** Unit chain cross-verification (fossil_imp_cell ↔ fossil_imp_area)
- **6c.** Time & grid coordinate integrity
- **6d.** Cross-variable & metadata consistency
- **6e.** CarbonTracker-format file spot-check (file counts, required variables, time axis integrity)
- **6f.** CT monthly → yearly file self-consistency (sum of 12 monthly files = yearly file)
- **6g.** TM5 yearly file spot-check — per-year `.nc` files in `outputs/yearly/` exist and have expected shape
- **6h.** `month_lengths` calendar accuracy — February of every leap year = 29×86400 s, non-leap = 28×86400 s; each year's 12 monthly lengths sum to its `year_length` (catches calendar offset bugs if time indexing drifts by one month)
- **6i.** Coordinate bounds coverage & contiguity — `lat_bnds` spans exactly -90 to +90, `lon_bnds` spans -180 to +180, each pair of adjacent bounds shares an edge (no gaps or overlaps), all widths uniformly 1°
- **6j.** float32 encoding fidelity of per-year TM5 files — for three sample years (first, middle, last), compare float32 per-year files to float64 monolithic; max relative error < 1e-5 (typical float32 ≈ 6×10⁻⁸), no underflow pixels (mono>0 but yr=0)
- **6k.** CF global attribute completeness & source provenance — `Conventions='CF-1.8'`, `title`/`source`/`history`/`institution` all present and non-empty, `source` contains CDIAC/EI/EDGAR/USGS keywords, `earth_radius` variable = 6371.009 km (John Miller's value)

*Section 7 — Comparison with Previous Products (7a–7d):*
- **7a.** Spatial correlation with previous products (r > 0.99)
- **7b.** Comparison with Miller's (John's) previous product
- **7c.** Interactive month-by-month comparison map (2026 vs 2025a)
- **7d.** Folium fractional-difference map

*Section 8 — Spatial Analysis (8a–8i):*
- **8a.** Hemisphere balance & top emitters
- **8b.** Sector breakdown — CDIAC stacked bar by fuel type + combustion/flaring/cement spatial patterns
- **8c.** Country-level totals vs CDIAC top 10 emitters (per-country ratio check)
- **8d.** Interannual variability hotspots — std dev and CV maps, top high-variance cells
- **8e.** 2020 COVID-19 anomaly spatial pattern — global drop and regional distribution
- **8f.** Top-emitting pixels in known industrial zones — US, China, Europe, India hot spots
- **8g.** Flaring spatial concentration — top 10% of cells should hold > 85% of flaring pattern (Gini-like)
- **8h.** Antarctica / polar cells zero check — no emissions below 60°S
- **8i.** Grid-cell roundtrip (all CDIAC countries) — for three representative years (1993, 2010, 2021), sums `fossil_imp` over each country's grid cells and compares against CDIAC national total (tolerance ±5%); extends check 8c (top-10 only) to all countries; flags countries where grid sum is zero but CDIAC has non-zero emissions (silent data drop)

*Section 9 — Seasonal & Temporal Analysis (9a–9d):*
- **9a.** Seasonal cycle sanity
- **9b.** Regional seasonal cycle — N. America, Eurasia, S. Hemisphere amplitude & phasing
- **9c.** Seasonal amplitude vs latitude — NH amplitude increases toward poles (Spearman test, 30–65°N core zone)
- **9d.** Seasonal cycle temporal stability — early vs late decade Pearson r (cycle shape should be stable over time)

*Section 10 — Sector & Source Accounting (10a–10c):*
- **10a.** Bunker fuel accounting — ocean-cell sum vs CDIAC bunker residual
- **10b.** Cement and flaring sector fractions of global total (cement 3–8%, flaring 1–5%)
- **10c.** Sector fraction stability — cement/flaring/combustion % smoothness over time (no sudden jumps)

*Section 11 — Pipeline Transformation Checks (11a–11f):*
- **11a.** Seasonal cycle annual-mean preservation — verifies `mean(1+seasff) ≈ 1.0` (NAM Blasing) and `mean(seasffa) ≈ 1.0` (Eurasian); also checks that NAM and Eurasian longitude regions do not overlap (which would double-modulate cells)
- **11b.** Per-pixel piqs integral preservation after clamping — verifies that the 12 monthly values reconstruct the annual mean for every pixel-year, and that no negative values survive the piqs clamp
- **11c.** End-to-end Gg C roundtrip — integrates the final NetCDF (`mol/m²/s × cell_area × month_seconds`), converts back to Gg C, and compares against CDIAC input totals (tolerance 0.1%); closes the full unit-conversion loop
- **11d.** December→January monthly continuity at year boundaries — flags year-boundary jumps that exceed 3× the within-year p99 month-to-month variation (catches off-by-one in seasonal tile, clamp-induced discontinuities)
- **11e.** Per-sector bunker residual sign — verifies that global − Σ(countries) is non-negative for each sector (gas, oil, coal, flaring, cement); a negative residual means ocean cells have negative sector emissions baked into their total
- **11f.** Spatial correlation between consecutive years — Pearson r of annual-mean grids between all adjacent years (should be > 0.99); catches catastrophic spatial errors that cancel in global totals

## How to Update for a New Year (e.g., 2027)

### 1. Acquire New Input Data

- [ ] Download the latest **EI Statistical Review** xlsx → `inputs/EI-Stats-Review-ALL-data-{year}.xlsx`
- [ ] Check for **CDIAC** updates at AppState (new national/global xlsx if available) → `inputs/CDIAC/`
- [ ] Download latest **USGS cement MCS** PDF, extract CSV → `inputs/USGS_cement/mcs{year}-cement.csv`
- [ ] Check for **EDGAR** updates → download TOTALS, NMM, and PRO_FFF sector zips from [edgar.jrc.ec.europa.eu](https://edgar.jrc.ec.europa.eu/dataset_ghg2025)
- [ ] Update `inputs/EI_2024_flaring_regions.json` and `inputs/EI_2024_fuel_regions.json` if EI changes its country groupings

### 2. Copy and Update Code

```bash
cp ingest_2026.py ingest_2027.py
cp ff_country_2026.py ff_country_2027.py
cp post_process_2026.py post_process_2027.py
cp split_ct_2026.py split_ct_2027.py
```

### 3. Update Parameters

**In `ingest_2027.py` (configuration section near top):**
```python
LAST_EI_YEAR = 2025  # ← increment
```
Also update `EI_XLSX` filename and any sheet skipfooter values if the spreadsheet format changed.

**In `ff_country_2027.py` (year config in `main()`):**
```python
yr_ei = 2025       # ← last year of EI data
yr_final = 2026    # ← final extrapolation year
```
Cement and flaring ratios are loaded automatically from `processed_inputs/` — no manual updates needed.

**In `post_process_2027.py` (configuration section near top):**
```python
NPZ_FILE     = "outputs/ff_monthly_2027_py.npz"
MONOLITHIC   = "outputs/ash_ff_2027.nc"
FILE_PREFIX  = "ash_ff_2027"
YR3          = 2026  # ← final year
```
Also update `SOURCE_STRING` with new data source years.

**In `split_ct_2027.py` (configuration section near top):**
```python
MONOLITHIC    = "outputs/ash_ff_2027.nc"
SOURCE_STRING = "..."  # ← update data source years
```

### 4. Run the Pipeline

```bash
# Step 0 (if EDGAR doesn't cover the new final year):
/opt/anaconda3/envs/p312/bin/python extrapolate_edgar.py

# Step 1:
/opt/anaconda3/envs/p312/bin/python ingest_2027.py

# Step 2:
/opt/anaconda3/envs/p312/bin/python ff_country_2027.py

# Step 3: Post-process to per-year NetCDFs (auto-calls split_ct):
/opt/anaconda3/envs/p312/bin/python post_process_2027.py

# Step 3b: (auto-called by step 3, or run standalone):
/opt/anaconda3/envs/p312/bin/python split_ct_2027.py

# Step 4: Run verify_2026.ipynb (update file paths in the Configuration & Setup section if needed)
```

### 5. Archive Previous Version

```bash
mv ff_country_2026.py ingest_2026.py archive/
mv outputs/ff_monthly_2026_py.npz archive/outputs/
mv processed_inputs/fracarr_2026.npz archive/processed_inputs/
# Keep outputs/ash_ff_2026.nc for comparison by the new verify-sums
```

## Environment

- **Python 3.12** with conda (`/opt/anaconda3/envs/p312/`). Key packages: `numpy`, `scipy`, `pandas`, `xarray`, `pint-xarray`, `cf_xarray`, `xesmf`, `xcdat`, `openpyxl`, `netCDF4`
- All steps are pure Python — no IDL dependency

## Directory Structure

```
miller-ff/
├── README.md
├── extrapolate_edgar.py          # Step 0: extrapolate EDGAR beyond coverage
├── ingest_2026.py               # Step 1: ingest & process all inputs
├── ff_country_2026.py           # Step 2: gridding & temporal interpolation
├── post_process_2026.py         # Step 3: .npz → per-year netCDF
├── split_ct_2026.py             # Step 3b: monolithic → CT-format per-year/month
├── verify_2026.ipynb            # Step 4: quality checks
├── inputs/                      # Raw input data
│   ├── CDIAC/                   #   CDIAC xlsx files
│   ├── EI-Stats-Review-ALL-data-2025.xlsx
│   ├── EI_2024_flaring_regions.json  # EI flaring region → country mapping
│   ├── EI_2024_fuel_regions.json     # EI fuel region → country mapping
│   ├── TOTALS_flx_nc_2025_GHG/ #   EDGAR TOTALS gridded flux .nc files
│   ├── NMM_flx_nc_2025_GHG/   #   EDGAR NMM (cement) sector fluxes
│   ├── PRO_FFF_flx_nc_2025_GHG/ # EDGAR PRO_FFF (flaring) sector fluxes
│   ├── USGS_cement/             #   USGS cement CSVs
│   ├── COUNTRY1X1.1993.mod.txt  #   GISS 1° country grid
│   ├── COUNTRY1X1.CODE.mod2.2013.csv
│   ├── emis_mon_usatotal_2col.txt  # Blasing seasonal cycle data
│   └── eurasian_seasff.txt         # Eurasian seasonal adjustment
├── processed_inputs/            # Intermediate CSVs/netCDFs from Step 1
│   ├── *.csv                    #   CDIAC, EI, USGS processed data
│   └── fracarr_2026.npz         #   Sector-specific EDGAR spatial fractions
├── outputs/                     # Final and intermediate outputs
│   ├── ff_monthly_2026_py.npz   #   gridding output (Step 2)
│   ├── yearly/                  #   ← PER-YEAR FILES FOR TM5
│   │   ├── ash_ff_2026.1993.nc  #     fossil_imp (time,lat,lon), float32
│   │   ├── ...                  #     one file per year
│   │   └── ash_ff_2026.2025.nc
│   ├── ct/                      #   ← CARBONTRACKER-FORMAT FILES
│   │   ├── flux1x1_ff.1993.nc   #     fossil_imp (date,lat,lon) + decimal_date etc.
│   │   ├── flux1x1_ff.199301.nc #     per-month files
│   │   └── ...
│   ├── ash_ff_2026.nc           #   Monolithic file (for verification)
│   ├── ash_ff_2025a.nc          #   Previous version (for comparison)
│   └── Miller_20240718.nc       #   John Miller's original (for comparison)
└── archive/                     # Previous version code & data
```
