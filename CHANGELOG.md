# Changelog

All notable changes to the Miller-Pera FF pipeline and its data products.

The **data product** and the **pipeline code** are versioned together — each
product is produced by a known state of the code. Versions are named after
the production year (`2026`) with a letter suffix for revisions (`2026b`),
matching the historical IDL convention.

This project loosely follows [Keep a Changelog](https://keepachangelog.com/).

---

## [2026b] — May 2026 — near-real-time extension through April 2026

Near-real-time companion to the frozen `2026` product, produced so NOAA GML
inversion runs can reach into 2026 before the next Energy Institute release
(mid-June). Requested by Andy Jacobson.

### Data product

- **New:** `gml_ff_co2_2026b_<method>.nc` — extends the 1993–2025 series
  through **April 2026** (400 monthly time steps). Delivered as a monolithic
  netCDF plus per-month CarbonTracker files; per-year files cover the 33 full
  years only.
- Two annual-baseline methods for the 2025 → 2026 step, written as separate
  files (`_assumed` and `_cm_yearly`) so the choice can be made at delivery:
  - `assumed` — gas/oil +2.5%, coal/flaring +1% per fuel (the `2026` rates
    carried forward one year).
  - `cm_yearly` — per-country CarbonMonitor Q1-2026/Q1-2025 ratio applied
    uniformly across all five sectors.
  The two agree within ~0.25% annually; see
  `outputs/v2026b_method_comparison.{md,png}`.
- Feb–Apr 2026 are overwritten per grid cell with
  `prior_year_same_month × CarbonMonitor_YoY_ratio` (ROW fallback for
  non-tracked countries, WORLD for ocean/bunker cells). April is filled once
  CarbonMonitor publishes it; until then that month keeps the spline output.

### Pipeline

- **`download_carbon_monitor.py`** — fetches the CarbonMonitor global daily
  CSV, validates schema + coverage, idempotent.
- **`ingest_2026.py`** — new `_load_carbon_monitor()` step: harmonises CM
  country names, drops aviation sectors + the EU27 aggregate, writes
  per-country monthly totals, intra-year ratios, YoY ratios, and a yearly
  proxy ratio to `processed_inputs/`.
- **`ff_country_2026.py`** — extended to 2026 with a `--method` switch and a
  new `_apply_cm_monthly_overwrite()` step (year-over-year anchored, after a
  v2026b-internal revision away from an intra-year anchor that imposed CM
  seasonality on non-seasonal regions).
- **`post_process_2026.py`** / **`split_ct_2026.py`** — partial-year aware;
  `--method`-tagged output filenames; `v2026b_annual_method` global attribute.
- **`compare_methods_2026b.py`** — new: side-by-side comparison report
  (markdown + 2×2 figure).
- **`verify_2026b.ipynb`** — new: 3 partial-year-aware checks (structure,
  per-cell YoY overwrite, bounded spline-propagation noise).
- Bug fix: `_apply_cm_monthly_overwrite()` no longer overwrites a month with
  a prior-year copy when every CM ratio for that month is NaN.

### Tooling

- **Tests:** new `tests/` suite — 76 `pytest` tests: pure-function unit
  tests, schema / grid / ratio guardrails (skip when inputs are absent),
  9 `hypothesis` property tests (PIQS integral preservation + continuity,
  `_distribute_to_grid` mass conservation, `_cumulative_extrap` chaining,
  calendar identities), and 13 integration tests for the stage-to-stage
  contracts (post_process → split_ct CarbonTracker transform, and the
  Gg C ↔ mol m⁻² s⁻¹ ↔ PgC conversion round-trip).
- **CI:** `.github/workflows/ci.yml` runs ruff + mypy + pytest on every push
  and pull request.
- **Reproducibility:** `environment.yml` pins the conda environment;
  `.pre-commit-config.yaml` runs the lint/type/hygiene gates locally on
  commit.
- **Delivery:** `package_delivery.py` builds the `send_to_ken/` bundle from
  source (previously hand-copied).
- **Shared constants:** `constants.py` — `EARTH_RADIUS`, `C_MOLAR_MASS`
  centralised (were duplicated module-level literals).
- `CHANGELOG.md` added.

---

## [2026] — March 2026 — frozen 1993–2025 product

The annual production product: gridded 1°×1° monthly fossil-fuel CO₂
emission estimates, 1993–2025, for use as prior fluxes in NOAA GML
atmospheric inversion systems.

### Data product

- `gml_ff_co2_2026.nc` — 1993–2025, 396 monthly time steps. (Renamed from
  `ash_ff_2026.nc`; the `ash_` prefix referred to a first name, the new name
  is institution-oriented.)
- Inputs: CDIAC-at-AppState national/global totals (through 2022), Energy
  Institute Statistical Review (2023–2024 ratios), USGS Mineral Commodity
  Summaries cement, EDGAR v8.0 spatial patterns.
- Method: country totals gridded via EDGAR sector patterns → bunker fuels
  over ocean → annual-to-monthly via Rasmussen (1991) PIQS spline → Blasing /
  EDGAR-derived seasonal cycles. Extrapolation 2023–2024 from EI ratios,
  2025 from assumed growth rates.

### Pipeline / tooling

- Six-stage pipeline: `extrapolate_edgar` → `ingest_2026` → `ff_country_2026`
  → `post_process_2026` → `split_ct_2026`, plus `verify_2026.ipynb`
  (90+ automated quality checks).
- `country_names.py` + `inputs/country_aliases.json` — single-source country
  harmonisation (189 canonical countries).
- Documentation: `README.md`, `methodology.html`, `landing_page.html`.
- Code-quality pass: `pyproject.toml` with a strict-but-practical ruff
  config; full type annotations (`mypy` clean); 439 ruff auto-fixes plus 28
  hand-fixes.

---

## [2025a] — prior product

`ash_ff_2025a.nc` — the previous release, retained under `outputs/` and
`inputs/` purely as a comparison baseline for the `verify_*` notebooks
(spatial-correlation and fractional-difference checks against the new
product). Not produced by the current code.
