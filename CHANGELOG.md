# Changelog

All notable changes to the Miller-Pera FF pipeline and its data products.

The **data product** and the **pipeline code** are versioned together — each
product is produced by a known state of the code. Versions are named after
the production year (`2026`) with a letter suffix for revisions (`2026b`),
matching the historical IDL convention.

This project loosely follows [Keep a Changelog](https://keepachangelog.com/).

---

## [1.0.1] — 2026-06-08 — security patch + Makefile hardening

Software-only patch released between data revisions. The **data product is
unchanged** (`PRODUCT_VERSION = "2026b"`); the recorded provenance for the
same inputs is identical (`nbconvert` is not in
`provenance._TRACKED_PACKAGES`).

### Tooling

- **Security:** bumped `nbconvert` 7.16.6 → 7.17.1 to clear three advisories
  flagged on the first push to GitHub: CVE-2025-53000 (HIGH, Windows code
  execution via uncontrolled search path), CVE-2026-39377 / CVE-2026-39378
  (MODERATE, path-traversal arbitrary file write / read). Updated in
  `environment.yml`, `requirements.txt`, and `conda-lock.yml`; the `p312`
  env updated to match.
- **Makefile:** added `.NOTPARALLEL` so `make -j v2026b` can't race the
  strictly-ordered pipeline stages and produce a silently-wrong output;
  routed `lint` / `typecheck` / `verify` through `$(PYTHON) -m` so the
  documented `make PYTHON="conda run -n p312 python" check` override reaches
  every gate, not just `test`.

---

## [2026b] — May 2026 — near-real-time extension through April 2026

Near-real-time companion to the frozen `2026` product, produced so NOAA GML
inversion runs can reach into 2026 before the next Energy Institute release
(mid-June). Requested by Andy Jacobson.

### Data product

- **New:** `gml_ff_co2_2026b_<method>.nc` — extends the 1993–2025 series
  through **April 2026** (400 monthly time steps). Delivered as a monolithic
  netCDF plus 400 per-month CarbonTracker files and 34 CarbonTracker per-year
  files: 33 full years (1993–2025) plus a partial `flux1x1_ff_<method>.2026.nc`
  (4 months, Jan–Apr). The TM5 `yearly/` per-year files remain full-years-only.
- Two annual-baseline methods for the 2025 → 2026 step, written as separate
  files (`_assumed` and `_cm_yearly`) so the choice can be made at delivery:
  - `assumed` — gas/oil +2.5%, coal/flaring +1% per fuel (the `2026` rates
    carried forward one year).
  - `cm_yearly` — per-country CarbonMonitor year-to-date ratio (sum of
    available 2026 months / same 2025 months) applied uniformly across all
    five sectors.
  The two agree to within ~0.03% on annual totals (spatial rel-RMS
  typically ~0.25%, up to ~0.9% in the most recent month); see
  `outputs/v2026b_method_comparison.{md,png}`.
- Feb–Apr 2026 are overwritten per grid cell with
  `prior_year_same_month × CarbonMonitor_YoY_ratio` (ROW fallback for
  non-tracked countries, WORLD for ocean/bunker cells). April is filled once
  CarbonMonitor publishes it; until then that month keeps the spline output.

### Pipeline

- **`download_carbon_monitor.py`** — fetches the CarbonMonitor global daily
  CSV, validates schema + coverage, idempotent.
- **`ingest.py`** — new `_load_carbon_monitor()` step: harmonises CM
  country names, drops aviation sectors + the EU27 aggregate, writes
  per-country monthly totals, intra-year ratios, YoY ratios, and a yearly
  proxy ratio to `processed_inputs/`.
- **`ff_country.py`** — extended to 2026 with a `--method` switch and a
  new `_apply_cm_monthly_overwrite()` step (year-over-year anchored, after a
  v2026b-internal revision away from an intra-year anchor that imposed CM
  seasonality on non-seasonal regions).
- **`post_process.py`** / **`split_ct.py`** — partial-year aware;
  `--method`-tagged output filenames; `v2026b_annual_method` global attribute.
  **Note for downstream consumers:** the CarbonTracker per-month/per-year
  files now carry only `fossil_imp` (+ coordinate/date bounds) as **float32**.
  Earlier deliveries (e.g. 20260225) were larger (~573 KB vs ~233 KB per
  month) because they were float64 and/or carried diagnostic variables
  (`fossil_imp_cell`, `cell_areas`); these are intentionally dropped here.
  The size reduction is a dtype + variable change, not merely compression.
- **`compare_methods.py`** — new: side-by-side comparison report
  (markdown + 2×2 figure).
- **`verify_nrt.ipynb`** — new: 3 partial-year-aware checks (structure,
  per-cell YoY overwrite, bounded spline-propagation noise).
- Bug fix: `_apply_cm_monthly_overwrite()` no longer overwrites a month with
  a prior-year copy when every CM ratio for that month is NaN.

### Tooling

- **Tests:** new `tests/` suite — 82 `pytest` tests: pure-function unit
  tests, schema / grid / ratio guardrails (skip when inputs are absent),
  9 `hypothesis` property tests (PIQS integral preservation + continuity,
  `_distribute_to_grid` mass conservation, `_cumulative_extrap` chaining,
  calendar identities), 13 integration tests for the stage-to-stage
  contracts (post_process → split_ct CarbonTracker transform, and the
  Gg C ↔ mol m⁻² s⁻¹ ↔ PgC conversion round-trip), and 6 provenance tests.
- **CI:** `.github/workflows/ci.yml` runs ruff + mypy + pytest on every push
  and pull request, installing the exact pinned `requirements.txt`.
- **Reproducibility:** `environment.yml` and `requirements.txt` pin every
  dependency to an exact version — the set the product was built and
  validated against. `.pre-commit-config.yaml` runs the lint/type/hygiene
  gates locally on commit.
- **Provenance:** `provenance.py` — every output netCDF now records the code
  commit (and clean/dirty state), package versions, and input-file
  fingerprints that produced it (merged in by `post_process.py` and
  `split_ct.py`).
- **Delivery:** `package_delivery.py` builds the `delivery/` bundle from
  source (previously hand-copied).
- **Shared constants:** `constants.py` — `EARTH_RADIUS`, `C_MOLAR_MASS`
  centralised (were duplicated module-level literals).
- `CHANGELOG.md` added.
- **Release metadata:** `CITATION.cff` gives a software + dataset citation
  (GitHub "Cite this repository"); `CONTRIBUTING.md` documents the developer
  workflow and the files that must stay version-synced; `LICENSE.md` carries
  the NOAA / U.S. Department of Commerce notice. Released pipeline states are
  git-tagged; `v1.0.0` is the first public release.

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

- Six-stage pipeline: `extrapolate_edgar` → `ingest` → `ff_country`
  → `post_process` → `split_ct`, plus `verify.ipynb`
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
