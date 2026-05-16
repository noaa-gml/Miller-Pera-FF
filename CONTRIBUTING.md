# Contributing

This repository holds the **Miller-Pera FF** pipeline — the code that builds
NOAA GML's gridded fossil-fuel CO₂ emissions product. These notes cover the
parts of the development workflow that are not obvious from reading the code.

For *running* the pipeline and updating it for a new production year, see
`README.md`. This file is about *changing* the code.

## Development environment

The pipeline is pure Python 3.12. The canonical environment is the conda env
`p312`:

```bash
conda env create -f environment.yml
conda activate p312
```

`environment.yml` is exact-pinned (see [Dependencies](#dependencies-and-what-must-stay-in-sync)).
Three geospatial packages — `xesmf`, `xcdat`, `esmpy` — are conda-forge only,
so conda is required for a full pipeline run: the EDGAR 0.1°→1° regrid needs
`xesmf`, which binds the compiled ESMF library.

**Let conda manage the scientific stack.** Don't `pip install` packages
(numpy, xarray, …) into `p312`: mixing pip and conda for the same package
leaves the environment with inconsistent metadata and, worse, ABI-mismatched
binaries that can silently corrupt numerical output. If an env drifts into
that state, rebuild it from `environment.yml`.

`requirements.txt` is the pip-installable subset of the same pins. It is
enough for the three gates below and every pipeline stage *except* the regrid
— it is what CI installs — but it cannot reproduce the whole pipeline:

```bash
python -m pip install -r requirements.txt   # no regridding
```

For a bit-identical environment, install from `conda-lock.yml` — the fully
resolved, multi-platform lockfile (every transitive package pinned to an
exact build): `conda-lock install -n p312 conda-lock.yml`. Regenerate it
with `make lock` after editing `environment.yml`.

There is no R (or other-language) code. The only non-Python files are legacy
IDL `.pro` scripts under `archive/`, kept for historical reference; they are
not part of the build.

## The three gates

Every change must pass three checks. CI (`.github/workflows/ci.yml`) runs all
three on every push and pull request:

```bash
ruff check      # lint
mypy .          # type-check
pytest          # tests
```

Run them locally before pushing — the next two sections explain how.

## Pre-commit hooks

`ruff` and `mypy` — plus whitespace / YAML / large-file hygiene checks — run
automatically on every commit. Install the hooks once:

```bash
pre-commit install
```

mypy is configured as a *system* hook: it uses whatever `mypy` is on your
`PATH`, so it can see the installed scientific packages and resolve their
types. **Commit from inside the activated `p312` env**, or the mypy hook will
fail or diverge from CI.

`pytest` is deliberately *not* a pre-commit hook — the data-dependent tests
are too slow to run on every commit. Run `pytest` yourself before pushing;
CI runs it regardless.

Run every hook against the whole repo at any time:

```bash
pre-commit run --all-files
```

## Running the tests

```bash
pytest
```

The 80-plus tests in `tests/` fall into three groups:

- **Unit / pure-function tests** — country-name harmonisation, the PIQS
  spline, ratio math, provenance. Always run.
- **Property-based tests** (`test_properties.py`, Hypothesis) — invariants
  checked over generated inputs.
- **Data-dependent tests** — input-schema, country-grid, ratio-sanity, and
  integration checks. These need the raw input files, which are *not* in the
  repo (`*.xlsx`, `*.nc`, the CarbonMonitor CSVs are all git-ignored), plus
  the intermediates in `processed_inputs/`. **They skip gracefully when those
  files are absent** — so a clean checkout, and CI, run the pure tests and
  skip the rest. To exercise them, produce the inputs first (see `README.md`).

When you add behaviour, add a test. When a test needs data, make it skip
cleanly if the data is missing — never assume it is present.

## Dependencies, and what must stay in sync

This is the part contributors trip on. Several files pin or list the same
things and **must be updated together**:

| If you… | also update… |
|---|---|
| add or bump a dependency | `environment.yml` **and** `requirements.txt` (exact-pinned, two formats: conda `=`, pip `==`), then regenerate `conda-lock.yml` with `make lock` |
| bump `ruff` | the `rev:` of the ruff hook in `.pre-commit-config.yaml` — it is pinned *separately*; if it drifts from the `ruff` version pin, your local lint differs from CI's |
| add a numerically-relevant package | `_TRACKED_PACKAGES` in `provenance.py`, so its version is recorded inside every output file |
| add a new pipeline module | `CODE_FILES` in `package_delivery.py`, or the file never reaches the delivery bundle |

Nothing mechanically enforces these — CI just runs the same gates your
machine should have. The practical rule: **if you skipped `pre-commit` or
`pytest` locally, you find out in CI.** Keeping `environment.yml`,
`requirements.txt`, and the pre-commit `ruff` rev on the same versions is
what makes "passes on my machine" and "passes in CI" the same statement.

## Code conventions

- **Ruff** runs with `select = ["ALL"]` (see `pyproject.toml`): every rule is
  on, then categories inappropriate for a research pipeline are switched off
  explicitly. A new finding is a real issue — fix it, or, if it genuinely
  does not apply, add it to the `ignore` list **with a comment saying why**.
  Line length is 100; target is `py312`.
- **Type annotations** — mypy runs near-strict (`[tool.mypy]` in
  `pyproject.toml`). New functions need annotations; tests are exempt.
- **Notebooks** (`*.ipynb`) are excluded from the default ruff scan — they
  hold research-style code. Lint one explicitly with
  `ruff check --fix verify.ipynb`.
- **Year configuration lives only in `config.py`** — the year span, product
  version, and input paths. The pipeline scripts are year-agnostic; never
  hard-code a year in a script. Physical constants live in `constants.py`.
  (See "Updating for a New Year" in `README.md`.)

## The delivery bundle

`delivery/` is a **build artifact** — it is wiped and rebuilt from the
source files by `package_delivery.py`. Never hand-edit it; edits are silently
lost on the next rebuild. If you add a file that belongs in a delivery, add
it to the relevant list (`DOC_FILES`, `CODE_FILES`, …) in
`package_delivery.py`.

## Output provenance

Every output netCDF carries global attributes recording how it was produced —
code commit and clean/dirty state, package versions, and input-file
fingerprints (`provenance.py`, merged in by `post_process.py` and
`split_ct.py`). If you change something that determines a numerical result,
make sure it is captured there.

## Commits and versioning

The data product and the code are versioned together — see `CHANGELOG.md`,
and add an entry for anything that changes the product or the workflow.
Released pipeline states are tagged (`git tag`) so a data user holding only
an output netCDF can check out the exact code that produced it.
